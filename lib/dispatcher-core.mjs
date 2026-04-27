// dispatcher-core.mjs — pure logic for the SQS → bot-runner /dial-out
// dispatcher Lambda.
//
// Why this is its own module (vs inlined in dispatcher.mjs):
//
//   AWS SDK v3 clients are heavy (~150 KB compressed each, plus a chain
//   of credential providers that hit the IMDS metadata endpoint at
//   import time). If we import them at the top of every test file the
//   suite spends most of its life waiting on credential providers that
//   will never resolve outside Lambda. Keeping the core pure with all
//   side-effecting calls injected as dependencies means:
//
//     1. Tests run in <100ms instead of timing out on IMDS lookups.
//     2. The same logic is reusable from a future EventBridge-triggered
//        path (e.g. retries-from-DLQ admin tool) without reaching into
//        SQS-event-shape parsing.
//     3. The Lambda handler stays small (~30 lines) and can be read at
//        a glance to see "what AWS calls are made for each message".
//
// Public API (used by dispatcher.mjs):
//
//   processMessage({ sqsRecord, deps })   — single SQS Record
//   processBatch({ event, deps })         — full Lambda event with N records
//
// `deps` is an object the caller supplies with these async functions:
//
//   call_request path (Phase 7D PR 1, voice_call_requests):
//     - claimRow(requestId)
//     - callDialOut(payload)
//     - markStatus(requestId, status, fields)
//
//   batch path (Phase 7D PR 2, voice_batch_rows + voice_batches):
//     - getBatch(batchId)
//     - claimBatchRow(batchRowId)
//     - markBatchRowStatus(batchRowId, status, fields)
//     - changeMessageVisibility(receiptHandle, secs)
//
// Tests inject simple stubs for each. Keeping the SDK out of this
// module means the test suite has zero credential / network setup.

// ────────────────────────────────────────────────────────────────────
// Public constants

// Match OG vocabulary (verified against live voice_call_requests rows
// 2026-04-27). Frontend dashboards already speak these strings; do
// not introduce a third dialect.
export const STATUS_QUEUED = 'queued';
export const STATUS_DIALING = 'dialing';
export const STATUS_COMPLETED = 'completed';
export const STATUS_FAILED = 'failed';
export const STATUS_DEAD_LETTER = 'dead_letter';
export const STATUS_SKIPPED = 'skipped';

// Statuses considered terminal — if we observe one of these on a
// re-delivered message, we ack the SQS message without re-dispatching.
const TERMINAL_STATUSES = new Set([
  STATUS_COMPLETED,
  STATUS_FAILED,
  STATUS_DEAD_LETTER,
  STATUS_SKIPPED,
]);

// 4xx responses from bot-runner are deterministic — retrying won't
// change the outcome (bad agent_id, missing to_number, etc.). Mark
// the row dead_letter immediately rather than chewing through all 3
// SQS attempts. 5xx responses ARE retried because they may be
// transient (Daily upstream blip, ECS cold start, etc.).
const BOT_RUNNER_PERMANENT_STATUS_FLOOR = 400;
const BOT_RUNNER_PERMANENT_STATUS_CEIL = 500; // exclusive upper bound

// Batch-path defer windows. Match OG voiceagent's call_queue_consumer.py:
//   - Outside the calling window: hide for 5 min, re-check.
//   - Batch is paused_by_user: hide for 1 min — a UI Resume click
//     should reflect quickly. We refetch the batch each time so the
//     dispatcher sees a flipped status as soon as the visibility
//     timeout elapses.
const OUTSIDE_WINDOW_VISIBILITY_SECS = 300;
const PAUSED_VISIBILITY_SECS = 60;

// Three-letter day codes the calling-window logic compares against.
// Matches OG's hardcoded set in batch_queue.py.
const DAY_CODES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

// voice_batches statuses that should pause / cancel dispatch.
// 'cancelled' / 'canceled' both seen in the wild — voice_batches.cancel
// route writes 'canceled' (American spelling) but the OG consumer
// code branched on 'cancelled' (British). Accept both.
const BATCH_CANCELLED_STATUSES = new Set(['cancelled', 'canceled']);
const BATCH_PAUSED_STATUSES = new Set(['paused']);

// ────────────────────────────────────────────────────────────────────
// Outcomes — small enum to make the contract with dispatcher.mjs
// explicit. The Lambda handler maps these to "ack" vs "throw to
// re-deliver".

export const OUTCOME_DISPATCHED = 'dispatched';
export const OUTCOME_ALREADY_PROCESSED = 'already_processed';
export const OUTCOME_PERMANENT_FAILURE = 'permanent_failure';
export const OUTCOME_TRANSIENT_FAILURE = 'transient_failure';
export const OUTCOME_BAD_MESSAGE = 'bad_message';
export const OUTCOME_NOT_THIS_DISPATCHER = 'not_this_dispatcher';
// Batch-source-only outcomes. Both result in the dispatcher RETURNING
// the message to SQS via change-message-visibility (not throwing) —
// they're polite "try later" rather than failures.
export const OUTCOME_DEFERRED_OUTSIDE_WINDOW = 'deferred_outside_window';
export const OUTCOME_DEFERRED_PAUSED = 'deferred_paused';
// Batch was cancelled while messages were in flight. We mark the row
// cancelled and ack — no retry.
export const OUTCOME_CANCELLED = 'cancelled';

// ────────────────────────────────────────────────────────────────────
// Body parsing

/**
 * Parse the SQS message body. The body shape was set by the producer
 * (the medcloud-voice-api POST /api/call-requests handler) and is the
 * same shape OG voiceagent's call_queue_consumer reads from. Keep
 * field-for-field compatible.
 *
 * Returns:
 *   { kind: 'call_request', body, requestId }
 *   { kind: 'batch', body }                    — out of scope this PR
 *   { kind: 'malformed', error }
 */
export function parseSqsBody(rawBody) {
  if (typeof rawBody !== 'string' || !rawBody.trim()) {
    return { kind: 'malformed', error: 'Empty or non-string body' };
  }
  let body;
  try {
    body = JSON.parse(rawBody);
  } catch (e) {
    return { kind: 'malformed', error: `JSON parse failed: ${e.message}` };
  }
  if (!body || typeof body !== 'object') {
    return { kind: 'malformed', error: 'Body did not parse to an object' };
  }

  const source = body.source || 'call_request';
  if (source === 'batch') {
    return { kind: 'batch', body };
  }
  if (source !== 'call_request') {
    return { kind: 'malformed', error: `Unknown source: ${source}` };
  }

  const requestId = body.request_id;
  if (typeof requestId !== 'string' || !requestId) {
    return { kind: 'malformed', error: 'Missing or non-string request_id' };
  }

  return { kind: 'call_request', body, requestId };
}

// ────────────────────────────────────────────────────────────────────
// Validation — checks done before dispatch. We want the dispatcher
// to never call /dial-out with a payload bot-runner would reject for
// schema reasons; that wastes a Lambda invocation and clouds CloudWatch
// metrics with 4xx noise that's actually our fault.

export function validateRow(row) {
  if (!row || typeof row !== 'object') {
    return 'Row not found';
  }
  if (typeof row.to_number !== 'string' || !row.to_number.trim()) {
    return 'to_number is required';
  }
  if (typeof row.agent_name !== 'string' || !row.agent_name.trim()) {
    return 'agent_name is required';
  }
  // from_number is optional (Daily uses default caller ID if omitted)
  // — match bot-runner /dial-out contract documented in handler.py:321.
  return null;
}

/**
 * Pure: is `now` (Date) inside the configured calling window?
 *
 * window shape (matches OG batch_queue.py + buildRowMessage producer):
 *   { start: 'HH:MM', end: 'HH:MM', timezone: 'IANA/Zone', days: ['Mon',...] }
 *
 * If the window object is missing/malformed we return TRUE (don't
 * block dispatch on bad metadata — fail open). This matches OG's
 * try/except behavior in _is_within_window.
 *
 * Implementation note on timezone: we use Intl.DateTimeFormat to
 * read wall-clock hours/minutes/day in the target tz. The dispatcher
 * Lambda runs in UTC; we never compare UTC strings to local windows.
 */
export function isWithinCallingWindow(window, now = new Date()) {
  if (!window || typeof window !== 'object') return true;
  const tz = window.timezone || 'America/New_York';
  const start = window.start || '00:00';
  const end = window.end || '23:59';
  const days = Array.isArray(window.days) && window.days.length
    ? window.days
    : ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'];

  let hh; let mm; let dayCode;
  try {
    // 'short' weekday gives 'Mon'/'Tue'/etc — matching the day-code
    // shape the producer writes.
    const fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      weekday: 'short',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
    const parts = Object.fromEntries(
      fmt.formatToParts(now).map((p) => [p.type, p.value]),
    );
    dayCode = parts.weekday;
    hh = String(parts.hour || '00').padStart(2, '0');
    // Some Intl impls return '24' for midnight in 24h mode. Normalize.
    if (hh === '24') hh = '00';
    mm = String(parts.minute || '00').padStart(2, '0');
  } catch {
    return true; // bad timezone string → don't block
  }

  if (!days.includes(dayCode)) return false;
  if (!DAY_CODES.includes(dayCode)) {
    // Producer wrote a bad day code; fail open.
    return true;
  }
  const nowHm = `${hh}:${mm}`;
  return start <= nowHm && nowHm <= end;
}

// ────────────────────────────────────────────────────────────────────
// Main entry per record

/**
 * Process one SQS Record.
 *
 * Flow:
 *   1. Parse body. Bad JSON → bad_message (DLQ via SQS auto-redrive).
 *   2. If batch source → not_this_dispatcher (ack message, log warn).
 *      Note: we DELETE the message on out-of-scope source. The batch
 *      dispatcher (follow-up PR) will read from a separate queue or
 *      filter at SQS event source mapping level once it lands.
 *   3. Atomically claim the row. claimRow() does:
 *        UPDATE voice_call_requests
 *           SET status='dialing', attempt_count = attempt_count+1
 *         WHERE id = $1 AND status IN ('queued','pending')
 *         RETURNING *;
 *      If 0 rows updated → already claimed (concurrent dispatcher) or
 *      already terminal. Either way: ack the message. The pipeline's
 *      after-call writes will land on the row from the OTHER dispatch.
 *   4. Validate the claimed row. Validation failure → mark dead_letter,
 *      ack message.
 *   5. Call /dial-out via lambda:invoke. Branch on response:
 *        2xx → leave row at status='dialing'. The pipeline will
 *               write 'completed'/'failed' from the after-call hook
 *               (PR #25/voice_calls write).
 *        4xx → mark dead_letter immediately. ack message.
 *        5xx or thrown → return transient_failure. dispatcher.mjs
 *               re-throws so SQS re-delivers up to maxReceiveCount.
 *               On the LAST attempt SQS auto-routes to the DLQ, AND
 *               the row remains at 'dialing'. We accept that race —
 *               better to surface as alarm-able stuck row than to
 *               clobber it with 'failed' on every retry attempt.
 */
export async function processMessage({ sqsRecord, deps, logger }) {
  const log = logger || console;
  const messageId = sqsRecord?.messageId || '<unknown>';
  const receiveCount = Number(
    sqsRecord?.attributes?.ApproximateReceiveCount || 1,
  );

  const parsed = parseSqsBody(sqsRecord?.body);

  if (parsed.kind === 'malformed') {
    log.error('[dispatcher] malformed message', {
      messageId,
      reason: parsed.error,
    });
    return { outcome: OUTCOME_BAD_MESSAGE, reason: parsed.error };
  }

  if (parsed.kind === 'batch') {
    return processBatchRow({
      sqsRecord, body: parsed.body, deps, log,
    });
  }

  const { body, requestId } = parsed;

  // Atomic claim. claimRow() returns either the freshly-claimed row
  // or null (if 0 rows matched the WHERE).
  let row;
  try {
    row = await deps.claimRow(requestId);
  } catch (e) {
    log.error('[dispatcher] claim failed (DB error)', {
      messageId,
      requestId,
      error: e.message,
    });
    return {
      outcome: OUTCOME_TRANSIENT_FAILURE,
      reason: `claim_db_error: ${e.message}`,
      retryable: true,
    };
  }

  if (!row) {
    // Either the row is already in a non-claimable status, or it
    // doesn't exist. Both are ack-worthy.
    log.info('[dispatcher] row not claimable (already processed or missing)', {
      messageId,
      requestId,
      receiveCount,
    });
    return { outcome: OUTCOME_ALREADY_PROCESSED, requestId };
  }

  // Validate the claimed row before calling /dial-out. If it fails
  // validation we mark dead_letter immediately (no retries — the row
  // will never become valid by waiting).
  const validationError = validateRow(row);
  if (validationError) {
    log.error('[dispatcher] validation failed after claim', {
      messageId,
      requestId,
      error: validationError,
    });
    await deps.markStatus(requestId, STATUS_DEAD_LETTER, {
      error_message: `validation: ${validationError}`,
    });
    return {
      outcome: OUTCOME_PERMANENT_FAILURE,
      reason: `validation: ${validationError}`,
    };
  }

  // Build /dial-out payload from the claimed row + body. The body
  // takes precedence for case_data because the producer guarantees
  // it's the freshest copy (Aurora row may not have it for legacy
  // requests). All other fields read from the row to ensure we use
  // the latest agent/from-number config even if the SQS message is
  // an old re-delivery.
  const dialOutPayload = {
    invocation_type: 'dial_out',
    to_number: row.to_number,
    from_number: row.from_number || undefined,
    agent_id: row.agent_name,
    case_data: body.case_data ?? row.case_data ?? {},
    session_id: body.session_id || `voice-out-${requestId.slice(0, 12)}`,
    // Pass through metadata for after-call writes (PR #25 wired them).
    request_id: requestId,
  };

  let response;
  try {
    response = await deps.callDialOut(dialOutPayload);
  } catch (e) {
    log.warn('[dispatcher] callDialOut threw', {
      messageId,
      requestId,
      error: e.message,
      receiveCount,
    });
    return {
      outcome: OUTCOME_TRANSIENT_FAILURE,
      reason: `dial_out_threw: ${e.message}`,
      retryable: true,
    };
  }

  const statusCode = response?.statusCode ?? 0;

  // 2xx — successful handoff to bot-runner. Pipeline owns terminal
  // status from here.
  if (statusCode >= 200 && statusCode < 300) {
    log.info('[dispatcher] dispatched', {
      messageId,
      requestId,
      statusCode,
      sessionId: response?.body?.session_id,
    });
    return {
      outcome: OUTCOME_DISPATCHED,
      requestId,
      sessionId: response?.body?.session_id,
    };
  }

  // 4xx — permanent. Mark dead_letter + ack.
  if (
    statusCode >= BOT_RUNNER_PERMANENT_STATUS_FLOOR
    && statusCode < BOT_RUNNER_PERMANENT_STATUS_CEIL
  ) {
    const errorDetail = response?.body?.detail || response?.body?.error
      || `bot-runner returned ${statusCode}`;
    log.error('[dispatcher] permanent failure from bot-runner', {
      messageId,
      requestId,
      statusCode,
      detail: errorDetail,
    });
    await deps.markStatus(requestId, STATUS_DEAD_LETTER, {
      error_message: `bot_runner_${statusCode}: ${errorDetail}`,
    });
    return {
      outcome: OUTCOME_PERMANENT_FAILURE,
      reason: `bot_runner_${statusCode}`,
      detail: errorDetail,
    };
  }

  // 5xx or unknown — transient. Surface as throw so SQS re-delivers
  // with exponential backoff (managed by the queue's redrive policy,
  // currently maxReceiveCount=3). On the final attempt the message
  // lands in the DLQ.
  const errorDetail = response?.body?.detail || response?.body?.error
    || `bot-runner returned ${statusCode}`;
  log.warn('[dispatcher] transient failure from bot-runner', {
    messageId,
    requestId,
    statusCode,
    detail: errorDetail,
    receiveCount,
  });
  return {
    outcome: OUTCOME_TRANSIENT_FAILURE,
    reason: `bot_runner_${statusCode}`,
    detail: errorDetail,
    retryable: true,
  };
}

// ────────────────────────────────────────────────────────────────────
// Batch-source path — for messages produced by lib/batch-enqueuer.mjs
// (POST /api/batches/:id/start). Phase 7D PR 2.
//
// Differences from the call_request path:
//
//   - Two-table model: voice_batch_rows is the row to claim;
//     voice_batches is the parent that holds agent_name/from_number
//     + status (paused/cancelled). The producer denormalizes both
//     into the SQS body so the dispatcher's hot path doesn't need
//     to JOIN — we only re-fetch the parent batch when its status
//     might have changed (paused/cancelled mid-flight).
//
//   - Three "polite" outcomes that change the SQS visibility timeout
//     instead of throwing for redrive: outside calling window, batch
//     paused, batch cancelled. None counts as a Lambda failure.
//
//   - Idempotent claim is on voice_batch_rows.id (not voice_batch_rows
//     .row_index — row_index is only unique within a batch).

async function processBatchRow({ sqsRecord, body, deps, log }) {
  const messageId = sqsRecord?.messageId || '<unknown>';
  const receiptHandle = sqsRecord?.receiptHandle;
  const receiveCount = Number(
    sqsRecord?.attributes?.ApproximateReceiveCount || 1,
  );

  const batchId = body.batch_id;
  const batchRowId = body.batch_row_id;
  const rowIndex = body.row_index ?? null;

  if (typeof batchId !== 'string' || !batchId
   || typeof batchRowId !== 'string' || !batchRowId) {
    log.error('[dispatcher] batch message missing batch_id/batch_row_id', {
      messageId,
    });
    return {
      outcome: OUTCOME_BAD_MESSAGE,
      reason: 'Missing batch_id or batch_row_id',
    };
  }

  // 1. Re-fetch the parent batch to check pause/cancel state. The
  // producer denormalized agent_name + from_number into the SQS
  // body so we don't need them from this fetch — we only care
  // about `.status` (and `.paused_by_user` if OG-style) for
  // rate-limit decisions. If the lookup fails, surface as
  // transient — better to retry than to dispatch into a paused
  // batch.
  let batch;
  try {
    batch = await deps.getBatch(batchId);
  } catch (e) {
    log.warn('[dispatcher] getBatch failed (transient)', {
      messageId,
      batchId,
      error: e.message,
    });
    return {
      outcome: OUTCOME_TRANSIENT_FAILURE,
      reason: `get_batch_failed: ${e.message}`,
      retryable: true,
    };
  }

  // Cancelled: mark the row cancelled and ack. No retry.
  if (batch && BATCH_CANCELLED_STATUSES.has(batch.status)) {
    log.info('[dispatcher] batch cancelled — marking row cancelled', {
      messageId,
      batchId,
      batchRowId,
    });
    if (deps.markBatchRowStatus) {
      await deps.markBatchRowStatus(batchRowId, 'cancelled', {
        error_message: 'batch cancelled while in flight',
      });
    }
    return { outcome: OUTCOME_CANCELLED, batchRowId };
  }

  // Paused: defer for 60s. We do NOT mark the row — the Resume
  // flow flips voice_batches.status back to 'running' and our
  // next poll picks the message back up unchanged.
  if (batch && (BATCH_PAUSED_STATUSES.has(batch.status) || batch.paused_by_user)) {
    log.info('[dispatcher] batch paused — deferring', {
      messageId,
      batchId,
      defer_secs: PAUSED_VISIBILITY_SECS,
    });
    if (deps.changeMessageVisibility && receiptHandle) {
      try {
        await deps.changeMessageVisibility(receiptHandle, PAUSED_VISIBILITY_SECS);
      } catch (e) {
        log.warn('[dispatcher] changeMessageVisibility failed (paused defer)', {
          messageId,
          error: e.message,
        });
      }
    }
    return {
      outcome: OUTCOME_DEFERRED_PAUSED,
      batchId,
      defer_secs: PAUSED_VISIBILITY_SECS,
    };
  }

  // 2. Calling-window check. The producer attached the window to
  // the message body; we don't trust voice_batches' columns here
  // because the user might have changed the window between
  // enqueue and dispatch. Use the message body's window to keep
  // dispatch decisions stable per-message.
  if (!isWithinCallingWindow(body.calling_window || null)) {
    log.info('[dispatcher] outside calling window — deferring', {
      messageId,
      window: body.calling_window,
      defer_secs: OUTSIDE_WINDOW_VISIBILITY_SECS,
    });
    if (deps.changeMessageVisibility && receiptHandle) {
      try {
        await deps.changeMessageVisibility(receiptHandle, OUTSIDE_WINDOW_VISIBILITY_SECS);
      } catch (e) {
        log.warn('[dispatcher] changeMessageVisibility failed (window defer)', {
          messageId,
          error: e.message,
        });
      }
    }
    return {
      outcome: OUTCOME_DEFERRED_OUTSIDE_WINDOW,
      defer_secs: OUTSIDE_WINDOW_VISIBILITY_SECS,
    };
  }

  // 3. Atomic claim. Same idempotency principle as call_request:
  // only transition queued→dialing. If the row is already in any
  // other state (cancelled, dialing, terminal), the WHERE matches
  // 0 rows and we ack the message without acting. Note we accept
  // either 'queued' (the producer's flag) or 'pending' (the
  // resting state on a fresh insert) so a manual SQS replay of an
  // unstarted row still works.
  let row;
  try {
    row = await deps.claimBatchRow(batchRowId);
  } catch (e) {
    log.error('[dispatcher] claimBatchRow failed (DB error)', {
      messageId,
      batchRowId,
      error: e.message,
    });
    return {
      outcome: OUTCOME_TRANSIENT_FAILURE,
      reason: `claim_db_error: ${e.message}`,
      retryable: true,
    };
  }

  if (!row) {
    log.info('[dispatcher] batch row not claimable (already processed)', {
      messageId,
      batchRowId,
      receiveCount,
    });
    return { outcome: OUTCOME_ALREADY_PROCESSED, batchRowId };
  }

  // 4. Build /dial-out payload. The body has the denormalized
  // agent_name/from_number/case_data; we reuse them. If the row
  // somehow has a different phone_e164 than the body's to_number
  // (shouldn't happen — producer copies straight from the row),
  // prefer the row as the freshest source of truth.
  const dialOutPayload = {
    invocation_type: 'dial_out',
    to_number: row.phone_e164 || body.to_number,
    from_number: body.from_number || undefined,
    agent_id: body.agent_name,
    case_data: body.case_data || row.case_data || {},
    session_id: `voice-out-batch-${batchRowId.slice(0, 12)}`,
    // Pass batch metadata so the after-call hook can populate
    // voice_calls.batch_id + batch_row_index. The Lambda's POST
    // /api/calls handler then mirrors the terminal status into
    // voice_batch_rows.
    batch_id: batchId,
    batch_row_index: rowIndex,
    batch_row_id: batchRowId,
  };

  // Validate. Same rules as call_request but using the row's
  // denormalized fields (phone_e164 + body's agent_name).
  const validationError = validateRow({
    to_number: dialOutPayload.to_number,
    agent_name: dialOutPayload.agent_id,
  });
  if (validationError) {
    log.error('[dispatcher] batch row validation failed', {
      messageId,
      batchRowId,
      error: validationError,
    });
    if (deps.markBatchRowStatus) {
      await deps.markBatchRowStatus(batchRowId, 'failed', {
        error_message: `validation: ${validationError}`,
      });
    }
    return {
      outcome: OUTCOME_PERMANENT_FAILURE,
      reason: `validation: ${validationError}`,
    };
  }

  // 5. Same /dial-out call as call_request. The bot-runner handler
  // doesn't care which source the dispatch came from — to_number
  // and agent_id are all it needs.
  let response;
  try {
    response = await deps.callDialOut(dialOutPayload);
  } catch (e) {
    log.warn('[dispatcher] callDialOut threw (batch path)', {
      messageId,
      batchRowId,
      error: e.message,
      receiveCount,
    });
    return {
      outcome: OUTCOME_TRANSIENT_FAILURE,
      reason: `dial_out_threw: ${e.message}`,
      retryable: true,
    };
  }

  const statusCode = response?.statusCode ?? 0;

  if (statusCode >= 200 && statusCode < 300) {
    log.info('[dispatcher] batch row dispatched', {
      messageId,
      batchRowId,
      batchId,
      rowIndex,
      statusCode,
      sessionId: response?.body?.session_id,
    });
    return {
      outcome: OUTCOME_DISPATCHED,
      batchRowId,
      sessionId: response?.body?.session_id,
    };
  }

  if (
    statusCode >= BOT_RUNNER_PERMANENT_STATUS_FLOOR
    && statusCode < BOT_RUNNER_PERMANENT_STATUS_CEIL
  ) {
    const errorDetail = response?.body?.detail || response?.body?.error
      || `bot-runner returned ${statusCode}`;
    log.error('[dispatcher] batch row permanent failure', {
      messageId,
      batchRowId,
      statusCode,
      detail: errorDetail,
    });
    if (deps.markBatchRowStatus) {
      await deps.markBatchRowStatus(batchRowId, 'failed', {
        error_message: `bot_runner_${statusCode}: ${errorDetail}`,
      });
    }
    return {
      outcome: OUTCOME_PERMANENT_FAILURE,
      reason: `bot_runner_${statusCode}`,
      detail: errorDetail,
    };
  }

  // 5xx — transient. Don't clobber the row's status; SQS will retry.
  const errorDetail = response?.body?.detail || response?.body?.error
    || `bot-runner returned ${statusCode}`;
  log.warn('[dispatcher] batch row transient failure', {
    messageId,
    batchRowId,
    statusCode,
    detail: errorDetail,
    receiveCount,
  });
  return {
    outcome: OUTCOME_TRANSIENT_FAILURE,
    reason: `bot_runner_${statusCode}`,
    detail: errorDetail,
    retryable: true,
  };
}

// ────────────────────────────────────────────────────────────────────
// Batch entry — Lambda passes us the whole event with up to N records
// at once. We process them sequentially (not parallel) because each
// dispatch acquires a DB connection and we don't want to swarm the
// pool. Reserved concurrency on the Lambda function caps the total
// system-wide parallelism (set to 10 in the bootstrap script).

export async function processBatch({ event, deps, logger }) {
  const log = logger || console;
  const records = event?.Records || [];
  if (!records.length) {
    log.info('[dispatcher] empty event — nothing to do');
    return { batchItemFailures: [] };
  }

  // SQS partial batch response: any record id we list here will be
  // re-delivered by SQS without affecting the others. See
  // https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting
  const batchItemFailures = [];
  const summary = {
    [OUTCOME_DISPATCHED]: 0,
    [OUTCOME_ALREADY_PROCESSED]: 0,
    [OUTCOME_PERMANENT_FAILURE]: 0,
    [OUTCOME_TRANSIENT_FAILURE]: 0,
    [OUTCOME_BAD_MESSAGE]: 0,
    [OUTCOME_NOT_THIS_DISPATCHER]: 0,
    [OUTCOME_DEFERRED_OUTSIDE_WINDOW]: 0,
    [OUTCOME_DEFERRED_PAUSED]: 0,
    [OUTCOME_CANCELLED]: 0,
  };

  for (const rec of records) {
    let result;
    try {
      result = await processMessage({ sqsRecord: rec, deps, logger });
    } catch (e) {
      log.error('[dispatcher] processMessage threw (treated as transient)', {
        messageId: rec?.messageId,
        error: e.message,
        stack: e.stack,
      });
      result = {
        outcome: OUTCOME_TRANSIENT_FAILURE,
        reason: `unhandled_exception: ${e.message}`,
        retryable: true,
      };
    }
    summary[result.outcome] = (summary[result.outcome] || 0) + 1;

    if (result.retryable) {
      batchItemFailures.push({ itemIdentifier: rec.messageId });
    }
  }

  log.info('[dispatcher] batch complete', {
    total: records.length,
    summary,
    failures: batchItemFailures.length,
  });

  return { batchItemFailures };
}
