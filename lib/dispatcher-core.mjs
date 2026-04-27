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
// `deps` is an object the caller supplies with three async functions:
//   - claimRow(requestId)     — atomically transition queued→dialing
//   - callDialOut(payload)    — invoke bot-runner /dial-out
//   - markStatus(requestId, status, fields) — best-effort status write
//
// All three are kept small enough to mock in tests with a few lines.

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
    log.warn('[dispatcher] batch source not handled by this dispatcher', {
      messageId,
      batchId: parsed.body.batch_id,
    });
    return { outcome: OUTCOME_NOT_THIS_DISPATCHER, source: 'batch' };
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
