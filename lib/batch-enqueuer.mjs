// batch-enqueuer.mjs — port of OG voiceagent server/batch_queue.py:start_batch
// to the Lambda layer. Phase 7D follow-up.
//
// Why we port (vs run as a long-lived service):
//
//   The original lived in an always-on EC2 process. Migrating to
//   Lambda means the API request that POSTs /api/batches/:id/start
//   does the enqueue work synchronously and returns. Pure stateless
//   — no daemons, no missed-batch recovery to worry about. Trade-off:
//   the API call blocks for ~50ms per row while we send to SQS, so
//   a 1000-row batch takes ~50s of API time. Acceptable for batch
//   start (rare, foreground operation); revisit if anyone starts
//   complaining about the wait.
//
// What this module owns:
//
//   start(pool, sqsClient, batchId)
//       Look up the batch + its pending rows, fan out one SQS
//       message per row, mark each row queued. Returns a summary
//       dict for the API caller.
//
//   buildRowMessage(batch, row)
//       Pure function. Builds the SQS body for a single row. Kept
//       separate so tests can verify the wire format without
//       running an SQS client.
//
// Wire format intentionally matches OG byte-for-byte (`source: "batch"`,
// `batch_id`, `batch_row_id`, `agent_name`, `to_number`, `from_number`,
// `case_data`, `concurrency`, `calling_window`, `dedup_key`,
// `attempt_count`, `max_attempts`). The dispatcher's batch handler
// (lib/dispatcher-core.mjs `processBatch` source path) reads this
// shape directly. Don't drift.

import { SendMessageCommand } from '@aws-sdk/client-sqs';

const STARTABLE_STATUSES = new Set([
  'draft',
  'ready',
  'paused',
  'scheduled',
  'running',
]);

// Default calling window — Mon-Fri 09:00-17:00 America/New_York.
// Matches OG's default. Caller (the API handler) should override
// with batch-specific window when present on voice_batches.
const DEFAULT_WINDOW = {
  start: '00:00',
  end: '23:59',
  timezone: 'America/New_York',
  days: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'],
};

/**
 * Pure: build the SQS message body for a single batch row.
 *
 * Exposed for testing. The producer + dispatcher must agree on this
 * shape; isolating it as a pure function lets tests pin the contract
 * without round-tripping SQS.
 */
export function buildRowMessage(batch, row) {
  const callingWindow = {
    start: batch.calling_window_start || DEFAULT_WINDOW.start,
    end: batch.calling_window_end || DEFAULT_WINDOW.end,
    timezone: batch.timezone || DEFAULT_WINDOW.timezone,
    days: batch.calling_window_days || DEFAULT_WINDOW.days,
  };

  return {
    source: 'batch',
    batch_id: batch.id,
    batch_row_id: row.id,
    row_index: row.row_index ?? null,
    agent_name: batch.agent_name,
    to_number: row.phone_e164,
    from_number: batch.from_number,
    case_data: row.case_data || {},
    concurrency: batch.concurrency || 1,
    calling_window: callingWindow,
    // Match OG's dedup_key shape — consumers may use it as an
    // idempotency hint (we don't currently, but keep parity).
    dedup_key: `batch_${batch.id}_row_${row.row_index ?? row.id}`,
    attempt_count: row.attempt_count || 0,
    max_attempts: row.max_attempts || 3,
  };
}

/**
 * Validate the batch is in a startable state and has the metadata
 * the dispatcher needs to reach /dial-out.
 */
export function validateBatch(batch) {
  if (!batch) return 'Batch not found';
  if (!STARTABLE_STATUSES.has(batch.status)) {
    return `Batch not in startable state (current: ${batch.status})`;
  }
  if (!batch.agent_name) return 'Batch missing agent_name';
  if (!batch.from_number) return 'Batch missing from_number';
  return null;
}

/**
 * Fan out all pending rows of a batch to SQS. Idempotent at the
 * row level: each row is updated to status='queued' as soon as its
 * SQS send succeeds, so a re-run of start() picks up only the rows
 * that didn't make it the first time.
 *
 * Args:
 *   pool        — pg.Pool for Aurora reads/writes
 *   sqsClient   — @aws-sdk/client-sqs SQSClient (caller-supplied so
 *                 tests can mock with a stub)
 *   batchId     — UUID of the batch to start
 *   queueUrl    — SQS queue URL (call-requests queue)
 *
 * Returns:
 *   {
 *     status: 'running' | 'completed' | 'error',
 *     batch_id, queued, total, failures: string[] | null,
 *     http_status?: number,    // present on validation errors
 *     error?: string,
 *   }
 */
export async function start(pool, sqsClient, batchId, queueUrl) {
  // 1. Load batch metadata.
  const batchRes = await pool.query(
    'SELECT * FROM voice_batches WHERE id = $1',
    [batchId],
  );
  if (!batchRes.rows.length) {
    return {
      status: 'error',
      error: 'Batch not found',
      batch_id: batchId,
      http_status: 404,
    };
  }
  const batch = batchRes.rows[0];

  const validationError = validateBatch(batch);
  if (validationError) {
    return {
      status: 'error',
      error: validationError,
      batch_id: batchId,
      http_status: validationError === 'Batch not found' ? 404 : 400,
    };
  }

  // 2. Flip the batch's status to 'running' before we start the
  // fan-out. If we crash mid-fan-out the partial-queued state is
  // recoverable: a re-run picks up only still-pending rows.
  await pool.query(
    `UPDATE voice_batches
        SET status = 'running',
            updated_at = NOW()
      WHERE id = $1`,
    [batchId],
  );

  // 3. Fetch pending rows. Note: status='pending' is the OG resting
  // state for a fresh batch_row insert. The Lambda's PUT
  // /api/batch-rows/:id flow is what bumps to 'queued' / 'dialing'
  // / terminal — we don't touch rows that aren't pending.
  const rowsRes = await pool.query(
    `SELECT id, row_index, phone_e164, case_data, attempt_count, max_attempts
       FROM voice_batch_rows
      WHERE batch_id = $1 AND status = 'pending'
      ORDER BY row_index ASC`,
    [batchId],
  );
  const rows = rowsRes.rows;

  if (!rows.length) {
    // No rows to dial — auto-finalize. Avoids a batch sitting in
    // 'running' forever when someone clicks Start on an empty/
    // already-completed batch.
    await pool.query(
      `UPDATE voice_batches
          SET status = 'completed', updated_at = NOW()
        WHERE id = $1`,
      [batchId],
    );
    return {
      status: 'completed',
      message: 'No pending rows',
      batch_id: batchId,
      queued: 0,
      total: 0,
      failures: null,
    };
  }

  // 4. Fan out. Send SQS first, then mark the row queued. If SQS
  // fails we leave the row at status='pending' so the next start()
  // run retries.
  let queued = 0;
  const failures = [];

  for (const row of rows) {
    const message = buildRowMessage(batch, row);
    try {
      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify(message),
        }),
      );
      // Atomic flip: only transition pending→queued. If the row is
      // already queued (e.g. retry of start()), the WHERE clause
      // makes this a no-op rather than a clobber.
      await pool.query(
        `UPDATE voice_batch_rows
            SET status = 'queued',
                updated_at = NOW()
          WHERE id = $1 AND status = 'pending'`,
        [row.id],
      );
      queued += 1;
    } catch (e) {
      failures.push(`row ${row.row_index ?? row.id}: ${e.message}`);
    }
  }

  return {
    status: 'running',
    batch_id: batchId,
    queued,
    total: rows.length,
    failures: failures.length ? failures : null,
  };
}
