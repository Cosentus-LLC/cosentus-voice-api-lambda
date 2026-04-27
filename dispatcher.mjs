// dispatcher.mjs — AWS Lambda entry for the SQS → bot-runner /dial-out
// dispatcher. Phase 7D, voice_call_requests path only.
//
// Triggered by an SQS event source mapping on
// medcloud-voice-call-requests with batch_size=1 and ReportBatchItemFailures
// enabled (so partial batch retries work).
//
// All decision logic lives in lib/dispatcher-core.mjs; this file only
// owns the AWS SDK plumbing (pg pool, lambda:Invoke) and the env-var
// surface. Keep it small enough to read at a glance.

import { Lambda, InvokeCommand } from '@aws-sdk/client-lambda';
import pg from 'pg';

import {
  processBatch,
  STATUS_DEAD_LETTER,
} from './lib/dispatcher-core.mjs';

// ────────────────────────────────────────────────────────────────────
// Env-var surface
//
// BOT_RUNNER_FUNCTION_NAME — name or ARN of the bot-runner Lambda.
//   Default matches the canonical name; override per-stage by setting
//   the env var on the dispatcher Lambda.
// DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS — Aurora connection.
//   Same names index.mjs uses; the bootstrap script copies them across.
// PG_POOL_MAX — pg pool size. Default 2 to match the dispatcher's
//   sequential per-record processing (1 in flight + 1 spare for DB
//   timeout reconnect). Bump for parallel-record processing.

const BOT_RUNNER_FUNCTION_NAME = process.env.BOT_RUNNER_FUNCTION_NAME
  || 'voice-agent-poc-bot-runner';

const PG_POOL_MAX = Number(process.env.PG_POOL_MAX || '2');

// ────────────────────────────────────────────────────────────────────
// Module-scope clients — instantiated once per Lambda container so
// warm invocations skip the connection-setup cost.

const lambdaClient = new Lambda({});

const pool = new pg.Pool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  max: PG_POOL_MAX,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 5000,
  // Aurora drops idle connections after ~5 minutes; reduce to 2 to
  // recycle them before that.
  ...(process.env.DB_SSL === 'true' ? { ssl: { rejectUnauthorized: false } } : {}),
});

// ────────────────────────────────────────────────────────────────────
// Deps wiring — these are the three async functions dispatcher-core
// expects. Each is intentionally thin so the unit tests in
// test/dispatcher.test.mjs can swap them with no AWS dependency.

/**
 * Atomically transition queued/pending → dialing for the given row.
 * Returns the updated row, or null if no row matched (already
 * claimed by another concurrent dispatcher, or already terminal).
 */
async function claimRow(requestId) {
  const sql = `
    UPDATE voice_call_requests
       SET status = 'dialing',
           attempt_count = COALESCE(attempt_count, 0) + 1,
           updated_at = NOW()
     WHERE id = $1
       AND status IN ('queued', 'pending')
     RETURNING *;
  `;
  const r = await pool.query(sql, [requestId]);
  return r.rows[0] || null;
}

/**
 * Best-effort status writer. Used for terminal transitions
 * (dead_letter on validation/4xx). NEVER throws — we don't want a
 * status-write failure to keep the SQS message in flight.
 */
async function markStatus(requestId, status, fields = {}) {
  try {
    const cols = ['status = $2', 'updated_at = NOW()'];
    const vals = [requestId, status];
    if (fields.error_message !== undefined) {
      vals.push(String(fields.error_message).slice(0, 1000));
      cols.push(`error_message = $${vals.length}`);
    }
    const sql = `UPDATE voice_call_requests SET ${cols.join(', ')} WHERE id = $1`;
    await pool.query(sql, vals);
  } catch (e) {
    console.error('[dispatcher] markStatus failed', {
      requestId,
      status,
      error: e.message,
    });
  }
}

/**
 * Invoke bot-runner /dial-out via lambda:invoke (NOT API Gateway).
 * Lower latency, IAM auth, no extra hop. The bot-runner's `route`
 * handler dispatches on `invocation_type=dial_out` in the body when
 * there's no `rawPath`.
 *
 * Returns a normalized { statusCode, body } shape for the core to
 * branch on. statusCode reflects the bot-runner HTTP-style response
 * (we look at FunctionError + payload.statusCode); raw Lambda
 * invocation errors throw out of here so the core treats them as
 * transient.
 */
async function callDialOut(payload) {
  const cmd = new InvokeCommand({
    FunctionName: BOT_RUNNER_FUNCTION_NAME,
    InvocationType: 'RequestResponse',
    Payload: JSON.stringify({
      // bot-runner's `route` handler uses these fields when the event
      // wasn't passed through API Gateway. See handler.py:46-83.
      invocation_type: 'dial_out',
      body: JSON.stringify(payload),
    }),
  });
  const resp = await lambdaClient.send(cmd);

  // FunctionError is set when bot-runner threw rather than returning
  // an HTTP-style response. Treat as transient (could be cold start
  // or IMDS hiccup) so SQS retries.
  if (resp.FunctionError) {
    const errPayload = resp.Payload
      ? JSON.parse(Buffer.from(resp.Payload).toString('utf8'))
      : {};
    throw new Error(
      `bot-runner FunctionError=${resp.FunctionError}: ${errPayload.errorMessage || 'unknown'}`,
    );
  }

  if (!resp.Payload) {
    throw new Error('bot-runner returned no payload');
  }

  const raw = JSON.parse(Buffer.from(resp.Payload).toString('utf8'));

  // Normalize. bot-runner returns API Gateway proxy shape:
  //   { statusCode: <int>, body: <json string>, ... }
  // Parse body to a JS value so the core can read .session_id, .detail
  // without a second JSON.parse.
  let body = raw.body;
  if (typeof body === 'string') {
    try {
      body = JSON.parse(body);
    } catch {
      /* leave as string */
    }
  }

  return {
    statusCode: raw.statusCode || 200,
    body: body ?? raw,
  };
}

// ────────────────────────────────────────────────────────────────────
// Lambda handler — kept thin. processBatch returns the partial-batch
// response shape SQS expects when ReportBatchItemFailures is on.

export const handler = async (event, context) => {
  const requestId = context?.awsRequestId;
  console.info('[dispatcher] handler invoked', {
    requestId,
    recordCount: event?.Records?.length || 0,
  });

  return processBatch({
    event,
    deps: { claimRow, markStatus, callDialOut },
    logger: console,
  });
};

// Re-export status constants so callers can verify what the dispatcher
// actually wrote without importing from lib/.
export { STATUS_DEAD_LETTER };
