// dispatcher.test.mjs — unit tests for lib/dispatcher-core.mjs.
//
// We test the core (pure logic, no AWS SDK) by injecting fake `deps`.
// This keeps the suite fast (<100ms) and means it runs the same way
// in CI, locally, and pre-deploy.
//
// Coverage map (one block per Phase 7D test ask in the scope doc):
//
//   parseSqsBody              — message-shape sanity
//   processMessage success    — happy path (queued → dialing → dispatched)
//   processMessage idempotency — concurrent + redelivery
//   processMessage failures   — 4xx, 5xx, throws, DB errors
//   processMessage validation — missing fields after claim
//   processMessage batch src  — out-of-scope source
//   processMessage case_data  — body wins, row falls back, empty default
//   processBatch              — partial batch failures + summary

import { describe, it, expect } from 'vitest';

import {
  parseSqsBody,
  validateRow,
  isWithinCallingWindow,
  processMessage,
  processBatch,
  OUTCOME_DISPATCHED,
  OUTCOME_ALREADY_PROCESSED,
  OUTCOME_PERMANENT_FAILURE,
  OUTCOME_TRANSIENT_FAILURE,
  OUTCOME_BAD_MESSAGE,
  OUTCOME_DEFERRED_OUTSIDE_WINDOW,
  OUTCOME_DEFERRED_PAUSED,
  OUTCOME_CANCELLED,
  STATUS_DEAD_LETTER,
} from '../lib/dispatcher-core.mjs';

// ────────────────────────────────────────────────────────────────────
// Helpers

const REQUEST_ID = '11111111-1111-1111-1111-111111111111';

function record({ body, messageId = 'm-1', receiveCount = 1 } = {}) {
  return {
    messageId,
    body: typeof body === 'string' ? body : JSON.stringify(body),
    attributes: { ApproximateReceiveCount: String(receiveCount) },
  };
}

function validRowFromBody() {
  return {
    id: REQUEST_ID,
    agent_name: 'chris-claim-status',
    to_number: '+19494360836',
    from_number: '+12098075018',
    case_data: { Patient_Name: 'Alex' },
    status: 'dialing', // post-claim
    attempt_count: 1,
  };
}

function makeDeps(overrides = {}) {
  const calls = { claim: [], dialOut: [], markStatus: [] };
  const deps = {
    async claimRow(id) {
      calls.claim.push(id);
      return validRowFromBody();
    },
    async callDialOut(payload) {
      calls.dialOut.push(payload);
      return {
        statusCode: 200,
        body: { session_id: 'voice-out-abc', status: 'started' },
      };
    },
    async markStatus(id, status, fields) {
      calls.markStatus.push({ id, status, fields });
    },
    ...overrides,
  };
  return { deps, calls };
}

// silence the dispatcher logger during tests; assertions look at deps
// state, not the log output.
const silentLogger = {
  info() {},
  warn() {},
  error() {},
};

// ────────────────────────────────────────────────────────────────────
// parseSqsBody

describe('parseSqsBody', () => {
  it('parses a valid call_request body', () => {
    const out = parseSqsBody(JSON.stringify({
      request_id: REQUEST_ID,
      to_number: '+15551234567',
    }));
    expect(out.kind).toBe('call_request');
    expect(out.requestId).toBe(REQUEST_ID);
  });

  it('defaults source to call_request when absent', () => {
    const out = parseSqsBody(JSON.stringify({ request_id: REQUEST_ID }));
    expect(out.kind).toBe('call_request');
  });

  it('flags batch source out of scope', () => {
    const out = parseSqsBody(JSON.stringify({
      source: 'batch',
      batch_id: 'b1',
      batch_row_id: 'r1',
    }));
    expect(out.kind).toBe('batch');
  });

  it('rejects empty body', () => {
    const out = parseSqsBody('');
    expect(out.kind).toBe('malformed');
  });

  it('rejects non-string body', () => {
    const out = parseSqsBody(null);
    expect(out.kind).toBe('malformed');
  });

  it('rejects malformed JSON', () => {
    const out = parseSqsBody('{not json');
    expect(out.kind).toBe('malformed');
  });

  it('rejects non-object JSON', () => {
    const out = parseSqsBody('"a string"');
    expect(out.kind).toBe('malformed');
  });

  it('rejects unknown source values', () => {
    const out = parseSqsBody(JSON.stringify({ source: 'twilio' }));
    expect(out.kind).toBe('malformed');
  });

  it('rejects missing request_id', () => {
    const out = parseSqsBody(JSON.stringify({ to_number: '+15551234567' }));
    expect(out.kind).toBe('malformed');
  });
});

// ────────────────────────────────────────────────────────────────────
// validateRow

describe('validateRow', () => {
  it('passes a valid row', () => {
    expect(validateRow(validRowFromBody())).toBeNull();
  });

  it('flags null row', () => {
    expect(validateRow(null)).toBe('Row not found');
  });

  it('flags missing to_number', () => {
    const r = validRowFromBody();
    r.to_number = '';
    expect(validateRow(r)).toMatch(/to_number/);
  });

  it('flags missing agent_name', () => {
    const r = validRowFromBody();
    r.agent_name = '';
    expect(validateRow(r)).toMatch(/agent_name/);
  });

  it('allows missing from_number (Daily uses default)', () => {
    const r = validRowFromBody();
    r.from_number = null;
    expect(validateRow(r)).toBeNull();
  });
});

// ────────────────────────────────────────────────────────────────────
// processMessage — happy path

describe('processMessage success path', () => {
  it('dispatches a fresh queued row', async () => {
    const { deps, calls } = makeDeps();
    const sqsRecord = record({
      body: { request_id: REQUEST_ID, case_data: { foo: 'bar' } },
    });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_DISPATCHED);
    expect(result.requestId).toBe(REQUEST_ID);
    expect(calls.claim).toEqual([REQUEST_ID]);
    expect(calls.dialOut).toHaveLength(1);
    expect(calls.markStatus).toHaveLength(0); // pipeline writes terminal status, not us
  });

  it('passes through agent_name as agent_id to bot-runner', async () => {
    // bot-runner accepts either UUID or name. We pass agent_name
    // because that's what voice_call_requests stores. Verify.
    const { deps, calls } = makeDeps();
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(calls.dialOut[0].agent_id).toBe('chris-claim-status');
  });

  it('omits from_number when row has none (lets Daily pick default)', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        const r = validRowFromBody();
        r.from_number = null;
        return r;
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(calls.dialOut[0].from_number).toBeUndefined();
  });
});

// ────────────────────────────────────────────────────────────────────
// processMessage — case_data resolution
//
// Spec note: body wins, row is fallback, empty {} is the floor.

describe('processMessage case_data resolution', () => {
  it('uses body.case_data when present (freshest)', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        const r = validRowFromBody();
        r.case_data = { row: 'data' };
        return r;
      },
    });
    const sqsRecord = record({
      body: { request_id: REQUEST_ID, case_data: { body: 'data' } },
    });

    await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(calls.dialOut[0].case_data).toEqual({ body: 'data' });
  });

  it('falls back to row.case_data when body has none', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        const r = validRowFromBody();
        r.case_data = { row: 'data' };
        return r;
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(calls.dialOut[0].case_data).toEqual({ row: 'data' });
  });

  it('defaults to {} when neither body nor row has case_data', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        const r = validRowFromBody();
        r.case_data = null;
        return r;
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(calls.dialOut[0].case_data).toEqual({});
  });
});

// ────────────────────────────────────────────────────────────────────
// processMessage — idempotency
//
// Two concurrent SQS messages for the same row should result in EXACTLY
// ONE /dial-out call. The atomic claim is what enforces this — the
// loser's claimRow returns null (0 rows updated).

describe('processMessage idempotency', () => {
  it('skips when row already claimed (claimRow returns null)', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        return null; // someone else got there first
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_ALREADY_PROCESSED);
    expect(calls.dialOut).toHaveLength(0);
  });

  it('skips when row is missing entirely (claimRow returns null)', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        return null;
      },
    });
    const sqsRecord = record({
      body: { request_id: REQUEST_ID },
      messageId: 'm-redelivered',
      receiveCount: 3,
    });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_ALREADY_PROCESSED);
    expect(calls.dialOut).toHaveLength(0);
  });
});

// ────────────────────────────────────────────────────────────────────
// processMessage — failures

describe('processMessage failures', () => {
  it('marks dead_letter on bot-runner 4xx', async () => {
    const { deps, calls } = makeDeps({
      async callDialOut() {
        return {
          statusCode: 400,
          body: { detail: 'Missing required field: agent_id' },
        };
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_PERMANENT_FAILURE);
    expect(calls.markStatus).toHaveLength(1);
    expect(calls.markStatus[0].status).toBe(STATUS_DEAD_LETTER);
    expect(calls.markStatus[0].fields.error_message).toMatch(/bot_runner_400/);
  });

  it('marks dead_letter on bot-runner 422', async () => {
    const { deps, calls } = makeDeps({
      async callDialOut() {
        return { statusCode: 422, body: { error: 'invalid e164' } };
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_PERMANENT_FAILURE);
    expect(calls.markStatus[0].status).toBe(STATUS_DEAD_LETTER);
  });

  it('returns transient on bot-runner 503 (Daily not ready)', async () => {
    const { deps, calls } = makeDeps({
      async callDialOut() {
        return { statusCode: 503, body: { detail: 'Daily dial-out not enabled' } };
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_TRANSIENT_FAILURE);
    expect(result.retryable).toBe(true);
    expect(calls.markStatus).toHaveLength(0); // leave row 'dialing' for retry
  });

  it('returns transient on bot-runner 500', async () => {
    const { deps, calls } = makeDeps({
      async callDialOut() {
        return { statusCode: 500, body: { detail: 'Internal server error' } };
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_TRANSIENT_FAILURE);
    expect(calls.markStatus).toHaveLength(0);
  });

  it('returns transient when callDialOut throws', async () => {
    const { deps, calls } = makeDeps({
      async callDialOut() {
        throw new Error('cold start timeout');
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_TRANSIENT_FAILURE);
    expect(result.retryable).toBe(true);
    expect(calls.markStatus).toHaveLength(0);
  });

  it('returns transient when claimRow throws (DB error)', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        throw new Error('connection terminated');
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_TRANSIENT_FAILURE);
    expect(result.retryable).toBe(true);
    expect(calls.dialOut).toHaveLength(0);
  });
});

// ────────────────────────────────────────────────────────────────────
// processMessage — validation after claim

describe('processMessage post-claim validation', () => {
  it('marks dead_letter when claimed row has no to_number', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        const r = validRowFromBody();
        r.to_number = '';
        return r;
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_PERMANENT_FAILURE);
    expect(calls.markStatus[0].status).toBe(STATUS_DEAD_LETTER);
    expect(calls.markStatus[0].fields.error_message).toMatch(/to_number/);
    expect(calls.dialOut).toHaveLength(0);
  });

  it('marks dead_letter when claimed row has no agent_name', async () => {
    const { deps, calls } = makeDeps({
      async claimRow() {
        const r = validRowFromBody();
        r.agent_name = '';
        return r;
      },
    });
    const sqsRecord = record({ body: { request_id: REQUEST_ID } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_PERMANENT_FAILURE);
    expect(calls.markStatus[0].fields.error_message).toMatch(/agent_name/);
  });
});

// ────────────────────────────────────────────────────────────────────
// processMessage — bad message + out of scope

describe('processMessage bad messages', () => {
  it('returns bad_message on malformed JSON (no claim, no dispatch)', async () => {
    const { deps, calls } = makeDeps();
    const sqsRecord = record({ body: '{not json' });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_BAD_MESSAGE);
    expect(calls.claim).toHaveLength(0);
    expect(calls.dialOut).toHaveLength(0);
  });

  it('returns bad_message on missing request_id', async () => {
    const { deps, calls } = makeDeps();
    const sqsRecord = record({ body: { to_number: '+15551234567' } });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_BAD_MESSAGE);
    expect(calls.claim).toHaveLength(0);
  });

  // Batch source IS handled now (Phase 7D PR 2). The "out of scope"
  // assertion was correct for PR 1; full batch coverage lives in
  // describe('processMessage batch source') below.
});

// ────────────────────────────────────────────────────────────────────
// isWithinCallingWindow — pure helper used by the batch path.
// We can't test "what time is it" deterministically without an
// injected `now`, which the helper accepts.

describe('isWithinCallingWindow', () => {
  // 2026-04-27 is a Monday. Use it consistently for these tests so
  // the days-of-week math is stable.
  const monday1030UTC = new Date('2026-04-27T10:30:00Z');
  const monday0300UTC = new Date('2026-04-27T03:00:00Z');
  // 2026-04-25 is a Saturday.
  const saturday1500UTC = new Date('2026-04-25T15:00:00Z');

  it('fails open on null window (no metadata = always allowed)', () => {
    expect(isWithinCallingWindow(null, monday1030UTC)).toBe(true);
  });

  it('fails open on malformed window (string instead of object)', () => {
    expect(isWithinCallingWindow('not an object', monday1030UTC)).toBe(true);
  });

  it('passes when within window (default Mon-Fri 09-17 ET)', () => {
    // 10:30 UTC = 06:30 ET (still before 09:00) → outside the
    // default window. So pick UTC time that's IN window:
    // 14:00 UTC = 10:00 ET → IN window.
    const monday14UTC = new Date('2026-04-27T14:00:00Z');
    expect(isWithinCallingWindow({
      start: '09:00',
      end: '17:00',
      timezone: 'America/New_York',
      days: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'],
    }, monday14UTC)).toBe(true);
  });

  it('rejects when outside window time', () => {
    // 03:00 UTC Monday = 23:00 Sun ET → wrong day in ET tz.
    expect(isWithinCallingWindow({
      start: '09:00',
      end: '17:00',
      timezone: 'America/New_York',
      days: ['Mon'],
    }, monday0300UTC)).toBe(false);
  });

  it('rejects on wrong weekday', () => {
    expect(isWithinCallingWindow({
      start: '00:00',
      end: '23:59',
      timezone: 'UTC',
      days: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'],
    }, saturday1500UTC)).toBe(false);
  });

  it('honors custom timezone', () => {
    // 10:30 UTC = 19:30 in Asia/Tokyo (JST = UTC+9). Not in 09-17.
    expect(isWithinCallingWindow({
      start: '09:00',
      end: '17:00',
      timezone: 'Asia/Tokyo',
      days: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'],
    }, monday1030UTC)).toBe(false);
  });

  it('fails open on bad timezone (refuses to block dispatch)', () => {
    expect(isWithinCallingWindow({
      start: '09:00',
      end: '17:00',
      timezone: 'Not/AReal/Zone',
      days: ['Mon'],
    }, monday1030UTC)).toBe(true);
  });

  it('uses defaults when window has missing fields', () => {
    // No days specified → falls back to Mon-Fri.
    expect(isWithinCallingWindow({
      start: '00:00',
      end: '23:59',
      timezone: 'UTC',
    }, saturday1500UTC)).toBe(false);
  });
});

// ────────────────────────────────────────────────────────────────────
// processMessage — batch source path
//
// The batch path mirrors call_request structurally but adds
// pause/cancel/window deferral semantics. The deps surface is a
// superset: getBatch / claimBatchRow / markBatchRowStatus /
// changeMessageVisibility on top of callDialOut.

const BATCH_ID = '99999999-9999-9999-9999-999999999999';
const BATCH_ROW_ID = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';

function batchRecord({
  body, messageId = 'm-batch-1', receiveCount = 1, receiptHandle = 'rh-1',
} = {}) {
  return {
    messageId,
    receiptHandle,
    body: typeof body === 'string' ? body : JSON.stringify(body),
    attributes: { ApproximateReceiveCount: String(receiveCount) },
  };
}

function batchBodyDefaults(overrides = {}) {
  return {
    source: 'batch',
    batch_id: BATCH_ID,
    batch_row_id: BATCH_ROW_ID,
    row_index: 7,
    agent_name: 'chris-claim-status',
    to_number: '+19494360836',
    from_number: '+12098075018',
    case_data: { Patient_Name: 'Batch Test' },
    // Use a permissive window by default so tests don't have to
    // mock the clock. Override per-test.
    calling_window: {
      start: '00:00',
      end: '23:59',
      timezone: 'UTC',
      days: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
    },
    ...overrides,
  };
}

function makeBatchDeps(overrides = {}) {
  const calls = {
    getBatch: [],
    claimBatchRow: [],
    markBatchRowStatus: [],
    changeMessageVisibility: [],
    dialOut: [],
  };
  const deps = {
    async getBatch(id) {
      calls.getBatch.push(id);
      return { id, status: 'running', paused_by_user: false };
    },
    async claimBatchRow(id) {
      calls.claimBatchRow.push(id);
      return {
        id,
        batch_id: BATCH_ID,
        row_index: 7,
        phone_e164: '+19494360836',
        case_data: { Patient_Name: 'Batch Test' },
        status: 'dialing',
        attempt_count: 1,
      };
    },
    async markBatchRowStatus(id, status, fields) {
      calls.markBatchRowStatus.push({ id, status, fields });
    },
    async changeMessageVisibility(receiptHandle, secs) {
      calls.changeMessageVisibility.push({ receiptHandle, secs });
    },
    async callDialOut(payload) {
      calls.dialOut.push(payload);
      return {
        statusCode: 200,
        body: { session_id: 'voice-out-batch-aaaa', status: 'started' },
      };
    },
    // call_request deps unused for these tests but provided so
    // makeBatchDeps is interchangeable with makeDeps when needed.
    async claimRow() { return null; },
    async markStatus() {},
    ...overrides,
  };
  return { deps, calls };
}

describe('processMessage batch source — happy path', () => {
  it('dispatches a fresh batch row', async () => {
    const { deps, calls } = makeBatchDeps();
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_DISPATCHED);
    expect(result.batchRowId).toBe(BATCH_ROW_ID);
    expect(calls.getBatch).toEqual([BATCH_ID]);
    expect(calls.claimBatchRow).toEqual([BATCH_ROW_ID]);
    expect(calls.dialOut).toHaveLength(1);
    expect(calls.markBatchRowStatus).toHaveLength(0);
  });

  it('passes batch metadata in dial-out payload (for after-call hook)', async () => {
    const { deps, calls } = makeBatchDeps();
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(calls.dialOut[0].batch_id).toBe(BATCH_ID);
    expect(calls.dialOut[0].batch_row_index).toBe(7);
    expect(calls.dialOut[0].batch_row_id).toBe(BATCH_ROW_ID);
    expect(calls.dialOut[0].agent_id).toBe('chris-claim-status');
    expect(calls.dialOut[0].to_number).toBe('+19494360836');
    expect(calls.dialOut[0].from_number).toBe('+12098075018');
    expect(calls.dialOut[0].case_data).toEqual({ Patient_Name: 'Batch Test' });
  });
});

describe('processMessage batch source — paused / cancelled', () => {
  it('defers when batch is paused (status=paused)', async () => {
    const { deps, calls } = makeBatchDeps({
      async getBatch(id) {
        return { id, status: 'paused', paused_by_user: true };
      },
    });
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_DEFERRED_PAUSED);
    expect(result.defer_secs).toBe(60);
    expect(calls.changeMessageVisibility).toHaveLength(1);
    expect(calls.changeMessageVisibility[0].secs).toBe(60);
    // Crucial: do NOT claim the row when deferring. Pause must be
    // a no-op on the row's state.
    expect(calls.claimBatchRow).toHaveLength(0);
    expect(calls.dialOut).toHaveLength(0);
  });

  it('defers when batch is paused via paused_by_user only', async () => {
    const { deps, calls } = makeBatchDeps({
      async getBatch(id) {
        // status is still 'running' but paused_by_user flipped via API
        return { id, status: 'running', paused_by_user: true };
      },
    });
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_DEFERRED_PAUSED);
    expect(calls.dialOut).toHaveLength(0);
  });

  it('marks row cancelled when batch is cancelled (British spelling)', async () => {
    const { deps, calls } = makeBatchDeps({
      async getBatch(id) {
        return { id, status: 'cancelled', paused_by_user: false };
      },
    });
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_CANCELLED);
    expect(calls.markBatchRowStatus).toHaveLength(1);
    expect(calls.markBatchRowStatus[0].status).toBe('cancelled');
    expect(calls.dialOut).toHaveLength(0);
    // No defer — cancel is terminal, ack the message.
    expect(calls.changeMessageVisibility).toHaveLength(0);
  });

  it('marks row cancelled when batch is canceled (American spelling)', async () => {
    // The Lambda's POST /api/batches/:id/cancel writes 'canceled'
    // (American). OG consumer code branched on 'cancelled'
    // (British). Dispatcher accepts both.
    const { deps, calls } = makeBatchDeps({
      async getBatch(id) {
        return { id, status: 'canceled', paused_by_user: false };
      },
    });
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_CANCELLED);
    expect(calls.markBatchRowStatus[0].status).toBe('cancelled');
  });
});

describe('processMessage batch source — calling window', () => {
  it('defers when outside calling window', async () => {
    const { deps, calls } = makeBatchDeps();
    // Permissive timezone but RESTRICTIVE day-of-week — Saturday-only
    // window dispatching during a non-Saturday should defer.
    const today = new Date();
    const isSaturday = today.getUTCDay() === 6;
    const onlyDay = isSaturday ? 'Sun' : 'Sat';

    const body = batchBodyDefaults({
      calling_window: {
        start: '00:00',
        end: '23:59',
        timezone: 'UTC',
        days: [onlyDay],
      },
    });
    const sqsRecord = batchRecord({ body });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_DEFERRED_OUTSIDE_WINDOW);
    expect(result.defer_secs).toBe(300);
    expect(calls.changeMessageVisibility).toHaveLength(1);
    expect(calls.changeMessageVisibility[0].secs).toBe(300);
    expect(calls.claimBatchRow).toHaveLength(0);
    expect(calls.dialOut).toHaveLength(0);
  });

  it('proceeds when within calling window', async () => {
    const { deps, calls } = makeBatchDeps();
    const sqsRecord = batchRecord({
      body: batchBodyDefaults({
        calling_window: {
          start: '00:00',
          end: '23:59',
          timezone: 'UTC',
          days: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
        },
      }),
    });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_DISPATCHED);
    expect(calls.changeMessageVisibility).toHaveLength(0);
  });
});

describe('processMessage batch source — idempotency', () => {
  it('skips when batch row already claimed', async () => {
    const { deps, calls } = makeBatchDeps({
      async claimBatchRow() {
        return null; // someone else claimed it / already terminal
      },
    });
    const sqsRecord = batchRecord({
      body: batchBodyDefaults(),
      receiveCount: 2,
    });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_ALREADY_PROCESSED);
    expect(calls.dialOut).toHaveLength(0);
    expect(calls.markBatchRowStatus).toHaveLength(0);
  });

  it('returns transient when getBatch throws (DB error)', async () => {
    const { deps, calls } = makeBatchDeps({
      async getBatch() {
        throw new Error('connection terminated');
      },
    });
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_TRANSIENT_FAILURE);
    expect(result.retryable).toBe(true);
    expect(calls.dialOut).toHaveLength(0);
  });
});

describe('processMessage batch source — failures', () => {
  it('marks row failed on bot-runner 4xx', async () => {
    const { deps, calls } = makeBatchDeps({
      async callDialOut() {
        return { statusCode: 400, body: { detail: 'bad agent' } };
      },
    });
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_PERMANENT_FAILURE);
    expect(calls.markBatchRowStatus).toHaveLength(1);
    expect(calls.markBatchRowStatus[0].status).toBe('failed');
    expect(calls.markBatchRowStatus[0].fields.error_message).toMatch(/bot_runner_400/);
  });

  it('returns transient on bot-runner 503 (Daily not ready)', async () => {
    const { deps, calls } = makeBatchDeps({
      async callDialOut() {
        return { statusCode: 503, body: { detail: 'Daily dial-out not enabled' } };
      },
    });
    const sqsRecord = batchRecord({ body: batchBodyDefaults() });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_TRANSIENT_FAILURE);
    expect(result.retryable).toBe(true);
    // Don't clobber the row's status — leave at 'dialing' for retry.
    expect(calls.markBatchRowStatus).toHaveLength(0);
  });

  it('marks row failed when validation fails (bad to_number)', async () => {
    const { deps, calls } = makeBatchDeps({
      async claimBatchRow(id) {
        return {
          id,
          batch_id: BATCH_ID,
          row_index: 7,
          phone_e164: '', // empty — validation should reject
          case_data: {},
          status: 'dialing',
          attempt_count: 1,
        };
      },
    });
    const sqsRecord = batchRecord({
      body: batchBodyDefaults({ to_number: '' }),
    });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_PERMANENT_FAILURE);
    expect(calls.markBatchRowStatus).toHaveLength(1);
    expect(calls.markBatchRowStatus[0].status).toBe('failed');
    expect(calls.markBatchRowStatus[0].fields.error_message).toMatch(/to_number/);
    expect(calls.dialOut).toHaveLength(0);
  });

  it('rejects body missing batch_id or batch_row_id', async () => {
    const { deps, calls } = makeBatchDeps();
    const sqsRecord = batchRecord({
      body: { source: 'batch', /* no batch_id / batch_row_id */ },
    });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_BAD_MESSAGE);
    expect(calls.getBatch).toHaveLength(0);
  });
});

// ────────────────────────────────────────────────────────────────────
// processBatch

describe('processBatch', () => {
  it('returns empty failures on empty event', async () => {
    const { deps } = makeDeps();
    const result = await processBatch({
      event: { Records: [] },
      deps,
      logger: silentLogger,
    });
    expect(result).toEqual({ batchItemFailures: [] });
  });

  it('handles a successful batch with no partial failures', async () => {
    const { deps, calls } = makeDeps();
    const result = await processBatch({
      event: {
        Records: [
          record({ body: { request_id: REQUEST_ID }, messageId: 'm-1' }),
          record({
            body: { request_id: '22222222-2222-2222-2222-222222222222' },
            messageId: 'm-2',
          }),
        ],
      },
      deps,
      logger: silentLogger,
    });
    expect(result.batchItemFailures).toEqual([]);
    expect(calls.dialOut).toHaveLength(2);
  });

  it('reports only the failing record in batchItemFailures', async () => {
    let count = 0;
    const { deps } = makeDeps({
      async callDialOut() {
        count += 1;
        if (count === 1) {
          return { statusCode: 200, body: { session_id: 'ok' } };
        }
        return { statusCode: 500, body: { detail: 'transient' } };
      },
    });
    const result = await processBatch({
      event: {
        Records: [
          record({ body: { request_id: REQUEST_ID }, messageId: 'm-ok' }),
          record({
            body: { request_id: '22222222-2222-2222-2222-222222222222' },
            messageId: 'm-fail',
          }),
        ],
      },
      deps,
      logger: silentLogger,
    });
    expect(result.batchItemFailures).toEqual([
      { itemIdentifier: 'm-fail' },
    ]);
  });

  it('does NOT report 4xx as a batch failure (permanent → ack)', async () => {
    const { deps } = makeDeps({
      async callDialOut() {
        return { statusCode: 400, body: { detail: 'bad' } };
      },
    });
    const result = await processBatch({
      event: {
        Records: [record({
          body: { request_id: REQUEST_ID },
          messageId: 'm-perm',
        })],
      },
      deps,
      logger: silentLogger,
    });
    expect(result.batchItemFailures).toEqual([]);
  });

  it('does NOT report bad_message as a batch failure (DLQ via SQS)', async () => {
    const { deps } = makeDeps();
    const result = await processBatch({
      event: {
        Records: [record({ body: '{not json', messageId: 'm-bad' })],
      },
      deps,
      logger: silentLogger,
    });
    expect(result.batchItemFailures).toEqual([]);
  });

  it('treats unhandled exceptions as transient failures', async () => {
    const { deps } = makeDeps({
      async claimRow() {
        // Synchronous throw inside an async function — should still be
        // caught by processBatch's try/catch and reported as a partial
        // batch failure.
        throw new Error('unexpected');
      },
    });
    const result = await processBatch({
      event: {
        Records: [record({
          body: { request_id: REQUEST_ID },
          messageId: 'm-explode',
        })],
      },
      deps,
      logger: silentLogger,
    });
    expect(result.batchItemFailures).toEqual([
      { itemIdentifier: 'm-explode' },
    ]);
  });
});
