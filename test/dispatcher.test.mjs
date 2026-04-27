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
  processMessage,
  processBatch,
  OUTCOME_DISPATCHED,
  OUTCOME_ALREADY_PROCESSED,
  OUTCOME_PERMANENT_FAILURE,
  OUTCOME_TRANSIENT_FAILURE,
  OUTCOME_BAD_MESSAGE,
  OUTCOME_NOT_THIS_DISPATCHER,
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

  it('skips batch source (out of scope this PR)', async () => {
    const { deps, calls } = makeDeps();
    const sqsRecord = record({
      body: { source: 'batch', batch_id: 'b1', batch_row_id: 'r1' },
    });

    const result = await processMessage({ sqsRecord, deps, logger: silentLogger });

    expect(result.outcome).toBe(OUTCOME_NOT_THIS_DISPATCHER);
    expect(calls.claim).toHaveLength(0);
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
