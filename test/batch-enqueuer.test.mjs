// batch-enqueuer.test.mjs — unit tests for lib/batch-enqueuer.mjs.
//
// We test the full module by injecting a fake pg.Pool (a minimal
// stub that records query() calls) and a fake SQS client. The pure
// helpers (buildRowMessage, validateBatch) are tested separately
// without any I/O.

import { describe, it, expect } from 'vitest';

import {
  start,
  buildRowMessage,
  validateBatch,
} from '../lib/batch-enqueuer.mjs';

const BATCH_ID = '11111111-1111-1111-1111-111111111111';
const QUEUE_URL = 'https://sqs.test.amazonaws.com/000/test-queue';

function fakeBatch(overrides = {}) {
  return {
    id: BATCH_ID,
    name: 'Test Batch',
    agent_name: 'chris-claim-status',
    from_number: '+12098075018',
    status: 'ready',
    concurrency: 5,
    timezone: 'America/New_York',
    calling_window_start: '09:00',
    calling_window_end: '17:00',
    calling_window_days: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'],
    ...overrides,
  };
}

function fakeRow(rowIndex, overrides = {}) {
  return {
    id: `row-${rowIndex}`,
    row_index: rowIndex,
    phone_e164: `+1555000${String(rowIndex).padStart(4, '0')}`,
    case_data: { Patient_Name: `Patient ${rowIndex}` },
    attempt_count: 0,
    max_attempts: 3,
    ...overrides,
  };
}

/**
 * Minimal pg.Pool stub. Records every query() call and returns a
 * preset response sequence in order. Fails loudly if the test calls
 * more queries than responses were supplied.
 */
function fakePool(responses) {
  const calls = [];
  let i = 0;
  return {
    calls,
    async query(sql, params) {
      calls.push({ sql, params });
      if (i >= responses.length) {
        throw new Error(`fakePool: query #${i + 1} not stubbed: ${sql.slice(0, 60)}`);
      }
      const r = responses[i];
      i += 1;
      if (r instanceof Error) throw r;
      return r;
    },
  };
}

function fakeSqs() {
  const sends = [];
  return {
    sends,
    async send(cmd) {
      // SendMessageCommand carries .input.QueueUrl + .input.MessageBody
      // for AWS SDK v3.
      sends.push(cmd.input || cmd);
      return { MessageId: `msg-${sends.length}` };
    },
  };
}

// ────────────────────────────────────────────────────────────────────
// Pure helpers

describe('buildRowMessage', () => {
  it('builds the OG batch wire format', () => {
    const batch = fakeBatch();
    const row = fakeRow(0);
    const msg = buildRowMessage(batch, row);

    expect(msg.source).toBe('batch');
    expect(msg.batch_id).toBe(BATCH_ID);
    expect(msg.batch_row_id).toBe('row-0');
    expect(msg.row_index).toBe(0);
    expect(msg.agent_name).toBe('chris-claim-status');
    expect(msg.to_number).toBe('+15550000000');
    expect(msg.from_number).toBe('+12098075018');
    expect(msg.case_data).toEqual({ Patient_Name: 'Patient 0' });
    expect(msg.concurrency).toBe(5);
    expect(msg.calling_window).toEqual({
      start: '09:00',
      end: '17:00',
      timezone: 'America/New_York',
      days: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'],
    });
    expect(msg.dedup_key).toBe(`batch_${BATCH_ID}_row_0`);
    expect(msg.attempt_count).toBe(0);
    expect(msg.max_attempts).toBe(3);
  });

  it('falls back to defaults when batch has no calling window', () => {
    const batch = fakeBatch({
      calling_window_start: null,
      calling_window_end: null,
      calling_window_days: null,
      timezone: null,
    });
    const row = fakeRow(1);
    const msg = buildRowMessage(batch, row);

    expect(msg.calling_window.start).toBe('00:00');
    expect(msg.calling_window.end).toBe('23:59');
    expect(msg.calling_window.timezone).toBe('America/New_York');
    expect(msg.calling_window.days).toEqual(['Mon', 'Tue', 'Wed', 'Thu', 'Fri']);
  });

  it('preserves null row_index in dedup_key when row.row_index missing', () => {
    const batch = fakeBatch();
    const row = { ...fakeRow(0), row_index: undefined };
    const msg = buildRowMessage(batch, row);
    // Falls back to row.id in the dedup_key. Some hand-inserted rows
    // may not have row_index; we keep the dedup_key unique.
    expect(msg.dedup_key).toBe(`batch_${BATCH_ID}_row_row-0`);
  });

  it('defaults case_data to {} when row has none', () => {
    const batch = fakeBatch();
    const row = { ...fakeRow(0), case_data: null };
    const msg = buildRowMessage(batch, row);
    expect(msg.case_data).toEqual({});
  });
});

describe('validateBatch', () => {
  it('passes a startable batch', () => {
    expect(validateBatch(fakeBatch())).toBeNull();
  });

  it('flags missing batch (null)', () => {
    expect(validateBatch(null)).toBe('Batch not found');
  });

  it('rejects non-startable status', () => {
    expect(validateBatch(fakeBatch({ status: 'completed' }))).toMatch(/not in startable state/);
  });

  it('accepts running status (re-start of partial)', () => {
    expect(validateBatch(fakeBatch({ status: 'running' }))).toBeNull();
  });

  it('accepts paused status (resume after pause)', () => {
    expect(validateBatch(fakeBatch({ status: 'paused' }))).toBeNull();
  });

  it('flags missing agent_name', () => {
    expect(validateBatch(fakeBatch({ agent_name: '' }))).toMatch(/agent_name/);
  });

  it('flags missing from_number', () => {
    expect(validateBatch(fakeBatch({ from_number: null }))).toMatch(/from_number/);
  });
});

// ────────────────────────────────────────────────────────────────────
// start() — integration with stubbed pool + sqs

describe('start()', () => {
  it('returns 404-ish when batch is missing', async () => {
    const pool = fakePool([
      { rows: [] },                  // SELECT batch
    ]);
    const sqs = fakeSqs();

    const result = await start(pool, sqs, BATCH_ID, QUEUE_URL);

    expect(result.status).toBe('error');
    expect(result.http_status).toBe(404);
    expect(result.error).toBe('Batch not found');
    expect(sqs.sends).toHaveLength(0);
  });

  it('returns 400-ish when batch is in non-startable status', async () => {
    const pool = fakePool([
      { rows: [fakeBatch({ status: 'completed' })] },
    ]);
    const sqs = fakeSqs();

    const result = await start(pool, sqs, BATCH_ID, QUEUE_URL);

    expect(result.status).toBe('error');
    expect(result.http_status).toBe(400);
    expect(result.error).toMatch(/not in startable state/);
  });

  it('auto-finalizes when there are no pending rows', async () => {
    const pool = fakePool([
      { rows: [fakeBatch()] },                            // SELECT batch
      { rowCount: 1, rows: [] },                          // UPDATE → running
      { rows: [] },                                       // SELECT pending rows: empty
      { rowCount: 1, rows: [] },                          // UPDATE → completed
    ]);
    const sqs = fakeSqs();

    const result = await start(pool, sqs, BATCH_ID, QUEUE_URL);

    expect(result.status).toBe('completed');
    expect(result.queued).toBe(0);
    expect(sqs.sends).toHaveLength(0);
    // Last UPDATE must flip status to 'completed'
    const lastSql = pool.calls[pool.calls.length - 1].sql;
    expect(lastSql).toMatch(/SET status = 'completed'/);
  });

  it('fans out one SQS message per pending row + flips each to queued', async () => {
    const rows = [fakeRow(0), fakeRow(1), fakeRow(2)];
    const pool = fakePool([
      { rows: [fakeBatch()] },                            // SELECT batch
      { rowCount: 1, rows: [] },                          // UPDATE → running
      { rows },                                            // SELECT pending rows
      { rowCount: 1, rows: [] },                          // UPDATE row 0 → queued
      { rowCount: 1, rows: [] },                          // UPDATE row 1 → queued
      { rowCount: 1, rows: [] },                          // UPDATE row 2 → queued
    ]);
    const sqs = fakeSqs();

    const result = await start(pool, sqs, BATCH_ID, QUEUE_URL);

    expect(result.status).toBe('running');
    expect(result.queued).toBe(3);
    expect(result.total).toBe(3);
    expect(result.failures).toBeNull();

    // SQS send count + per-message body shape
    expect(sqs.sends).toHaveLength(3);
    for (let i = 0; i < 3; i += 1) {
      const body = JSON.parse(sqs.sends[i].MessageBody);
      expect(body.source).toBe('batch');
      expect(body.batch_id).toBe(BATCH_ID);
      expect(body.batch_row_id).toBe(`row-${i}`);
      expect(body.row_index).toBe(i);
      expect(body.agent_name).toBe('chris-claim-status');
    }
  });

  it('records SQS failures but continues with remaining rows', async () => {
    const rows = [fakeRow(0), fakeRow(1)];
    const pool = fakePool([
      { rows: [fakeBatch()] },                            // SELECT batch
      { rowCount: 1, rows: [] },                          // UPDATE → running
      { rows },                                            // SELECT pending rows
      // Note: row 0's UPDATE never fires because SQS send fails.
      { rowCount: 1, rows: [] },                          // UPDATE row 1 → queued
    ]);
    let sendCount = 0;
    const sqs = {
      sends: [],
      async send(cmd) {
        sendCount += 1;
        if (sendCount === 1) throw new Error('SQS unavailable');
        this.sends.push(cmd.input || cmd);
        return { MessageId: 'msg-2' };
      },
    };

    const result = await start(pool, sqs, BATCH_ID, QUEUE_URL);

    expect(result.queued).toBe(1);
    expect(result.total).toBe(2);
    expect(result.failures).toHaveLength(1);
    expect(result.failures[0]).toMatch(/row 0/);
    expect(result.failures[0]).toMatch(/SQS unavailable/);
  });

  it('atomic UPDATE only flips pending → queued (not other states)', async () => {
    // Confirm the row UPDATE clause has the WHERE status='pending' guard
    // — that's what makes start() safe to re-run.
    const pool = fakePool([
      { rows: [fakeBatch()] },                            // SELECT batch
      { rowCount: 1, rows: [] },                          // UPDATE → running
      { rows: [fakeRow(0)] },                              // SELECT pending rows
      { rowCount: 1, rows: [] },                          // UPDATE row → queued
    ]);
    const sqs = fakeSqs();

    await start(pool, sqs, BATCH_ID, QUEUE_URL);

    const rowUpdate = pool.calls[3];
    expect(rowUpdate.sql).toMatch(/SET status = 'queued'/);
    expect(rowUpdate.sql).toMatch(/AND status = 'pending'/);
  });
});
