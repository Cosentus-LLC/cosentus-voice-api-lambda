/**
 * Frontend-v2 alignment P1 tests.
 *
 * Covers the 12 route additions/changes from
 * docs/frontend-v2-alignment-audit.md Bucket B. Each test stubs the
 * pg.Pool and (when relevant) external services so we can run pure
 * unit tests without touching Aurora, S3, or ElevenLabs.
 *
 * The handler in index.mjs reads from a module-level `pool` constant,
 * so the standard test pattern is to import handler + the modules it
 * uses, then monkey-patch `pool.query` on the imported pg.Pool
 * instance. We rely on vitest's `vi.spyOn(pool, 'query')` for that.
 *
 * Out of scope here:
 *   - Multi-step DB transactions (covered by existing tests)
 *   - Validation paths (covered by validators.test.mjs)
 *   - Real ElevenLabs / S3 calls (we inject fake fetch / S3 client)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

import { handler } from '../index.mjs';
import { parseS3Uri, presignS3Url } from '../lib/s3-presign.mjs';
import { parseMultipart, parseSpreadsheet } from '../lib/multipart-parser.mjs';
import { elToVoiceRow, isConfigured, listVoices, getVoice } from '../lib/elevenlabs-client.mjs';

// --- pg.Pool stub ------------------------------------------------------

import pg from 'pg';

function stubPool(handlers = {}) {
  const calls = [];
  const queue = Array.isArray(handlers) ? [...handlers] : null;
  const original = pg.Pool.prototype.query;
  const stub = vi.spyOn(pg.Pool.prototype, 'query').mockImplementation(async (sql, params) => {
    calls.push({ sql, params });
    if (queue) {
      const next = queue.shift();
      if (typeof next === 'function') return next(sql, params);
      if (next) return next;
    } else if (typeof handlers === 'function') {
      return handlers(sql, params);
    }
    return { rows: [] };
  });
  return { calls, restore: () => { stub.mockRestore(); } };
}

// --- Helpers -----------------------------------------------------------

function evt(method, path, opts = {}) {
  return {
    httpMethod: method,
    path,
    queryStringParameters: opts.qs || null,
    headers: opts.headers || {},
    body: opts.body !== undefined
      ? (typeof opts.body === 'string' ? opts.body : JSON.stringify(opts.body))
      : null,
    isBase64Encoded: opts.isBase64Encoded || false,
  };
}

function parseBody(res) {
  if (!res.body) return null;
  try { return JSON.parse(res.body); } catch { return res.body; }
}

// --- B1+B2: Agent draft nested aliases ---------------------------------

describe('B1+B2: GET/PUT /api/agents/:name/draft', () => {
  let stub;
  beforeEach(() => { vi.restoreAllMocks(); });

  it('GET returns the draft row when one exists', async () => {
    const draft = { agent_id: 'chris', display_name: 'Chris', tts_voice_id: 'v1' };
    stub = stubPool(() => ({ rows: [draft] }));
    const res = await handler(evt('GET', '/api/agents/chris/draft'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res)).toEqual(draft);
  });

  it('GET returns null when no draft exists yet', async () => {
    stub = stubPool(() => ({ rows: [] }));
    const res = await handler(evt('GET', '/api/agents/chris/draft'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res)).toBeNull();
  });

  it('PUT updates existing draft', async () => {
    let updateCalled = false;
    stub = stubPool((sql) => {
      if (sql.startsWith('SELECT')) return { rows: [{ agent_id: 'chris', display_name: 'old' }] };
      if (sql.startsWith('UPDATE')) {
        updateCalled = true;
        return { rows: [{ agent_id: 'chris', display_name: 'new' }] };
      }
      return { rows: [] };
    });
    // Use display_name — a benign field that doesn't trigger
    // tts_provider/tts_model cross-field validation.
    const res = await handler(evt('PUT', '/api/agents/chris/draft', {
      body: { display_name: 'new' },
    }));
    expect(res.statusCode).toBe(200);
    expect(updateCalled).toBe(true);
    expect(parseBody(res).display_name).toBe('new');
  });

  it('PUT inserts a fresh draft when none exists (first-patch flow)', async () => {
    let insertCalled = false;
    stub = stubPool((sql) => {
      if (sql.startsWith('SELECT * FROM voice_agent_drafts WHERE agent_id')) return { rows: [] };
      if (sql.startsWith('UPDATE')) return { rows: [] };
      if (sql.startsWith('INSERT')) { insertCalled = true; return { rows: [] }; }
      if (sql.startsWith('SELECT * FROM voice_agent_drafts')) {
        return { rows: [{ agent_id: 'chris', display_name: 'new' }] };
      }
      return { rows: [] };
    });
    const res = await handler(evt('PUT', '/api/agents/chris/draft', {
      body: { display_name: 'new' },
    }));
    expect(res.statusCode).toBe(200);
    expect(insertCalled).toBe(true);
  });

  it('DELETE removes the draft (companion of GET/PUT alias)', async () => {
    stub = stubPool(() => ({ rows: [] }));
    const res = await handler(evt('DELETE', '/api/agents/chris/draft'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res)).toEqual({ deleted: true });
  });
});

// --- B3+B4: Agent versions nested aliases ------------------------------

describe('B3+B4: GET/POST /api/agents/:name/versions', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('GET wraps the array in {versions: [...]}', async () => {
    stubPool(() => ({ rows: [{ id: 'v1', version_number: 2 }, { id: 'v2', version_number: 1 }] }));
    const res = await handler(evt('GET', '/api/agents/chris/versions'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res)).toEqual({
      versions: [
        { id: 'v1', version_number: 2 },
        { id: 'v2', version_number: 1 },
      ],
    });
  });

  it('POST inserts a new version row + returns id', async () => {
    let insertCalled = false;
    stubPool((sql) => {
      if (sql.startsWith('INSERT')) { insertCalled = true; return { rows: [] }; }
      return { rows: [] };
    });
    const res = await handler(evt('POST', '/api/agents/chris/versions', {
      body: {
        version_number: 3,
        version_name: 'add tools',
        description: 'wired press_digit',
        config_snapshot: { llm_model: 'x' },
        phone_assignments: [],
      },
    }));
    expect(res.statusCode).toBe(201);
    expect(insertCalled).toBe(true);
    const body = parseBody(res);
    expect(body.id).toBeDefined();
    expect(body.agent_id).toBe('chris');
    expect(body.version_number).toBe(3);
  });

  it('POST uses URL :name as agent_id when body.agent_id is absent', async () => {
    let insertedAgentId = null;
    stubPool((sql, params) => {
      if (sql.startsWith('INSERT')) {
        insertedAgentId = params[1];
        return { rows: [] };
      }
      return { rows: [] };
    });
    await handler(evt('POST', '/api/agents/chris/versions', {
      body: { version_number: 4 },
    }));
    expect(insertedAgentId).toBe('chris');
  });
});

// --- B5: DELETE /api/calls/:id -----------------------------------------

describe('B5: DELETE /api/calls/:id', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('soft-deletes (sets hidden=true) and returns {deleted: true}', async () => {
    let updateSql = null;
    stubPool((sql) => {
      if (sql.startsWith('UPDATE voice_calls')) {
        updateSql = sql;
        return { rows: [{ id: 'call-1' }] };
      }
      return { rows: [] };
    });
    const res = await handler(evt('DELETE', '/api/calls/call-1'));
    expect(res.statusCode).toBe(200);
    const body = parseBody(res);
    expect(body.deleted).toBe(true);
    expect(body.status).toBe('hidden');
    expect(updateSql).toContain('hidden = true');
  });

  it('returns 404 when no call matches the id', async () => {
    stubPool(() => ({ rows: [] }));
    const res = await handler(evt('DELETE', '/api/calls/missing'));
    expect(res.statusCode).toBe(404);
  });

  it('legacy PUT :id/hide still works (no regression)', async () => {
    stubPool(() => ({ rows: [{ id: 'call-2' }] }));
    const res = await handler(evt('PUT', '/api/calls/call-2/hide'));
    expect(res.statusCode).toBe(200);
    const body = parseBody(res);
    expect(body.status).toBe('hidden');
  });
});

// --- B6: /batches/:id/results 302 + /download-url presign --------------

describe('B6: GET /api/batches/:id/results + /download-url presigning', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('/results 302s to the presigned URL when output exists', async () => {
    stubPool(() => ({ rows: [{ output_file_path: 'https://example.com/already-presigned.xlsx', status: 'completed' }] }));
    const res = await handler(evt('GET', '/api/batches/abc/results'));
    expect(res.statusCode).toBe(302);
    expect(res.headers.Location).toBe('https://example.com/already-presigned.xlsx');
  });

  it('/results returns 404 when output_file_path is null', async () => {
    stubPool(() => ({ rows: [{ output_file_path: null, status: 'running' }] }));
    const res = await handler(evt('GET', '/api/batches/abc/results'));
    expect(res.statusCode).toBe(404);
    expect(parseBody(res).status).toBe('running');
  });

  it('/results returns 404 when batch is missing', async () => {
    stubPool(() => ({ rows: [] }));
    const res = await handler(evt('GET', '/api/batches/missing/results'));
    expect(res.statusCode).toBe(404);
  });

  it('/download-url returns {url: presigned} for an https-stored path (passthrough)', async () => {
    stubPool(() => ({ rows: [{ output_file_path: 'https://example.com/file.xlsx' }] }));
    const res = await handler(evt('GET', '/api/batches/abc/download-url'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).url).toBe('https://example.com/file.xlsx');
  });

  it('/download-url returns {url: null} when no output yet', async () => {
    stubPool(() => ({ rows: [{ output_file_path: null }] }));
    const res = await handler(evt('GET', '/api/batches/abc/download-url'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).url).toBeNull();
  });
});

// --- B7: Multipart upload ---------------------------------------------

describe('B7: POST /api/batches/upload (multipart)', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('still accepts the legacy JSON {rows: [...]} shape', async () => {
    let insertedRows = null;
    stubPool((sql, params) => {
      if (sql.startsWith('INSERT')) {
        insertedRows = JSON.parse(params[5]);
        return { rows: [] };
      }
      return { rows: [] };
    });
    const res = await handler(evt('POST', '/api/batches/upload', {
      headers: { 'content-type': 'application/json' },
      body: {
        agent_name: 'chris',
        from_number: '+12091234567',
        rows: [
          { phone: '+15551234567', name: 'Alice' },
          { phone: '+15559876543', name: 'Bob' },
        ],
      },
    }));
    expect(res.statusCode).toBe(201);
    const body = parseBody(res);
    expect(body.summary.total).toBe(2);
    expect(body.summary.valid).toBe(2);
    expect(insertedRows).toBeTruthy();
    expect(insertedRows.length).toBe(2);
  });

  it('returns 400 when multipart parse fails', async () => {
    const res = await handler(evt('POST', '/api/batches/upload', {
      headers: { 'content-type': 'multipart/form-data; boundary=--not-real' },
      body: 'garbage',
    }));
    // Either 400 (parse) or 400 (missing file) — both acceptable
    expect(res.statusCode).toBe(400);
  });

  it('returns 400 when agent_name is missing', async () => {
    const res = await handler(evt('POST', '/api/batches/upload', {
      headers: { 'content-type': 'application/json' },
      body: { rows: [] },
    }));
    expect(res.statusCode).toBe(400);
    expect(parseBody(res).detail).toMatch(/agent_name/);
  });

  it('parses multipart body end-to-end', async () => {
    // Build a real multipart body. busboy will parse it.
    const boundary = '----test-boundary-xyz';
    const csvContent = 'phone,name\n+15551234567,Alice\n+15559876543,Bob\n';
    const body = [
      `--${boundary}`,
      'Content-Disposition: form-data; name="agent_name"',
      '',
      'chris',
      `--${boundary}`,
      'Content-Disposition: form-data; name="from_number"',
      '',
      '+12091234567',
      `--${boundary}`,
      'Content-Disposition: form-data; name="file"; filename="list.csv"',
      'Content-Type: text/csv',
      '',
      csvContent,
      `--${boundary}--`,
      '',
    ].join('\r\n');

    stubPool(() => ({ rows: [] }));
    const res = await handler(evt('POST', '/api/batches/upload', {
      headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
      body,
    }));
    expect(res.statusCode).toBe(201);
    const parsed = parseBody(res);
    expect(parsed.summary.total).toBe(2);
    expect(parsed.summary.valid).toBe(2);
    expect(parsed.rows[0].phone_e164).toBe('+15551234567');
    expect(parsed.rows[1].phone_e164).toBe('+15559876543');
    expect(parsed.columns).toEqual(['phone', 'name']);
  });

  it('marks bad phone rows as invalid / fixable', async () => {
    stubPool(() => ({ rows: [] }));
    const res = await handler(evt('POST', '/api/batches/upload', {
      headers: { 'content-type': 'application/json' },
      body: {
        agent_name: 'chris',
        rows: [
          { phone: '+15551234567', name: 'Alice' },  // valid
          { phone: '5551234567', name: 'Bob' },       // fixable (missing +)
          { phone: '', name: 'Carol' },               // invalid (no phone)
        ],
      },
    }));
    expect(res.statusCode).toBe(201);
    const body = parseBody(res);
    expect(body.summary.valid).toBe(1);
    expect(body.summary.fixable).toBe(1);
    expect(body.summary.invalid).toBe(1);
  });
});

// --- B9: agent_display_name filter alias -------------------------------

describe('B9: GET /api/calls?agent_display_name=...', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('filters by agent_display_name when query param is present', async () => {
    let filterParams = null;
    stubPool((sql, params) => {
      if (sql.startsWith('SELECT COUNT(*)')) {
        filterParams = params;
        return { rows: [{ count: '0' }] };
      }
      return { rows: [] };
    });
    await handler(evt('GET', '/api/calls', {
      qs: { agent_display_name: 'Chris — claim status' },
    }));
    expect(filterParams).toContain('Chris — claim status');
  });

  it('agent_name and agent_display_name can be combined', async () => {
    let filterParams = null;
    stubPool((sql, params) => {
      if (sql.startsWith('SELECT COUNT(*)')) {
        filterParams = params;
        return { rows: [{ count: '0' }] };
      }
      return { rows: [] };
    });
    await handler(evt('GET', '/api/calls', {
      qs: { agent_name: 'chris-claim-status', agent_display_name: 'Chris — claim status' },
    }));
    expect(filterParams).toContain('chris-claim-status');
    expect(filterParams).toContain('Chris — claim status');
  });
});

// --- B10: recording-url presigning -------------------------------------

describe('B10: GET /api/calls/:id/recording-url', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('returns {url: null} when call has no recording', async () => {
    stubPool(() => ({ rows: [{ recording_path: null }] }));
    const res = await handler(evt('GET', '/api/calls/call-1/recording-url'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).url).toBeNull();
  });

  it('passes through already-https URLs', async () => {
    stubPool(() => ({ rows: [{ recording_path: 'https://cdn.example.com/recording.wav' }] }));
    const res = await handler(evt('GET', '/api/calls/call-1/recording-url'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).url).toBe('https://cdn.example.com/recording.wav');
  });
});

// --- presign helper unit tests -----------------------------------------

describe('presignS3Url helper', () => {
  it('parseS3Uri splits s3://bucket/key correctly', () => {
    expect(parseS3Uri('s3://my-bucket/path/file.wav')).toEqual({
      bucket: 'my-bucket',
      key: 'path/file.wav',
    });
  });

  it('parseS3Uri returns null for non-s3 URIs', () => {
    expect(parseS3Uri('https://example.com/file.wav')).toBeNull();
    expect(parseS3Uri('')).toBeNull();
    expect(parseS3Uri(null)).toBeNull();
  });

  it('presignS3Url passes through https URLs', async () => {
    const url = await presignS3Url('https://example.com/file.wav');
    expect(url).toBe('https://example.com/file.wav');
  });

  it('presignS3Url returns null for empty input', async () => {
    expect(await presignS3Url(null)).toBeNull();
    expect(await presignS3Url('')).toBeNull();
    expect(await presignS3Url(undefined)).toBeNull();
  });
});

// --- multipart parser unit tests ---------------------------------------

describe('parseMultipart helper', () => {
  it('rejects when Content-Type is not multipart', async () => {
    await expect(parseMultipart({
      headers: { 'content-type': 'application/json' },
      body: '{}',
    })).rejects.toThrow(/multipart/);
  });

  it('returns empty fields/files when body is empty', async () => {
    const r = await parseMultipart({
      headers: { 'content-type': 'multipart/form-data; boundary=x' },
      body: '',
    });
    expect(r).toEqual({ fields: {}, files: [] });
  });

  it('parses fields + a single file', async () => {
    const boundary = '----b';
    const body = [
      `--${boundary}`,
      'Content-Disposition: form-data; name="agent_name"',
      '',
      'chris',
      `--${boundary}`,
      'Content-Disposition: form-data; name="file"; filename="test.csv"',
      'Content-Type: text/csv',
      '',
      'phone,name\n+15551234567,Alice\n',
      `--${boundary}--`,
      '',
    ].join('\r\n');
    const r = await parseMultipart({
      headers: { 'content-type': `multipart/form-data; boundary=${boundary}` },
      body,
    });
    expect(r.fields.agent_name).toBe('chris');
    expect(r.files.length).toBe(1);
    expect(r.files[0].filename).toBe('test.csv');
    expect(r.files[0].buffer.toString('utf8')).toContain('Alice');
  });
});

describe('parseSpreadsheet helper (CSV path)', () => {
  it('parses a simple CSV buffer into columns + rows', () => {
    const csv = 'phone,name\n+15551234567,Alice\n+15559876543,Bob\n';
    const { columns, rows } = parseSpreadsheet({
      filename: 'list.csv',
      mimeType: 'text/csv',
      buffer: Buffer.from(csv, 'utf8'),
    });
    expect(columns).toEqual(['phone', 'name']);
    expect(rows).toEqual([
      { phone: '+15551234567', name: 'Alice' },
      { phone: '+15559876543', name: 'Bob' },
    ]);
  });

  it('handles quoted fields with embedded commas', () => {
    const csv = 'phone,note\n+15551234567,"Alice, primary"\n';
    const { rows } = parseSpreadsheet({
      filename: 'x.csv',
      buffer: Buffer.from(csv, 'utf8'),
    });
    expect(rows[0].note).toBe('Alice, primary');
  });

  it('strips a UTF-8 BOM from the first column header', () => {
    const csv = '\uFEFFphone,name\n+15551234567,Alice\n';
    const { columns } = parseSpreadsheet({
      filename: 'x.csv',
      buffer: Buffer.from(csv, 'utf8'),
    });
    expect(columns).toEqual(['phone', 'name']);
  });
});

// --- B11: ElevenLabs sync + refresh ------------------------------------

describe('B11: ElevenLabs sync + refresh', () => {
  const oldKey = process.env.ELEVENLABS_API_KEY;
  beforeEach(() => { vi.restoreAllMocks(); });

  it('isConfigured() reflects ELEVENLABS_API_KEY env var', () => {
    delete process.env.ELEVENLABS_API_KEY;
    expect(isConfigured()).toBe(false);
    process.env.ELEVENLABS_API_KEY = 'test-key';
    expect(isConfigured()).toBe(true);
    if (oldKey !== undefined) process.env.ELEVENLABS_API_KEY = oldKey;
    else delete process.env.ELEVENLABS_API_KEY;
  });

  it('listVoices returns unconfigured when key is missing', async () => {
    delete process.env.ELEVENLABS_API_KEY;
    const r = await listVoices();
    expect(r.ok).toBe(false);
    expect(r.voices).toEqual([]);
    expect(r.error).toMatch(/ELEVENLABS_API_KEY/);
    if (oldKey !== undefined) process.env.ELEVENLABS_API_KEY = oldKey;
  });

  it('listVoices uses injected fetch when key is set', async () => {
    process.env.ELEVENLABS_API_KEY = 'test-key';
    const fakeFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ voices: [{ voice_id: 'v1', name: 'Alice' }] }),
    });
    const r = await listVoices({ fetchImpl: fakeFetch });
    expect(r.ok).toBe(true);
    expect(r.voices.length).toBe(1);
    expect(r.voices[0].voice_id).toBe('v1');
    expect(fakeFetch).toHaveBeenCalledOnce();
    const [url, init] = fakeFetch.mock.calls[0];
    expect(url).toMatch(/api\.elevenlabs\.io\/v1\/voices$/);
    expect(init.headers['xi-api-key']).toBe('test-key');
    if (oldKey !== undefined) process.env.ELEVENLABS_API_KEY = oldKey;
    else delete process.env.ELEVENLABS_API_KEY;
  });

  it('getVoice maps a 404 to ok:false with status', async () => {
    process.env.ELEVENLABS_API_KEY = 'test-key';
    const fakeFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
      json: async () => ({ detail: { message: 'not found' } }),
    });
    const r = await getVoice('v-missing', { fetchImpl: fakeFetch });
    expect(r.ok).toBe(false);
    expect(r.status).toBe(404);
    if (oldKey !== undefined) process.env.ELEVENLABS_API_KEY = oldKey;
    else delete process.env.ELEVENLABS_API_KEY;
  });

  it('elToVoiceRow flattens labels into top-level fields', () => {
    const row = elToVoiceRow({
      voice_id: 'v1',
      name: 'Alice',
      description: 'A voice',
      preview_url: 'https://x',
      labels: { gender: 'female', accent: 'american', age: 'young' },
      category: 'premade',
    });
    expect(row).toMatchObject({
      voice_id: 'v1',
      name: 'Alice',
      gender: 'female',
      accent: 'american',
      age: 'young',
      category: 'premade',
    });
  });

  it('POST /api/voices/sync returns unconfigured stub when key is missing', async () => {
    delete process.env.ELEVENLABS_API_KEY;
    stubPool(() => ({ rows: [] }));
    const res = await handler(evt('POST', '/api/voices/sync'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).count).toBe(0);
    expect(parseBody(res).message).toMatch(/not yet configured/);
  });

  it('POST /api/voices/:id/refresh falls back to cached row when key missing', async () => {
    delete process.env.ELEVENLABS_API_KEY;
    stubPool(() => ({ rows: [{ voice_id: 'v1', name: 'Alice' }] }));
    const res = await handler(evt('POST', '/api/voices/v1/refresh'));
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).voice_id).toBe('v1');
  });
});

// --- Dev API Gateway route prefix stripping ---------------------------

describe('parseRoute: /dev/voice prefix stripping', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('strips /dev/voice/ prefix the same way /prod/voice/ is stripped', async () => {
    stubPool(() => ({ rows: [] }));
    const res = await handler({
      httpMethod: 'GET',
      path: '/dev/voice/api/health',
      // Newer HTTP API v2 events use rawPath; older REST API events use path.
      // The dev-route case uses HTTP API v2, so include both for parity.
      rawPath: '/dev/voice/api/health',
    });
    expect(res.statusCode).toBe(200);
    const body = parseBody(res);
    expect(body.status).toBe('healthy');
  });

  it('strips /dev/voice on a nested CRUD path (e.g. agent draft)', async () => {
    let queriedSql = null;
    stubPool((sql) => {
      queriedSql = sql;
      return { rows: [{ agent_id: 'chris', display_name: 'Test' }] };
    });
    const res = await handler({
      httpMethod: 'GET',
      rawPath: '/dev/voice/api/agents/chris/draft',
    });
    expect(res.statusCode).toBe(200);
    expect(queriedSql).toContain('voice_agent_drafts');
  });

  it('legacy /prod/voice route still works (no regression)', async () => {
    stubPool(() => ({ rows: [] }));
    const res = await handler({
      httpMethod: 'GET',
      rawPath: '/prod/voice/api/health',
    });
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).status).toBe('healthy');
  });

  it('legacy /voice route still works (no regression)', async () => {
    stubPool(() => ({ rows: [] }));
    const res = await handler({
      httpMethod: 'GET',
      rawPath: '/voice/api/health',
    });
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).status).toBe('healthy');
  });

  it('un-prefixed /api/ paths still work (existing behavior)', async () => {
    stubPool(() => ({ rows: [] }));
    const res = await handler({
      httpMethod: 'GET',
      rawPath: '/api/health',
    });
    expect(res.statusCode).toBe(200);
    expect(parseBody(res).status).toBe('healthy');
  });
});

// --- B8: /api/calls/agents response shape -----------------------------

describe('B8: GET /api/calls/agents (verify shape)', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('returns {agents: [{agent_name, display_name}]}', async () => {
    stubPool(() => ({
      rows: [
        { agent_name: 'chris-claim-status', agent_display_name: 'Chris — claim status' },
        { agent_name: 'sarah-denial', agent_display_name: null }, // null display falls back to name
      ],
    }));
    const res = await handler(evt('GET', '/api/calls/agents'));
    expect(res.statusCode).toBe(200);
    const body = parseBody(res);
    expect(body.agents.length).toBe(2);
    expect(body.agents[0]).toEqual({
      agent_name: 'chris-claim-status',
      display_name: 'Chris — claim status',
    });
    expect(body.agents[1]).toEqual({
      agent_name: 'sarah-denial',
      display_name: 'sarah-denial',
    });
  });
});
