/**
 * Unit tests for lib/daily-phone-client.mjs.
 *
 * Uses a fake `fetch` implementation passed via the `fetchImpl` injection
 * point so these tests don't hit api.daily.co. The client is designed to
 * be fetch-agnostic for exactly this reason.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  searchAvailable,
  buy,
  release,
  DailyApiError,
} from '../lib/daily-phone-client.mjs';
import { AREA_CODE_TO_STATE } from '../lib/area-code-map.mjs';

/**
 * Build a fake fetch. Takes a map of URL-suffix → responder. Each responder
 * can be { status, body } or a function (url, init) => { status, body }.
 * Also records every call so tests can assert on request shapes.
 */
function makeFakeFetch(routes) {
  const calls = [];
  const fetchImpl = async (url, init = {}) => {
    calls.push({ url, init });
    const match = Object.entries(routes).find(([suffix]) => url.endsWith(suffix));
    if (!match) {
      throw new Error(`No fake route matches ${url}`);
    }
    const [, responder] = match;
    const r = typeof responder === 'function' ? responder(url, init) : responder;
    return {
      ok: r.status >= 200 && r.status < 300,
      status: r.status,
      statusText: `status ${r.status}`,
      text: async () => (typeof r.body === 'string' ? r.body : JSON.stringify(r.body ?? '')),
    };
  };
  return { fetchImpl, calls };
}

describe('searchAvailable', () => {
  const origKey = process.env.DAILY_API_KEY;
  beforeEach(() => { process.env.DAILY_API_KEY = 'test-key'; });
  afterEach(() => {
    if (origKey === undefined) delete process.env.DAILY_API_KEY;
    else process.env.DAILY_API_KEY = origKey;
  });

  it('maps area code 209 to CA, filters to area code, returns normalized list', async () => {
    const { fetchImpl, calls } = makeFakeFetch({
      '/list-available-numbers?region=CA': {
        status: 200,
        body: {
          total_count: 4,
          data: [
            { number: '+12098210844', region: 'CA' },
            { number: '+12098210846', region: 'CA' },
            { number: '+14155551234', region: 'CA' }, // Different area code — filtered out
            { number: '+16505551234', region: 'CA' }, // Different area code — filtered out
          ],
        },
      },
    });

    const result = await searchAvailable({ areaCode: '209', fetchImpl });

    expect(calls).toHaveLength(1);
    expect(calls[0].url).toContain('region=CA');
    expect(calls[0].init.headers.Authorization).toBe('Bearer test-key');

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      number: '+12098210844',
      area_code: '209',
      region: 'CA',
    });
  });

  it('accepts region directly (bypasses area-code-to-state lookup)', async () => {
    const { fetchImpl, calls } = makeFakeFetch({
      '/list-available-numbers?region=NY': {
        status: 200,
        body: { total_count: 1, data: [{ number: '+12125551234', region: 'NY' }] },
      },
    });

    const result = await searchAvailable({ region: 'NY', fetchImpl });
    expect(calls[0].url).toContain('region=NY');
    expect(result).toHaveLength(1);
  });

  it('limits results to the requested count', async () => {
    const data = Array.from({ length: 50 }, (_, i) => ({
      number: `+1209555${String(i).padStart(4, '0')}`,
      region: 'CA',
    }));
    const { fetchImpl } = makeFakeFetch({
      '/list-available-numbers?region=CA': { status: 200, body: { total_count: 50, data } },
    });

    const result = await searchAvailable({ areaCode: '209', limit: 5, fetchImpl });
    expect(result).toHaveLength(5);
  });

  it('rejects unknown area code with 400', async () => {
    const { fetchImpl } = makeFakeFetch({});
    await expect(
      searchAvailable({ areaCode: '999', fetchImpl })
    ).rejects.toMatchObject({ name: 'DailyApiError', statusCode: 400 });
  });

  it('rejects when neither areaCode nor region is provided', async () => {
    const { fetchImpl } = makeFakeFetch({});
    await expect(
      searchAvailable({ fetchImpl })
    ).rejects.toMatchObject({ name: 'DailyApiError', statusCode: 400 });
  });

  it('propagates Daily API errors with status + message', async () => {
    const { fetchImpl } = makeFakeFetch({
      '/list-available-numbers?region=CA': {
        status: 401,
        body: { error: 'unauthorized', info: 'invalid api key' },
      },
    });
    await expect(
      searchAvailable({ areaCode: '209', fetchImpl })
    ).rejects.toMatchObject({ name: 'DailyApiError', statusCode: 401 });
  });

  it('throws when DAILY_API_KEY env var is unset', async () => {
    delete process.env.DAILY_API_KEY;
    const { fetchImpl } = makeFakeFetch({});
    await expect(
      searchAvailable({ areaCode: '209', fetchImpl })
    ).rejects.toThrow(/DAILY_API_KEY/);
  });
});

describe('buy', () => {
  const origKey = process.env.DAILY_API_KEY;
  beforeEach(() => { process.env.DAILY_API_KEY = 'test-key'; });
  afterEach(() => {
    if (origKey === undefined) delete process.env.DAILY_API_KEY;
    else process.env.DAILY_API_KEY = origKey;
  });

  it('POSTs the number, returns {id, number}', async () => {
    const { fetchImpl, calls } = makeFakeFetch({
      '/buy-phone-number': {
        status: 200,
        body: { id: 'daily-uuid-abc', number: '+12098210844' },
      },
    });

    const result = await buy({ number: '+12098210844', fetchImpl });

    expect(calls[0].init.method).toBe('POST');
    expect(JSON.parse(calls[0].init.body)).toEqual({ number: '+12098210844' });
    expect(result).toEqual({ id: 'daily-uuid-abc', number: '+12098210844' });
  });

  it('rejects without a number', async () => {
    const { fetchImpl } = makeFakeFetch({});
    await expect(buy({ fetchImpl })).rejects.toMatchObject({
      name: 'DailyApiError',
      statusCode: 400,
    });
  });

  it('wraps an incomplete Daily response as 502', async () => {
    const { fetchImpl } = makeFakeFetch({
      '/buy-phone-number': { status: 200, body: { id: 'x' /* no number */ } },
    });
    await expect(
      buy({ number: '+12098210844', fetchImpl })
    ).rejects.toMatchObject({ statusCode: 502 });
  });

  it('propagates Daily 409 (number already sold) correctly', async () => {
    const { fetchImpl } = makeFakeFetch({
      '/buy-phone-number': {
        status: 409,
        body: { error: 'conflict', info: 'number already purchased' },
      },
    });
    await expect(
      buy({ number: '+12098210844', fetchImpl })
    ).rejects.toMatchObject({ statusCode: 409 });
  });
});

describe('release', () => {
  const origKey = process.env.DAILY_API_KEY;
  beforeEach(() => { process.env.DAILY_API_KEY = 'test-key'; });
  afterEach(() => {
    if (origKey === undefined) delete process.env.DAILY_API_KEY;
    else process.env.DAILY_API_KEY = origKey;
  });

  it('DELETEs with the daily_number_id and returns { released: true }', async () => {
    const { fetchImpl, calls } = makeFakeFetch({
      '/phone-number/daily-uuid-abc': {
        status: 200,
        body: { deleted: true },
      },
    });

    const result = await release({ dailyNumberId: 'daily-uuid-abc', fetchImpl });

    expect(calls[0].init.method).toBe('DELETE');
    expect(result).toEqual({ released: true, alreadyGone: false });
  });

  it('treats 404 from Daily as "already released" (idempotent)', async () => {
    const { fetchImpl } = makeFakeFetch({
      '/phone-number/unknown-id': {
        status: 404,
        body: { error: 'not-found' },
      },
    });
    const result = await release({ dailyNumberId: 'unknown-id', fetchImpl });
    expect(result).toEqual({ released: true, alreadyGone: true });
  });

  it('rejects without dailyNumberId', async () => {
    const { fetchImpl } = makeFakeFetch({});
    await expect(release({ fetchImpl })).rejects.toMatchObject({
      statusCode: 400,
    });
  });

  it('propagates non-404 Daily errors', async () => {
    const { fetchImpl } = makeFakeFetch({
      '/phone-number/daily-uuid-abc': {
        status: 500,
        body: { error: 'internal' },
      },
    });
    await expect(
      release({ dailyNumberId: 'daily-uuid-abc', fetchImpl })
    ).rejects.toMatchObject({ statusCode: 500 });
  });
});

describe('AREA_CODE_TO_STATE map', () => {
  it('maps California area codes correctly', () => {
    expect(AREA_CODE_TO_STATE['209']).toBe('CA');
    expect(AREA_CODE_TO_STATE['415']).toBe('CA');
    expect(AREA_CODE_TO_STATE['213']).toBe('CA');
  });

  it('maps New York area codes correctly', () => {
    expect(AREA_CODE_TO_STATE['212']).toBe('NY');
    expect(AREA_CODE_TO_STATE['718']).toBe('NY');
    expect(AREA_CODE_TO_STATE['917']).toBe('NY');
  });

  it('maps Texas area codes correctly', () => {
    expect(AREA_CODE_TO_STATE['512']).toBe('TX');
    expect(AREA_CODE_TO_STATE['713']).toBe('TX');
  });

  it('maps all 50 states (at least one area code each) plus DC', () => {
    const values = new Set(Object.values(AREA_CODE_TO_STATE));
    const expected = [
      'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
      'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
      'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
      'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
      'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    ];
    for (const state of expected) {
      expect(values.has(state), `missing area code mapping for ${state}`).toBe(true);
    }
  });

  it('does not include Canadian area codes (Daily only supports US)', () => {
    // Toronto, Vancouver, Montreal — all Canadian, should not be present.
    expect(AREA_CODE_TO_STATE['416']).toBeUndefined();
    expect(AREA_CODE_TO_STATE['604']).toBeUndefined();
    expect(AREA_CODE_TO_STATE['514']).toBeUndefined();
  });

  it('is frozen (no accidental mutations)', () => {
    expect(() => {
      AREA_CODE_TO_STATE['999'] = 'XX';
    }).toThrow();
  });
});

describe('DailyApiError', () => {
  it('is an Error subclass with statusCode', () => {
    const err = new DailyApiError('boom', 502);
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('DailyApiError');
    expect(err.message).toBe('boom');
    expect(err.statusCode).toBe(502);
  });
});
