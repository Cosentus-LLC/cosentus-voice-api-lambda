/**
 * Daily.co phone-number API client.
 *
 * Exposes three functions consumed by the Lambda's /api/phone-numbers
 * routes. All network calls go through fetch (Node 22 built-in) so no
 * additional npm dependency is needed.
 *
 * Daily's REST API for phone numbers:
 *
 *   GET    /v1/list-available-numbers?region=<STATE>
 *             Returns { total_count, data: [{ number, region }, ...] }.
 *             The `region` param is a 2-letter US state code ("CA", "NY", ...).
 *             There is NO area-code filter on Daily's side; we apply that
 *             server-side in searchAvailable() below.
 *
 *   POST   /v1/buy-phone-number   body: { number: "+1XXXYYYZZZZ" }
 *             Returns { id, number }. The `id` is the daily_number_id we
 *             persist to Aurora so we can release later.
 *
 *   DELETE /v1/phone-number/<daily_number_id>
 *             Releases the number. Returns 200 with { deleted: true } on
 *             success; 404 if the id is unknown (we treat as "already
 *             released" and proceed with our DB delete).
 *
 * Reference:
 *   https://docs.daily.co/reference/rest-api/phone-numbers
 *   https://docs.daily.co/reference/rest-api/phone-numbers/buy-phone-number
 *   https://docs.daily.co/reference/rest-api/phone-numbers/release-phone-number
 */

import { AREA_CODE_TO_STATE } from './area-code-map.mjs';

const DAILY_API_BASE = 'https://api.daily.co/v1';

class DailyApiError extends Error {
  constructor(message, statusCode) {
    super(message);
    this.name = 'DailyApiError';
    this.statusCode = statusCode;
  }
}

/**
 * Fetch a Daily API key at call time. Sourced from the Lambda env var
 * `DAILY_API_KEY`. Throws if unset — there is no meaningful default.
 *
 * Split out as its own function (rather than read once at module load)
 * so tests can swap in a test key via setDailyApiKey() or by setting
 * process.env.DAILY_API_KEY before importing the module.
 */
function getDailyApiKey() {
  const key = process.env.DAILY_API_KEY;
  if (!key) {
    throw new DailyApiError(
      'DAILY_API_KEY env var not set — cannot call Daily phone number API.',
      500,
    );
  }
  return key;
}

async function dailyFetch(path, { method = 'GET', body, fetchImpl } = {}) {
  const f = fetchImpl || fetch;
  const url = `${DAILY_API_BASE}${path}`;
  const resp = await f(url, {
    method,
    headers: {
      'Authorization': `Bearer ${getDailyApiKey()}`,
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await resp.text();
  let parsed = null;
  try { parsed = text ? JSON.parse(text) : null; } catch { /* not JSON — keep text */ }

  if (!resp.ok) {
    const message =
      (parsed && (parsed.info || parsed.error)) ||
      `Daily API ${method} ${path} failed: ${resp.status} ${resp.statusText}`;
    throw new DailyApiError(message, resp.status);
  }
  return parsed;
}

/**
 * Search Daily's inventory for available numbers.
 *
 * Daily's API only filters by state `region`, not by area code. We map
 * the requested area code to its US state, query Daily for that state,
 * then filter the returned list to numbers whose area code matches.
 *
 * Callers can also skip area-code filtering by passing { region: "CA" }
 * directly — useful if we add a region dropdown to the UI later.
 *
 * Returns a normalized list:
 *   [{ number: "+12098210844", area_code: "209", region: "CA" }, ...]
 *
 * Throws DailyApiError for unknown area codes or Daily API failures.
 */
export async function searchAvailable({ areaCode, region, limit = 30, fetchImpl } = {}) {
  let targetRegion = region;
  if (!targetRegion) {
    if (!areaCode) {
      throw new DailyApiError(
        'searchAvailable requires either areaCode or region',
        400,
      );
    }
    targetRegion = AREA_CODE_TO_STATE[String(areaCode)];
    if (!targetRegion) {
      throw new DailyApiError(
        `Unknown US area code: ${areaCode}. If this is valid, add it to area-code-map.mjs.`,
        400,
      );
    }
  }

  const resp = await dailyFetch(
    `/list-available-numbers?region=${encodeURIComponent(targetRegion)}`,
    { fetchImpl },
  );

  const all = Array.isArray(resp?.data) ? resp.data : [];
  const filtered = areaCode
    ? all.filter((n) => {
        // Strip leading +1 country code, check the next 3 digits.
        const digits = String(n.number || '').replace(/\D/g, '');
        const areaPart = digits.length === 11 ? digits.slice(1, 4) : digits.slice(0, 3);
        return areaPart === String(areaCode);
      })
    : all;

  return filtered.slice(0, limit).map((n) => ({
    number: n.number,
    area_code: String(n.number || '').replace(/\D/g, '').slice(-10, -7),
    region: n.region,
  }));
}

/**
 * Buy a phone number from Daily.
 *
 * Returns { id, number } where `id` is the daily_number_id we store in
 * Aurora. Both fields are required so we can release the number later.
 */
export async function buy({ number, fetchImpl } = {}) {
  if (!number) {
    throw new DailyApiError('buy requires a number', 400);
  }
  const resp = await dailyFetch('/buy-phone-number', {
    method: 'POST',
    body: { number },
    fetchImpl,
  });
  if (!resp?.id || !resp?.number) {
    throw new DailyApiError(
      `Daily buy-phone-number returned incomplete response: ${JSON.stringify(resp)}`,
      502,
    );
  }
  return { id: resp.id, number: resp.number };
}

/**
 * Release a previously-purchased Daily phone number.
 *
 * Idempotent — if Daily returns 404 we treat the release as already done
 * (the ID wasn't known to Daily, so there's nothing to release on their
 * side) and return { released: true, alreadyGone: true }.
 */
export async function release({ dailyNumberId, fetchImpl } = {}) {
  if (!dailyNumberId) {
    throw new DailyApiError('release requires dailyNumberId', 400);
  }
  try {
    await dailyFetch(
      `/phone-number/${encodeURIComponent(dailyNumberId)}`,
      { method: 'DELETE', fetchImpl },
    );
    return { released: true, alreadyGone: false };
  } catch (err) {
    if (err instanceof DailyApiError && err.statusCode === 404) {
      return { released: true, alreadyGone: true };
    }
    throw err;
  }
}

// Exported for tests to check instanceof.
export { DailyApiError };
