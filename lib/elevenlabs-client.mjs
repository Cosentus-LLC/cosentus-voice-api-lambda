/**
 * Minimal ElevenLabs API client.
 *
 * Used by:
 *   - POST /api/voices/sync     (B11 — pull full voice library)
 *   - POST /api/voices/:id/refresh (B11 — refresh one voice's metadata)
 *
 * Uses native fetch (available in Node 22.x Lambda runtime).
 *
 * Auth: `XI_API_KEY` header set to process.env.ELEVENLABS_API_KEY.
 * When the env var is absent, every exported function returns an
 * "unconfigured" result instead of throwing — the route handler
 * surfaces this to the client and degrades to the existing
 * stub-style response so the frontend's Sync / Refresh buttons
 * don't error.
 *
 * Shape contract:
 *   isConfigured() → boolean
 *
 *   listVoices(opts) → Promise<{ ok: boolean, voices: Voice[], error?: string }>
 *     Calls GET /v1/voices on api.elevenlabs.io.
 *
 *   getVoice(voiceId, opts) → Promise<{ ok: boolean, voice?: Voice, status?: number, error?: string }>
 *     Calls GET /v1/voices/:voice_id.
 *
 * Voice shape (passed straight through from ElevenLabs JSON):
 *   {
 *     voice_id, name, category, description, preview_url,
 *     labels: { gender?, accent?, age?, descriptive?, use_case? },
 *     ...rest
 *   }
 *
 * opts.fetchImpl is a dependency-injection seam for tests; defaults
 * to globalThis.fetch.
 */

const API_BASE = 'https://api.elevenlabs.io';

export function isConfigured() {
  return Boolean(process.env.ELEVENLABS_API_KEY);
}

function defaultFetch() {
  // Bind to globalThis so vitest's spy-on-fetch pattern works.
  return globalThis.fetch.bind(globalThis);
}

async function callElevenLabs(path, opts = {}) {
  const apiKey = process.env.ELEVENLABS_API_KEY;
  if (!apiKey) {
    return { ok: false, status: 0, body: null, error: 'unconfigured' };
  }
  const fetchImpl = opts.fetchImpl || defaultFetch();
  const url = `${API_BASE}${path}`;
  let res;
  try {
    res = await fetchImpl(url, {
      method: 'GET',
      headers: { 'xi-api-key': apiKey, 'Accept': 'application/json' },
    });
  } catch (e) {
    return { ok: false, status: 0, body: null, error: `network: ${e.message}` };
  }
  let body = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      body,
      error: body?.detail?.message || body?.message || `HTTP ${res.status}`,
    };
  }
  return { ok: true, status: res.status, body, error: null };
}

export async function listVoices(opts = {}) {
  if (!isConfigured()) {
    return { ok: false, voices: [], error: 'ELEVENLABS_API_KEY not set' };
  }
  const r = await callElevenLabs('/v1/voices', opts);
  if (!r.ok) {
    return { ok: false, voices: [], error: r.error };
  }
  const voices = Array.isArray(r.body?.voices) ? r.body.voices : [];
  return { ok: true, voices, error: null };
}

export async function getVoice(voiceId, opts = {}) {
  if (!isConfigured()) {
    return { ok: false, voice: null, error: 'ELEVENLABS_API_KEY not set' };
  }
  if (!voiceId || typeof voiceId !== 'string') {
    return { ok: false, voice: null, error: 'voice_id is required' };
  }
  const r = await callElevenLabs(`/v1/voices/${encodeURIComponent(voiceId)}`, opts);
  if (!r.ok) {
    return {
      ok: false,
      voice: null,
      status: r.status,
      error: r.error,
    };
  }
  return { ok: true, voice: r.body, error: null };
}

/**
 * Reduce an ElevenLabs voice payload to the column set the
 * voice_voices upsert expects. Centralized here so /sync and
 * /:id/refresh produce identical rows.
 */
export function elToVoiceRow(elVoice) {
  if (!elVoice || typeof elVoice !== 'object') return null;
  const labels = elVoice.labels || {};
  return {
    voice_id: elVoice.voice_id,
    name: elVoice.name || elVoice.voice_id,
    description: elVoice.description || null,
    preview_url: elVoice.preview_url || null,
    gender: labels.gender || null,
    accent: labels.accent || null,
    age: labels.age || null,
    category: elVoice.category || null,
    labels: labels,
  };
}
