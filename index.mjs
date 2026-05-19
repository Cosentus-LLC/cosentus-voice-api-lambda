/**
 * MedCloud Voice API — Standalone Lambda
 * 
 * Completely independent from MedCloud RCM Lambda.
 * Own tables (voice_* prefix), own routes, own deployment.
 *
 * ── Routes ──
 *   GET/POST     /api/agents              — List/create agents
 *   GET/PUT/DEL  /api/agents/:name        — Agent CRUD
 *   POST         /api/agents/:name/clone  — Clone agent
 *   GET/PUT      /api/agents/:name/prompt — System prompt management
 *   GET          /api/agent-schema        — Agent config schema (models, voices, etc)
 *   GET/POST/PUT /api/agent-drafts/:id    — Draft management (replaces Supabase)
 *   GET/PUT      /api/agents/:name/draft  — Nested alias of agent-drafts (frontend uses this path)
 *   GET/POST     /api/agent-versions/:id  — Version history (replaces Supabase)
 *   GET/POST     /api/agents/:name/versions — Nested alias of agent-versions (frontend uses this path)
 *   GET          /api/calls               — Call history (paginated, filterable)
 *   GET          /api/calls/:id           — Call detail
 *   DELETE       /api/calls/:id           — Soft-delete (alias of PUT :id/hide)
 *   GET          /api/calls/:id/recording-url — Signed recording URL
 *   GET          /api/calls/agents        — Distinct agent names from calls
 *   GET          /api/batches             — List batches
 *   GET          /api/batches/:id         — Batch detail + calls
 *   POST         /api/batches/upload      — Upload CSV or Excel (multipart/form-data)
 *   PUT          /api/batches/:id/rows    — Update mapped rows
 *   POST         /api/batches/:id/start   — Start batch
 *   POST         /api/batches/:id/pause   — Pause batch
 *   POST         /api/batches/:id/resume  — Resume batch
 *   POST         /api/batches/:id/cancel  — Cancel batch
 *   GET          /api/batches/:id/status  — Batch progress
 *   GET          /api/batches/:id/download-url — Results download (returns { url } with presigned URL)
 *   GET          /api/batches/:id/results — Results download (302 redirect to presigned URL)
 *   DELETE       /api/batches/:id/draft   — Delete draft batch
 *   GET          /api/phone-numbers       — List phone numbers
 *   POST         /api/phone-numbers       — Add phone number
 *   PUT          /api/phone-numbers/:id   — Update assignment
 *   POST         /api/phone-numbers/sync-twilio — Sync from Twilio
 *   GET          /api/phone-numbers/search — Search available numbers
 *   POST         /api/phone-numbers/purchase — Buy number
 *   POST         /api/phone-numbers/:id/release — Release number
 *   GET          /api/voices              — List voices
 *   POST         /api/voices/sync         — Sync from ElevenLabs
 *   POST         /api/voices/lookup       — Lookup voice by ID
 *   POST         /api/voices/add          — Add voice to library
 *   POST         /api/voices/:id/refresh  — Refresh voice metadata
 *   DELETE       /api/voices/:id          — Remove voice
 *   GET          /api/voices/:id/agents   — Agents using this voice
 *   GET          /api/health              — Health check
 *
 * Deploy: zip this + node_modules (pg) → Lambda medcloud-voice-api
 * Requires: Aurora PostgreSQL (same cluster as MedCloud, isolated tables)
 */

import pg from 'pg';
import crypto from 'crypto';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import * as dailyPhoneClient from './lib/daily-phone-client.mjs';
import { presignS3Url } from './lib/s3-presign.mjs';
import { parseMultipart, parseSpreadsheet } from './lib/multipart-parser.mjs';
import * as elevenlabs from './lib/elevenlabs-client.mjs';
const { Pool } = pg;

// ─── SQS (call request queue) ───────────────────────────────────────────────
const sqsClient = new SQSClient({ region: 'us-east-1' });
const CALL_QUEUE_URL = process.env.CALL_QUEUE_URL
  || 'https://sqs.us-east-1.amazonaws.com/825269749545/medcloud-voice-call-requests';

// ─── Connection (same Aurora cluster, voice_* tables only) ─────────────────
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'medcloud',
  user: process.env.DB_USER || 'medcloud',
  password: process.env.DB_PASS,
  port: 5432, max: 5,
  ssl: process.env.DB_HOST ? { rejectUnauthorized: process.env.DB_SSL_STRICT !== 'false' } : false,
  idleTimeoutMillis: 60000,
});

const uuid = () => crypto.randomUUID();
const respond = (code, body) => ({ statusCode: code, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Methods': '*' }, body: JSON.stringify(body) });

// 302 redirect response. Used by GET /api/batches/:id/results to send
// the browser straight to the presigned S3 URL instead of returning
// a JSON wrapper (so a plain `<a href="/api/batches/:id/results"
// download>` works).
const redirect = (location) => ({
  statusCode: 302,
  headers: {
    Location: location,
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': '*',
    'Access-Control-Allow-Methods': '*',
  },
  body: '',
});

// ─── Cold Start: Create voice_* tables ─────────────────────────────────────
let tablesReady = false;
async function ensureTables() {
  if (tablesReady) return;
  const statements = [
    // Voice Agents
    `CREATE TABLE IF NOT EXISTS voice_agents (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(100) UNIQUE NOT NULL,
      display_name VARCHAR(200) NOT NULL DEFAULT '',
      description TEXT DEFAULT '',
      llm_provider VARCHAR(50) DEFAULT 'anthropic',
      llm_model VARCHAR(100) DEFAULT 'us.anthropic.claude-sonnet-4-6',
      temperature NUMERIC(3,2) DEFAULT 0.7,
      max_tokens INTEGER DEFAULT 1024,
      enable_prompt_caching BOOLEAN DEFAULT false,
      tts_provider VARCHAR(50) DEFAULT 'elevenlabs',
      tts_voice_id VARCHAR(100) DEFAULT '',
      tts_model VARCHAR(100) DEFAULT 'eleven_turbo_v2_5',
      tts_stability NUMERIC(3,2) DEFAULT 0.5,
      tts_similarity_boost NUMERIC(3,2) DEFAULT 0.75,
      tts_style NUMERIC(3,2) DEFAULT 0,
      tts_use_speaker_boost BOOLEAN DEFAULT true,
      tts_speed NUMERIC(3,2) DEFAULT 1.0,
      stt_provider VARCHAR(50) DEFAULT 'deepgram',
      stt_language VARCHAR(10) DEFAULT 'en',
      stt_keywords TEXT[] DEFAULT '{}',
      tools JSONB DEFAULT '[]',
      system_prompt TEXT DEFAULT '',
      first_message TEXT DEFAULT '',
      ivr_goal TEXT DEFAULT '',
      recording_enabled BOOLEAN DEFAULT true,
      recording_channels INTEGER DEFAULT 2,
      post_call_analyses JSONB DEFAULT '{"model":"us.anthropic.claude-haiku-4-5-20251001","fields":[]}',
      idle_timeout_secs INTEGER DEFAULT 30,
      idle_message TEXT DEFAULT '',
      max_call_duration_secs INTEGER DEFAULT 1800,
      voicemail_action VARCHAR(30) DEFAULT 'hang_up',
      voicemail_message TEXT DEFAULT '',
      max_retries INTEGER DEFAULT 3,
      retry_delay_secs INTEGER DEFAULT 300,
      default_concurrency INTEGER DEFAULT 1,
      calling_window_start VARCHAR(10) DEFAULT '09:00',
      calling_window_end VARCHAR(10) DEFAULT '17:00',
      calling_window_days TEXT[] DEFAULT '{mon,tue,wed,thu,fri}',
      is_active BOOLEAN DEFAULT true,
      speak_first BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`,
    // Agent Drafts (replaces Supabase)
    `CREATE TABLE IF NOT EXISTS voice_agent_drafts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      agent_id VARCHAR(100) NOT NULL UNIQUE,
      name VARCHAR(100) NOT NULL,
      display_name VARCHAR(200) DEFAULT '',
      description TEXT DEFAULT '',
      llm_provider VARCHAR(50) DEFAULT 'anthropic',
      llm_model VARCHAR(100) DEFAULT '',
      temperature NUMERIC(3,2) DEFAULT 0.7,
      max_tokens INTEGER DEFAULT 1024,
      enable_prompt_caching BOOLEAN DEFAULT false,
      tts_provider VARCHAR(50) DEFAULT 'elevenlabs',
      tts_voice_id VARCHAR(100) DEFAULT '',
      tts_model VARCHAR(100) DEFAULT '',
      tts_stability NUMERIC(3,2) DEFAULT 0.5,
      tts_similarity_boost NUMERIC(3,2) DEFAULT 0.75,
      tts_style NUMERIC(3,2) DEFAULT 0,
      tts_use_speaker_boost BOOLEAN DEFAULT true,
      tts_speed NUMERIC(3,2) DEFAULT 1.0,
      stt_provider VARCHAR(50) DEFAULT 'deepgram',
      stt_language VARCHAR(10) DEFAULT 'en',
      stt_keywords TEXT[] DEFAULT '{}',
      tools JSONB DEFAULT '[]',
      system_prompt TEXT DEFAULT '',
      first_message TEXT DEFAULT '',
      ivr_goal TEXT DEFAULT '',
      recording_enabled BOOLEAN DEFAULT true,
      recording_channels INTEGER DEFAULT 2,
      post_call_analyses JSONB DEFAULT '{"model":"us.anthropic.claude-haiku-4-5-20251001","fields":[]}',
      idle_timeout_secs INTEGER DEFAULT 30,
      idle_message TEXT DEFAULT '',
      max_call_duration_secs INTEGER DEFAULT 1800,
      voicemail_action VARCHAR(30) DEFAULT 'hang_up',
      voicemail_message TEXT DEFAULT '',
      max_retries INTEGER DEFAULT 3,
      retry_delay_secs INTEGER DEFAULT 300,
      default_concurrency INTEGER DEFAULT 1,
      calling_window_start VARCHAR(10) DEFAULT '09:00',
      calling_window_end VARCHAR(10) DEFAULT '17:00',
      calling_window_days TEXT[] DEFAULT '{mon,tue,wed,thu,fri}',
      has_unpublished_changes BOOLEAN DEFAULT false,
      speak_first BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`,
    // Agent Versions (replaces Supabase)
    `CREATE TABLE IF NOT EXISTS voice_agent_versions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      agent_id VARCHAR(100) NOT NULL,
      version_number INTEGER NOT NULL,
      version_name VARCHAR(200) DEFAULT '',
      description TEXT DEFAULT '',
      config_snapshot JSONB DEFAULT '{}',
      phone_assignments JSONB DEFAULT '[]',
      published_at TIMESTAMPTZ DEFAULT NOW(),
      published_by VARCHAR(200)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_vav_agent ON voice_agent_versions(agent_id, version_number DESC)`,
    // Drop and recreate calls if it exists with wrong schema
    // voice_calls table (no DROP — data must persist)
    `CREATE TABLE IF NOT EXISTS voice_calls (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      agent_name VARCHAR(100),
      agent_display_name VARCHAR(200),
      from_number VARCHAR(30),
      target_number VARCHAR(30) NOT NULL,
      direction VARCHAR(20) DEFAULT 'outbound',
      status VARCHAR(30) DEFAULT 'pending',
      started_at TIMESTAMPTZ,
      ended_at TIMESTAMPTZ,
      duration_secs INTEGER,
      case_data JSONB DEFAULT '{}',
      transcript JSONB DEFAULT '[]',
      recording_path TEXT,
      post_call_analyses JSONB DEFAULT '{}',
      error TEXT,
      batch_id UUID,
      batch_row_index INTEGER,
      session_id VARCHAR(100),
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`,
    `CREATE INDEX IF NOT EXISTS idx_vc_agent ON voice_calls(agent_name)`,
    `CREATE INDEX IF NOT EXISTS idx_vc_status ON voice_calls(status)`,
    `CREATE INDEX IF NOT EXISTS idx_vc_batch ON voice_calls(batch_id)`,
    `CREATE INDEX IF NOT EXISTS idx_vc_created ON voice_calls(created_at DESC)`,
    // Phase 7E PR 3: session_id correlates voice_calls to Daily's room_name
    // (and any other per-call artifact key). The recording webhook arrives
    // with payload.room_name and we use it to find the row to patch
    // recording_path on. Indexed for the lookup.
    `CREATE INDEX IF NOT EXISTS idx_vc_session ON voice_calls(session_id)`,
    // Batches
    `CREATE TABLE IF NOT EXISTS voice_batches (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(200) DEFAULT '',
      agent_name VARCHAR(100) NOT NULL,
      agent_display_name VARCHAR(200),
      from_number VARCHAR(30) NOT NULL,
      status VARCHAR(30) DEFAULT 'draft',
      total_rows INTEGER DEFAULT 0,
      completed_rows INTEGER DEFAULT 0,
      failed_rows INTEGER DEFAULT 0,
      column_mapping JSONB DEFAULT '{}',
      config JSONB DEFAULT '{}',
      rows JSONB DEFAULT '[]',
      input_file_path TEXT,
      output_file_path TEXT,
      timezone VARCHAR(50),
      calling_window_start VARCHAR(10),
      calling_window_end VARCHAR(10),
      calling_window_days TEXT[],
      concurrency INTEGER DEFAULT 1,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`,
    // Phone Numbers
    `CREATE TABLE IF NOT EXISTS voice_phone_numbers (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      number VARCHAR(30) UNIQUE NOT NULL,
      friendly_name VARCHAR(200) DEFAULT '',
      inbound_agent_id UUID,
      outbound_agent_id UUID,
      is_active BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`,
    // Voices
    `CREATE TABLE IF NOT EXISTS voice_voices (
      voice_id VARCHAR(100) PRIMARY KEY,
      name VARCHAR(200) NOT NULL,
      custom_name VARCHAR(200),
      description TEXT,
      preview_url TEXT,
      gender VARCHAR(30),
      accent VARCHAR(50),
      age VARCHAR(30),
      category VARCHAR(50),
      labels JSONB DEFAULT '{}',
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,

    // ─── TIER 2: Payer Knowledge Graph ─────────────────────────────────
    `CREATE TABLE IF NOT EXISTS voice_payer_knowledge (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      payer_name VARCHAR(200) NOT NULL,
      payer_id VARCHAR(100) UNIQUE,
      phone_claims VARCHAR(30),
      phone_auth VARCHAR(30),
      phone_eligibility VARCHAR(30),
      phone_appeals VARCHAR(30),
      fax_claims VARCHAR(30),
      fax_appeals VARCHAR(30),
      portal_url TEXT,
      ivr_path_claims JSONB DEFAULT '[]',
      ivr_path_auth JSONB DEFAULT '[]',
      ivr_path_eligibility JSONB DEFAULT '[]',
      timely_filing_days INTEGER DEFAULT 90,
      appeal_deadline_days INTEGER DEFAULT 60,
      corrected_claim_deadline_days INTEGER DEFAULT 90,
      accepts_electronic BOOLEAN DEFAULT true,
      payer_type VARCHAR(30) DEFAULT 'commercial',
      notes TEXT DEFAULT '',
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,

    // ─── TIER 2: Call Flow Templates ───────────────────────────────────
    `CREATE TABLE IF NOT EXISTS voice_call_templates (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(100) NOT NULL UNIQUE,
      display_name VARCHAR(200) NOT NULL,
      use_case VARCHAR(50) NOT NULL,
      description TEXT DEFAULT '',
      system_prompt TEXT NOT NULL,
      first_message TEXT DEFAULT '',
      required_fields JSONB DEFAULT '[]',
      success_criteria JSONB DEFAULT '[]',
      post_call_analyses JSONB DEFAULT '{"model":"us.anthropic.claude-haiku-4-5-20251001","fields":[]}',
      llm_model VARCHAR(100) DEFAULT 'us.anthropic.claude-sonnet-4-6',
      is_default BOOLEAN DEFAULT false,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,

    // ─── TIER 3: Post-Call Auto-Actions log ────────────────────────────
    `CREATE TABLE IF NOT EXISTS voice_auto_actions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      call_id UUID REFERENCES voice_calls(id),
      action_type VARCHAR(50) NOT NULL,
      target_table VARCHAR(50),
      target_id UUID,
      payload JSONB DEFAULT '{}',
      status VARCHAR(20) DEFAULT 'completed',
      error TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,

    // ─── TIER 4: Per-Call Cost Tracking ────────────────────────────────
    `CREATE TABLE IF NOT EXISTS voice_call_costs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      call_id UUID REFERENCES voice_calls(id) UNIQUE,
      telephony_cost NUMERIC(8,4) DEFAULT 0,
      stt_cost NUMERIC(8,4) DEFAULT 0,
      llm_cost NUMERIC(8,4) DEFAULT 0,
      tts_cost NUMERIC(8,4) DEFAULT 0,
      total_cost NUMERIC(8,4) DEFAULT 0,
      telephony_minutes NUMERIC(8,2) DEFAULT 0,
      llm_tokens_in INTEGER DEFAULT 0,
      llm_tokens_out INTEGER DEFAULT 0,
      tts_chars INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,

    // ─── TIER 4: Inbound Call Routing ──────────────────────────────────
    `CREATE TABLE IF NOT EXISTS voice_inbound_routes (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      phone_number VARCHAR(30) NOT NULL,
      agent_name VARCHAR(100) REFERENCES voice_agents(name),
      routing_rules JSONB DEFAULT '{}',
      greeting TEXT DEFAULT 'Thank you for calling. How can I help you?',
      active BOOLEAN DEFAULT true,
      calls_received INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,

    // ─── TIER 3: Call Quality Scores ───────────────────────────────────
    `CREATE TABLE IF NOT EXISTS voice_call_scores (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      call_id UUID REFERENCES voice_calls(id) UNIQUE,
      overall_score INTEGER,
      collected_reference BOOLEAN DEFAULT false,
      collected_deadline BOOLEAN DEFAULT false,
      collected_action BOOLEAN DEFAULT false,
      collected_fax_portal BOOLEAN DEFAULT false,
      collected_rep_name BOOLEAN DEFAULT false,
      confirmed_denial_reason BOOLEAN DEFAULT false,
      asked_reprocess BOOLEAN DEFAULT false,
      professional_tone BOOLEAN DEFAULT true,
      score_details JSONB DEFAULT '{}',
      created_at TIMESTAMPTZ DEFAULT NOW()
    )`,
  ];
  for (const sql of statements) {
    try { await pool.query(sql); } catch (e) { console.warn('[voice-api] Table setup warning:', e.message?.substring(0, 100)); }
  }
  // Idempotent column additions for existing tables. ADD COLUMN IF NOT EXISTS
  // is supported on Postgres 9.6+. If the column is already there this is a
  // no-op.
  const migrations = [
    `ALTER TABLE voice_agents       ADD COLUMN IF NOT EXISTS ivr_goal TEXT DEFAULT ''`,
    `ALTER TABLE voice_agent_drafts ADD COLUMN IF NOT EXISTS ivr_goal TEXT DEFAULT ''`,
    // Phone-number provider support. Defaults to 'twilio' so every
    // existing row gets that value — existing Twilio numbers keep
    // working unchanged while Daily rolls out. daily_number_id is
    // only populated for provider='daily' rows; it's the UUID returned
    // by Daily's POST /buy-phone-number and is required for release.
    `ALTER TABLE voice_phone_numbers ADD COLUMN IF NOT EXISTS provider VARCHAR(20) NOT NULL DEFAULT 'twilio'`,
    `ALTER TABLE voice_phone_numbers ADD COLUMN IF NOT EXISTS daily_number_id UUID`,
    `ALTER TABLE voice_phone_numbers ADD CONSTRAINT voice_phone_numbers_provider_valid CHECK (provider IN ('twilio', 'daily')) NOT VALID`,
    // Phase 7E PR 3: session_id correlates voice_calls rows to Daily room_name
    // (= what the recording webhook brings back). Idempotent ALTER + index
    // for existing deployments that pre-date the CREATE TABLE addition.
    `ALTER TABLE voice_calls ADD COLUMN IF NOT EXISTS session_id VARCHAR(100)`,
    `CREATE INDEX IF NOT EXISTS idx_vc_session ON voice_calls(session_id)`,
    // v2 Layer 1 follow-up: speak_first controls whether the bot
    // generates an opening message or waits for the user. Defaults
    // to true so every existing agent keeps its current behavior
    // (all agents in production today implicitly speak first via a
    // non-empty first_message).
    `ALTER TABLE voice_agents       ADD COLUMN IF NOT EXISTS speak_first BOOLEAN NOT NULL DEFAULT true`,
    `ALTER TABLE voice_agent_drafts ADD COLUMN IF NOT EXISTS speak_first BOOLEAN NOT NULL DEFAULT true`,
  ];
  for (const sql of migrations) {
    try { await pool.query(sql); } catch (e) { console.warn('[voice-api] Migration warning:', e.message?.substring(0, 100)); }
  }
  tablesReady = true;
  console.log('[voice-api] Tables ready');
}

// ─── Helpers ───────────────────────────────────────────────────────────────
function parseRoute(event) {
  let path = event.rawPath || event.path || '/';
  // Strip API Gateway stage + route prefix (e.g. /prod/voice/api/health → /api/health)
  path = path.replace(/^\/prod\/voice/, '').replace(/^\/voice/, '') || '/';
  const method = event.requestContext?.http?.method || event.httpMethod || 'GET';
  const qs = event.queryStringParameters || {};
  let body = {};
  try { body = event.body ? JSON.parse(typeof event.body === 'string' ? event.body : JSON.stringify(event.body)) : {}; } catch { body = {}; }
  
  // Parse path segments: /api/agents/chris/prompt → resource=agents, id=chris, sub=prompt
  const parts = path.replace(/^\/api\//, '').split('/').filter(Boolean);
  const resource = parts[0] || '';
  const id = parts[1] || null;
  const sub = parts[2] || null;
  
  return { path, method, qs, body, resource, id, sub, parts };
}

const AGENT_COLS = `id, name, display_name, description, llm_provider, llm_model, temperature, max_tokens, enable_prompt_caching, tts_provider, tts_voice_id, tts_model, tts_stability, tts_similarity_boost, tts_style, tts_use_speaker_boost, tts_speed, stt_provider, stt_language, stt_keywords, tools, system_prompt, first_message, ivr_goal, recording_enabled, recording_channels, post_call_analyses, idle_timeout_secs, idle_message, max_call_duration_secs, voicemail_action, voicemail_message, max_retries, retry_delay_secs, default_concurrency, calling_window_start, calling_window_end, calling_window_days, is_active, speak_first, created_at, updated_at`;

// E.164 validator + normalizer. Used by GET /api/phone-numbers/lookup
// (Phase 7B bot-runner path). Daily occasionally drops the leading `+`
// from webhook bodies, so we accept both `+12098075018` and
// `12098075018` and always store/query with the `+`. Exported for
// test/phone-lookup.test.mjs.
//
// Regex rules:
//   - starts with `+`
//   - next digit is 1–9 (no leading zero after the +)
//   - then 6–14 more digits (E.164 is 8–15 digits total including country code)
export function normalizeE164(raw) {
  if (typeof raw !== 'string') return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const withPlus = trimmed.startsWith('+') ? trimmed : `+${trimmed}`;
  if (!/^\+[1-9]\d{6,14}$/.test(withPlus)) return null;
  return withPlus;
}

// ─── Validation constants (shared by schema endpoint + write handlers) ─────
// NOTE: the following module-level constants and validators are `export`ed
// purely so the test suite in `test/` can import them. This has zero runtime
// effect on the Lambda handler — the deploy zip still exposes `handler` as
// the only Lambda entrypoint.
export const VALID_LLM_MODELS = [
  'claude-sonnet-4-6',
  'claude-haiku-4-5-20241022',
  'claude-sonnet-4-5-20250514',
];
export const VALID_TTS_PROVIDERS = ['elevenlabs'];
// Per-provider model lists. Keep flat VALID_TTS_MODELS for legacy consumers
// that don't know about providers yet.
export const TTS_MODELS_BY_PROVIDER = {
  elevenlabs: ['eleven_turbo_v2_5', 'eleven_flash_v2_5', 'eleven_multilingual_v2'],
};
export const VALID_TTS_MODELS = Object.values(TTS_MODELS_BY_PROVIDER).flat();
export const VALID_STT_PROVIDERS = ['deepgram', 'elevenlabs'];
export const VALID_STT_LANGUAGES = ['en', 'es', 'fr', 'de', 'pt', 'it', 'ja', 'ko', 'zh', 'ar', 'hi'];
export const VALID_TOOL_TYPES = ['end_call', 'press_digit', 'transfer_call'];
export const VALID_VOICEMAIL_ACTIONS = ['hang_up', 'leave_message'];
export const VALID_CALLING_DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
export const VALID_POST_CALL_FIELD_TYPES = ['text', 'selector'];

export const FIELD_RANGES = {
  temperature:            { min: 0.0, max: 1.0,  step: 0.05, default: 0.2 },
  max_tokens:             { min: 50,  max: 1000, step: 10,   default: 390 },
  tts_stability:          { min: 0.0, max: 1.0,  step: 0.05, default: 0.75 },
  tts_similarity_boost:   { min: 0.0, max: 1.0,  step: 0.05, default: 0.8 },
  tts_style:              { min: 0.0, max: 1.0,  step: 0.05, default: 0.0 },
  tts_speed:              { min: 0.7, max: 1.2,  step: 0.05, default: 1.0 },
  idle_timeout_secs:      { min: 5,   max: 120,  step: 1,    default: 30 },
  max_call_duration_secs: { min: 60,  max: 3600, step: 30,   default: 1800 },
  max_retries:            { min: 0,   max: 10,   step: 1,    default: 3 },
  retry_delay_secs:       { min: 30,  max: 3600, step: 30,   default: 300 },
  default_concurrency:    { min: 1,   max: 50,   step: 1,    default: 1 },
  recording_channels:     { min: 1,   max: 2,    step: 1,    default: 2 },
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const E164_RE = /^\+[1-9]\d{6,14}$/;
const AGENT_NAME_RE = /^[a-z0-9][a-z0-9\-]*$/;
const VOICE_ID_RE = /^[a-zA-Z0-9]{10,40}$/;
const TIME_RE = /^\d{2}:\d{2}(:\d{2})?$/;

/**
 * Build the runtime-config shape consumed by the Fargate voice pipeline.
 *
 * Returns an OG-compatible AgentConfig object (nested llm / tts / stt /
 * tools / recording / post_call_analyses) so the forked cosentus-voice-engine
 * can parse identical shapes with zero translation. This intentionally
 * mirrors `voiceagent/core/config_loader.py::row_to_config` in the old
 * repo — any schema additions here need to land on both sides.
 *
 * Fork-specific notes for downstream consumers:
 * - `llm.provider` may be 'anthropic' from Aurora. The fork ignores that
 *   and always routes through Bedrock; see the fork's
 *   `_resolve_bedrock_model_id()` adapter.
 * - `tools` is passed through as `[{type, description, settings}, ...]`.
 *   The fork maps each `type` to a local Python tool implementation in
 *   Phase 7C; until then it's informational.
 * - `post_call_analyses` and `recording` are pass-through for Phase 7E.
 */
export function buildRuntimeConfig(row) {
  // pg returns NUMERIC columns as strings. Convert defensively so consumers
  // get real numbers instead of "0.75". Belongs in a type parser one day
  // (tech-debt issue #4) — for now it's enough to fix at the response
  // boundary of this single endpoint.
  const num = (v, fallback = null) => {
    if (v === null || v === undefined) return fallback;
    const n = typeof v === 'number' ? v : parseFloat(v);
    return Number.isFinite(n) ? n : fallback;
  };

  const pcaRaw = row.post_call_analyses;
  const pcaConfig =
    pcaRaw && typeof pcaRaw === 'object' && Array.isArray(pcaRaw.fields)
      ? {
          model: pcaRaw.model || 'claude-haiku-4-5-20251001',
          fields: pcaRaw.fields
            .filter((f) => f && typeof f === 'object')
            .map((f) => ({
              name: f.name || '',
              type: f.type || 'text',
              description: f.description || '',
              format_examples: Array.isArray(f.format_examples) ? f.format_examples : [],
              choices: Array.isArray(f.choices) ? f.choices : [],
            })),
        }
      : null;

  const toolsRaw = Array.isArray(row.tools) ? row.tools : [];
  const tools = toolsRaw
    .filter((t) => t && typeof t === 'object' && typeof t.type === 'string')
    .map((t) => ({
      type: t.type,
      description: typeof t.description === 'string' ? t.description : '',
      settings: t.settings && typeof t.settings === 'object' ? t.settings : {},
    }));

  return {
    name: row.name,
    display_name: row.display_name || row.name,
    description: row.description || '',
    system_prompt: row.system_prompt || '',
    first_message: row.first_message || '',
    ivr_goal: row.ivr_goal || '',
    // v2 Layer 1 follow-up. Defaults to true for any pre-migration
    // row where the column is null (shouldn't happen — column is
    // NOT NULL — but defensive coalesce matches every other field).
    speak_first: row.speak_first === false ? false : true,
    llm: {
      provider: row.llm_provider || 'anthropic',
      model: row.llm_model || 'claude-sonnet-4-6',
      max_tokens: num(row.max_tokens, 200),
      temperature: num(row.temperature, 0.7),
      enable_prompt_caching: Boolean(row.enable_prompt_caching),
    },
    tts: {
      provider: row.tts_provider || 'elevenlabs',
      voice_id: row.tts_voice_id || '',
      model: row.tts_model || 'eleven_turbo_v2_5',
      settings: {
        stability: num(row.tts_stability),
        similarity_boost: num(row.tts_similarity_boost),
        style: num(row.tts_style),
        use_speaker_boost:
          row.tts_use_speaker_boost === null || row.tts_use_speaker_boost === undefined
            ? null
            : Boolean(row.tts_use_speaker_boost),
        speed: num(row.tts_speed),
      },
    },
    stt: {
      provider: row.stt_provider || 'deepgram',
      language: row.stt_language || 'en',
      keywords: Array.isArray(row.stt_keywords) ? row.stt_keywords : [],
    },
    tools,
    recording: {
      enabled: row.recording_enabled !== false,
      channels: num(row.recording_channels, 2),
    },
    post_call_analyses: pcaConfig,
    // Metadata for pipeline's structured log `agent_config_loaded`.
    // `version` is the row's updated_at as unix millis so the fork can
    // log it without parsing ISO strings.
    _meta: {
      agent_id: row.id,
      version: row.updated_at ? new Date(row.updated_at).getTime() : 0,
    },
  };
}

/**
 * Validate the merged state of a draft save (UPSERT or UPDATE).
 *
 * Pure helper — takes the existing DB row (or null/undefined) and the
 * incoming patch, returns validation errors. Extracted from the POST and
 * PUT /api/agent-drafts handlers so the same merge-and-validate code path
 * runs for both and so it's unit-testable without mocking `pool`.
 *
 * The ORDER of spread matters: existing → incoming. Incoming fields
 * overwrite existing ones, matching pg's ON CONFLICT DO UPDATE semantics.
 */
export function validateDraftMerged(existingRow, incomingBody) {
  const merged = { ...(existingRow || {}), ...(incomingBody || {}) };
  return validateAgentData(merged);
}

export function validateAgentData(data) {
  const errors = [];

  // Numeric ranges
  for (const [field, range] of Object.entries(FIELD_RANGES)) {
    if (data[field] !== undefined && data[field] !== null) {
      const val = parseFloat(data[field]);
      if (isNaN(val)) {
        errors.push(`${field} must be a number, got: ${data[field]}`);
      } else if (val < range.min || val > range.max) {
        errors.push(`${field} must be between ${range.min} and ${range.max}, got: ${val}`);
      }
    }
  }

  // Agent name format
  if (data.name !== undefined && data.name !== null && !AGENT_NAME_RE.test(data.name)) {
    errors.push('name must be lowercase alphanumeric with hyphens (e.g., chris-claim-status)');
  }

  // String length limits
  if (data.display_name && data.display_name.length > 200) {
    errors.push('display_name must be 200 characters or fewer');
  }
  if (data.system_prompt && data.system_prompt.length > 50000) {
    errors.push('system_prompt must be 50,000 characters or fewer');
  }
  // ivr_goal: optional, no length constraint — it's a free-form navigation
  // goal passed verbatim to Pipecat's IVRNavigator.
  if (data.first_message && data.first_message.length > 2000) {
    errors.push('first_message must be 2,000 characters or fewer');
  }

  // LLM model
  if (data.llm_model && !VALID_LLM_MODELS.includes(data.llm_model)) {
    errors.push(`Invalid llm_model: ${data.llm_model}. Valid: ${VALID_LLM_MODELS.join(', ')}`);
  }

  // TTS
  if (data.tts_provider && !VALID_TTS_PROVIDERS.includes(data.tts_provider)) {
    errors.push(`Invalid tts_provider: ${data.tts_provider}. Valid: ${VALID_TTS_PROVIDERS.join(', ')}`);
  }
  if (data.tts_model) {
    // Provider-aware model validation: if a provider is specified (or being
    // changed in this update), only accept models valid for that provider.
    // Fall back to the flat list if provider context isn't available.
    const providerForModel = data.tts_provider || null;
    const allowed = providerForModel
      ? (TTS_MODELS_BY_PROVIDER[providerForModel] || [])
      : VALID_TTS_MODELS;
    if (!allowed.includes(data.tts_model)) {
      errors.push(
        `Invalid tts_model: ${data.tts_model}` +
        (providerForModel ? ` for provider ${providerForModel}` : '') +
        `. Valid: ${allowed.join(', ')}`
      );
    }
  }
  if (data.tts_voice_id && data.tts_voice_id !== '' && !VOICE_ID_RE.test(data.tts_voice_id)) {
    errors.push(`Invalid tts_voice_id format: ${data.tts_voice_id}`);
  }

  // STT
  if (data.stt_provider && !VALID_STT_PROVIDERS.includes(data.stt_provider)) {
    errors.push(`Invalid stt_provider: ${data.stt_provider}. Valid: ${VALID_STT_PROVIDERS.join(', ')}`);
  }
  if (data.stt_language && !VALID_STT_LANGUAGES.includes(data.stt_language)) {
    errors.push(`Invalid stt_language: ${data.stt_language}. Valid: ${VALID_STT_LANGUAGES.join(', ')}`);
  }

  // Voicemail action
  if (data.voicemail_action && !VALID_VOICEMAIL_ACTIONS.includes(data.voicemail_action)) {
    errors.push(`Invalid voicemail_action: ${data.voicemail_action}. Valid: ${VALID_VOICEMAIL_ACTIONS.join(', ')}`);
  }

  // Recording channels
  if (data.recording_channels !== undefined && data.recording_channels !== null && ![1, 2].includes(data.recording_channels)) {
    errors.push('recording_channels must be 1 or 2');
  }

  // Calling window — agent-level fields are deprecated (batch-only now)
  // Skipping validation; they're harmless on the agent row but ignored by the calling engine.
  // Kept inert for backward compat with older frontend payloads.
  if (false && data.calling_window_days && Array.isArray(data.calling_window_days)) {
    for (const day of data.calling_window_days) {
      if (!VALID_CALLING_DAYS.includes(day)) {
        errors.push(`Invalid calling day: "${day}". Valid: ${VALID_CALLING_DAYS.join(', ')}`);
      }
    }
  }

  // Tools — deep validation
  if (data.tools && Array.isArray(data.tools)) {
    for (let i = 0; i < data.tools.length; i++) {
      const tool = data.tools[i];
      if (!tool.type) {
        errors.push(`tools[${i}]: missing type`);
        continue;
      }
      if (!VALID_TOOL_TYPES.includes(tool.type)) {
        errors.push(`tools[${i}]: invalid type "${tool.type}". Valid: ${VALID_TOOL_TYPES.join(', ')}`);
      }
      if (tool.type === 'transfer_call' && tool.settings?.targets && typeof tool.settings.targets === 'object') {
        for (const [name, number] of Object.entries(tool.settings.targets)) {
          if (!E164_RE.test(number)) {
            errors.push(`tools[${i}] transfer target "${name}": "${number}" is not valid E.164 format`);
          }
        }
      }
    }
  }

  // Post-call analyses — deep validation
  const pca = data.post_call_analyses;
  if (pca && typeof pca === 'object' && Array.isArray(pca.fields)) {
    for (let i = 0; i < pca.fields.length; i++) {
      const f = pca.fields[i];
      if (!f.name) {
        errors.push(`post_call_analyses.fields[${i}]: missing name`);
      }
      if (f.type && !VALID_POST_CALL_FIELD_TYPES.includes(f.type)) {
        errors.push(`post_call_analyses.fields[${i}]: invalid type "${f.type}". Valid: ${VALID_POST_CALL_FIELD_TYPES.join(', ')}`);
      }
      if (f.type === 'selector' && (!Array.isArray(f.choices) || f.choices.length === 0)) {
        errors.push(`post_call_analyses.fields[${i}]: selector type requires non-empty choices array`);
      }
    }
  }

  return errors;
}

function validatePhoneData(data) {
  const errors = [];
  if (data.number && !E164_RE.test(data.number)) {
    errors.push('number must be E.164 format (e.g., +14155551234)');
  }
  if (data.inbound_agent_id && data.inbound_agent_id !== '' && !UUID_RE.test(data.inbound_agent_id)) {
    errors.push('inbound_agent_id must be a valid UUID');
  }
  if (data.outbound_agent_id && data.outbound_agent_id !== '' && !UUID_RE.test(data.outbound_agent_id)) {
    errors.push('outbound_agent_id must be a valid UUID');
  }
  return errors;
}

// ─── Agent Schema (static config for frontend dropdowns) ──────────────────
const AGENT_SCHEMA = {
  llm_models: VALID_LLM_MODELS,
  tts_providers: VALID_TTS_PROVIDERS,
  tts_models: VALID_TTS_MODELS,
  tts_models_by_provider: TTS_MODELS_BY_PROVIDER,
  stt_providers: VALID_STT_PROVIDERS,
  stt_languages: VALID_STT_LANGUAGES,
  tool_types: VALID_TOOL_TYPES,
  tool_settings_schema: {
    end_call: {},
    press_digit: {},
    transfer_call: {
      targets: {
        type: 'object',
        label: 'Named transfer targets',
        description: 'Keys are target names the LLM picks from, values are E.164 phone numbers',
      },
    },
  },
  voicemail_actions: VALID_VOICEMAIL_ACTIONS,
  calling_days: VALID_CALLING_DAYS,
  post_call_field_types: VALID_POST_CALL_FIELD_TYPES,
  max_prompt_length: 50000,
  max_display_name_length: 200,
  max_first_message_length: 2000,
  valid_recording_channels: [1, 2],
  field_ranges: { ...FIELD_RANGES, tts_use_speaker_boost: { default: false } },
  defaults: {
    // LLM
    temperature: 0.2,
    max_tokens: 390,
    llm_provider: 'anthropic',
    llm_model: 'claude-sonnet-4-6',
    enable_prompt_caching: true,

    // TTS
    tts_provider: 'elevenlabs',
    tts_model: 'eleven_turbo_v2_5',
    tts_voice_id: '',
    tts_stability: 0.75,
    tts_similarity_boost: 0.8,
    tts_style: 0.0,
    tts_speed: 1.0,
    tts_use_speaker_boost: false,

    // STT
    stt_provider: 'elevenlabs',
    stt_language: 'en',
    stt_keywords: [],

    // Content
    description: '',
    system_prompt: '',
    first_message: '',
    ivr_goal: '',

    // Behavior / timing
    idle_timeout_secs: 30,
    idle_message: '',
    max_call_duration_secs: 1800,
    voicemail_action: 'hang_up',
    voicemail_message: '',
    max_retries: 3,
    retry_delay_secs: 300,
    default_concurrency: 1,

    // Tools / post-call
    tools: [],
    post_call_analyses: { model: 'claude-sonnet-4-6', fields: [] },

    // Recording — system-level, always on. Exposed so the frontend mapper has
    // fallback values; not user-configurable.
    recording_enabled: true,
    recording_channels: 2,

    // Agent state
    is_active: true,

    // NOTE: calling_window_* belong to batches, not agents. The frontend
    // reads them via a soft fallback (empty string / empty array) so they
    // are intentionally omitted here.
  },
};

// ─── Handler ───────────────────────────────────────────────────────────────
export async function handler(event) {
  // CORS preflight
  if (event.requestContext?.http?.method === 'OPTIONS' || event.httpMethod === 'OPTIONS') {
    return respond(200, { ok: true });
  }

  await ensureTables();
  const { path, method, qs, body, resource, id, sub, parts } = parseRoute(event);

  try {
    // ═══ Health ═══
    // Returns 200 with DB reachability + deploy metadata. Safe to hit
    // unauthenticated — the frontend's deploy.sh uses this as a smoke test
    // after each Lambda deploy to verify the expected version is live.
    if (resource === 'health') {
      const r = await pool.query('SELECT 1 as ok');
      return respond(200, {
        status: 'healthy',
        db: r.rows[0]?.ok === 1,
        version: 'voice-api-1.1.0',
        // Populated at deploy time by scripts/deploy.sh via an env var so
        // we can correlate a live /health response to a specific commit
        // in the cosentus-voice-api-lambda repo. Falls back to 'unknown'
        // when the env var isn't set (e.g. legacy deploys).
        git_sha: process.env.BUILD_SHA || 'unknown',
      });
    }

    // ═══ Agent Schema ═══
    if (resource === 'agent-schema' && method === 'GET') {
      return respond(200, AGENT_SCHEMA);
    }

    // ═══ Agents ═══
    if (resource === 'agents') {
      // GET /api/agents — list all
      if (method === 'GET' && !id) {
        const r = await pool.query('SELECT id, name, display_name, description, llm_model, tts_model, tts_voice_id FROM voice_agents ORDER BY display_name');
        return respond(200, { agents: r.rows });
      }

      // GET /api/calls/agents — distinct agent names from calls
      if (id === 'agents' && method === 'GET' && parts[0] === 'calls') {
        // This is actually under /api/calls/agents, handled below
      }

      // POST /api/agents — create
      if (method === 'POST' && !id) {
        const vErrors = validateAgentData(body);
        if (vErrors.length) return respond(400, { error: 'Validation failed', details: vErrors });
        const name = body.name?.trim();
        if (!name) return respond(400, { detail: 'name is required' });
        const existing = await pool.query('SELECT id FROM voice_agents WHERE name = $1', [name]);
        if (existing.rows.length) return respond(409, { detail: `Agent "${name}" already exists` });
        const newId = uuid();
        const cols = ['id', 'name', 'display_name', 'description', 'llm_provider', 'llm_model', 'temperature', 'max_tokens', 'system_prompt', 'first_message', 'tts_provider', 'tts_voice_id', 'tts_model', 'tools', 'post_call_analyses', 'stt_provider', 'stt_language', 'speak_first'];
        const vals = [newId, name, body.display_name || name, body.description || '', body.llm_provider || 'anthropic', body.llm_model || 'us.anthropic.claude-sonnet-4-6', body.temperature ?? 0.7, body.max_tokens ?? 1024, body.system_prompt || '', body.first_message || '', body.tts_provider || 'elevenlabs', body.tts_voice_id || '', body.tts_model || 'eleven_turbo_v2_5', JSON.stringify(body.tools || []), JSON.stringify(body.post_call_analyses || { model: 'us.anthropic.claude-haiku-4-5-20251001', fields: [] }), body.stt_provider || 'elevenlabs', body.stt_language || 'en', body.speak_first === false ? false : true];
        const placeholders = vals.map((_, i) => `$${i + 1}`).join(', ');
        await pool.query(`INSERT INTO voice_agents (${cols.join(', ')}) VALUES (${placeholders})`, vals);
        const r = await pool.query(`SELECT ${AGENT_COLS} FROM voice_agents WHERE id = $1`, [newId]);
        return respond(201, r.rows[0]);
      }

      // GET /api/agents/:name
      if (method === 'GET' && id && !sub) {
        const r = await pool.query(`SELECT ${AGENT_COLS} FROM voice_agents WHERE name = $1`, [id]);
        if (!r.rows.length) return respond(404, { detail: 'Agent not found' });
        return respond(200, r.rows[0]);
      }

      // PUT /api/agents/:name — full update
      if (method === 'PUT' && id && !sub) {
        const vErrors = validateAgentData(body);
        if (vErrors.length) return respond(400, { error: 'Validation failed', details: vErrors });
        const r = await pool.query('SELECT id FROM voice_agents WHERE name = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Agent not found' });
        const agentId = r.rows[0].id;
        const updates = [];
        const vals = [];
        let idx = 1;
        const allowed = ['display_name', 'description', 'llm_provider', 'llm_model', 'temperature', 'max_tokens', 'enable_prompt_caching', 'tts_provider', 'tts_voice_id', 'tts_model', 'tts_stability', 'tts_similarity_boost', 'tts_style', 'tts_use_speaker_boost', 'tts_speed', 'stt_provider', 'stt_language', 'stt_keywords', 'tools', 'system_prompt', 'first_message', 'ivr_goal', 'recording_enabled', 'recording_channels', 'post_call_analyses', 'idle_timeout_secs', 'idle_message', 'max_call_duration_secs', 'voicemail_action', 'voicemail_message', 'max_retries', 'retry_delay_secs', 'default_concurrency', 'calling_window_start', 'calling_window_end', 'calling_window_days', 'is_active', 'speak_first'];
        for (const col of allowed) {
          if (body[col] !== undefined) {
            updates.push(`${col} = $${idx}`);
            vals.push(typeof body[col] === 'object' ? JSON.stringify(body[col]) : body[col]);
            idx++;
          }
        }
        if (updates.length) {
          updates.push(`updated_at = NOW()`);
          vals.push(agentId);
          await pool.query(`UPDATE voice_agents SET ${updates.join(', ')} WHERE id = $${idx}`, vals);
        }
        const updated = await pool.query(`SELECT ${AGENT_COLS} FROM voice_agents WHERE id = $1`, [agentId]);
        return respond(200, updated.rows[0]);
      }

      // DELETE /api/agents/:name
      if (method === 'DELETE' && id && !sub) {
        const r = await pool.query('DELETE FROM voice_agents WHERE name = $1 RETURNING id', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Agent not found' });
        await pool.query('DELETE FROM voice_agent_drafts WHERE agent_id = $1', [id]);
        return respond(200, { deleted: true });
      }

      // POST /api/agents/:name/clone
      if (method === 'POST' && id && sub === 'clone') {
        const src = await pool.query(`SELECT ${AGENT_COLS} FROM voice_agents WHERE name = $1`, [id]);
        if (!src.rows.length) return respond(404, { detail: 'Source agent not found' });
        const newName = body.name?.trim();
        if (!newName) return respond(400, { detail: 'name required for clone' });
        const agent = src.rows[0];
        const newId = uuid();
        await pool.query(
          `INSERT INTO voice_agents (id, name, display_name, description, llm_provider, llm_model, temperature, max_tokens, system_prompt, first_message, tts_provider, tts_voice_id, tts_model, tools, post_call_analyses, stt_provider, stt_language, speak_first)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
          [newId, newName, body.display_name || newName, agent.description, agent.llm_provider, agent.llm_model, agent.temperature, agent.max_tokens, agent.system_prompt, agent.first_message, agent.tts_provider, agent.tts_voice_id, agent.tts_model, JSON.stringify(agent.tools), JSON.stringify(agent.post_call_analyses), agent.stt_provider, agent.stt_language, agent.speak_first === false ? false : true]
        );
        const r = await pool.query(`SELECT ${AGENT_COLS} FROM voice_agents WHERE id = $1`, [newId]);
        return respond(201, r.rows[0]);
      }

      // GET /api/agents/:id/runtime-config
      //
      // Returns the full agent config the Fargate pipeline needs to run a
      // call. Shape matches the OG voiceagent/core/config_loader.py
      // AgentConfig model exactly — nested llm/tts/stt/tools/recording/
      // post_call_analyses. See `buildRuntimeConfig` for the field mapping.
      //
      // `:id` accepts BOTH the agent UUID (Daily inbound webhook in 7B
      // passes voice_phone_numbers.inbound_agent_id which is UUID) and the
      // agent name (scripts + the OG EC2 pipeline's convention). Detected
      // via UUID_RE; the lookup column switches accordingly.
      if (method === 'GET' && id && sub === 'runtime-config') {
        const isUuid = UUID_RE.test(id);
        const r = await pool.query(
          `SELECT ${AGENT_COLS} FROM voice_agents WHERE ${isUuid ? 'id' : 'name'} = $1`,
          [id]
        );
        if (!r.rows.length) return respond(404, { detail: 'Agent not found' });
        return respond(200, buildRuntimeConfig(r.rows[0]));
      }

      // GET /api/agents/:name/prompt
      if (method === 'GET' && id && sub === 'prompt') {
        const r = await pool.query('SELECT system_prompt FROM voice_agents WHERE name = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Agent not found' });
        const content = r.rows[0].system_prompt || '';
        const vars = [...content.matchAll(/\{\{\s*([^}]+?)\s*\}\}/g)].map(m => m[1].trim());
        return respond(200, { content, prompt_variables: [...new Set(vars)] });
      }

      // PUT /api/agents/:name/prompt
      if (method === 'PUT' && id && sub === 'prompt') {
        const { content } = body;
        if (typeof content !== 'string') return respond(400, { detail: 'content required' });
        await pool.query('UPDATE voice_agents SET system_prompt = $1, updated_at = NOW() WHERE name = $2', [content, id]);
        const vars = [...content.matchAll(/\{\{\s*([^}]+?)\s*\}\}/g)].map(m => m[1].trim());
        return respond(200, { prompt_variables: [...new Set(vars)], prompt_preview: content.substring(0, 500) });
      }

      // ─── Nested aliases for frontend's RESTful path style ─────────
      // The voiceagentfront app calls /api/agents/:name/draft and
      // /api/agents/:name/versions instead of the older flat
      // /api/agent-drafts/:id and /api/agent-versions/:id routes.
      // Both shapes resolve to the same handlers — kept defensive so
      // any other caller still using the flat paths keeps working.
      // Frontend-v2 alignment audit B1/B2/B3/B4 (2026-05-19).

      // GET /api/agents/:name/draft → forwards to agent-drafts GET logic
      if (method === 'GET' && id && sub === 'draft') {
        const r = await pool.query('SELECT * FROM voice_agent_drafts WHERE agent_id = $1', [id]);
        return respond(200, r.rows[0] || null);
      }

      // PUT /api/agents/:name/draft → forwards to agent-drafts PUT logic
      if (method === 'PUT' && id && sub === 'draft') {
        // Mirror the flat /api/agent-drafts/:id behavior exactly:
        // load existing, validate merged state, then UPDATE/UPSERT.
        const existing = await pool.query(
          'SELECT * FROM voice_agent_drafts WHERE agent_id = $1', [id]
        );
        const vErrors = validateDraftMerged(existing.rows[0], body);
        if (vErrors.length) return respond(400, { error: 'Validation failed', details: vErrors });
        body.updated_at = new Date().toISOString();
        for (const k of Object.keys(body)) {
          if (typeof body[k] === 'object' && body[k] !== null) body[k] = JSON.stringify(body[k]);
        }
        const cols = Object.keys(body).filter(k => /^[a-z_]+$/.test(k) && k !== 'id' && k !== 'agent_id');
        if (!cols.length) {
          const r = await pool.query('SELECT * FROM voice_agent_drafts WHERE agent_id = $1', [id]);
          return respond(200, r.rows[0] || null);
        }
        const vals = cols.map(k => body[k]);
        vals.push(id);
        const sets = cols.map((c, i) => `${c} = $${i + 1}`).join(', ');
        // UPSERT-on-update behavior: if no row exists for this
        // agent_id yet, INSERT one so the frontend's "init draft on
        // first patch" pattern works.
        const updated = await pool.query(
          `UPDATE voice_agent_drafts SET ${sets} WHERE agent_id = $${vals.length} RETURNING *`,
          vals,
        );
        if (updated.rows.length) {
          return respond(200, updated.rows[0]);
        }
        // No existing draft — create one. INSERT with the patch
        // values + agent_id; missing columns fall back to defaults.
        const insertCols = ['agent_id', ...cols];
        const insertVals = [id, ...cols.map(k => body[k])];
        const placeholders = insertCols.map((_, i) => `$${i + 1}`).join(', ');
        await pool.query(
          `INSERT INTO voice_agent_drafts (${insertCols.join(', ')}) VALUES (${placeholders})
             ON CONFLICT (agent_id) DO UPDATE SET ${cols.map(c => `${c} = EXCLUDED.${c}`).join(', ')}`,
          insertVals,
        );
        const after = await pool.query('SELECT * FROM voice_agent_drafts WHERE agent_id = $1', [id]);
        return respond(200, after.rows[0]);
      }

      // DELETE /api/agents/:name/draft → defensive alias used by
      // frontend's batches/new unmount cleanup (and parity with
      // GET/PUT alias). Existing flat path doesn't have a DELETE
      // either; this is a small addition that closes the loop.
      if (method === 'DELETE' && id && sub === 'draft') {
        await pool.query('DELETE FROM voice_agent_drafts WHERE agent_id = $1', [id]);
        return respond(200, { deleted: true });
      }

      // GET /api/agents/:name/versions → forwards to agent-versions
      // GET logic, returning the array directly so the frontend's
      // {versions} unwrap or bare-array handling both work.
      if (method === 'GET' && id && sub === 'versions') {
        const r = await pool.query(
          'SELECT * FROM voice_agent_versions WHERE agent_id = $1 ORDER BY version_number DESC',
          [id],
        );
        // Frontend's `listAgentVersions` accepts `{versions: [...]}`
        // OR a bare array. Standardize on the wrapped form here so
        // future callers don't have to handle two shapes.
        return respond(200, { versions: r.rows });
      }

      // POST /api/agents/:name/versions → forwards to agent-versions
      // POST logic. Coerces agent_id from URL param so the body
      // doesn't need to repeat it; the frontend currently sends it
      // anyway, but either shape works.
      if (method === 'POST' && id && sub === 'versions') {
        const newId = uuid();
        const agentId = body.agent_id || id;
        const { version_number, version_name, description, config_snapshot, phone_assignments, published_by } = body;
        await pool.query(
          `INSERT INTO voice_agent_versions (id, agent_id, version_number, version_name, description, config_snapshot, phone_assignments, published_at, published_by)
           VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8)`,
          [newId, agentId, version_number, version_name || '', description || '', JSON.stringify(config_snapshot || {}), JSON.stringify(phone_assignments || []), published_by || null]
        );
        return respond(201, { id: newId, agent_id: agentId, version_number, version_name: version_name || '', description: description || '' });
      }
    }

    // ═══ Agent Drafts (replaces Supabase) ═══
    if (resource === 'agent-drafts') {
      // GET /api/agent-drafts/:agent_id
      if (method === 'GET' && id) {
        const r = await pool.query('SELECT * FROM voice_agent_drafts WHERE agent_id = $1', [id]);
        return respond(200, r.rows[0] || null);
      }
      // POST /api/agent-drafts — insert draft
      if (method === 'POST') {
        console.log('[draft-debug] incoming body:', JSON.stringify({
          agent_id: body.agent_id, name: body.name,
          max_tokens: body.max_tokens, temperature: body.temperature,
          tts_stability: body.tts_stability, tts_speed: body.tts_speed,
          tts_style: body.tts_style, tts_similarity_boost: body.tts_similarity_boost,
        }));
        // Drop null/undefined/empty values for numeric/non-string cols so we don't
        // accidentally overwrite good values with blanks from partial PUTs
        const NUMERIC_COLS = new Set([
          'temperature', 'max_tokens', 'tts_stability', 'tts_similarity_boost',
          'tts_style', 'tts_speed', 'recording_channels', 'idle_timeout_secs',
          'max_call_duration_secs', 'max_retries', 'retry_delay_secs',
          'default_concurrency',
        ]);
        for (const k of Object.keys(body)) {
          if (body[k] === null || body[k] === undefined) { delete body[k]; continue; }
          if (NUMERIC_COLS.has(k) && (body[k] === '' || Number.isNaN(Number(body[k])))) {
            delete body[k];
          }
        }

        // Validate the MERGED state (existing row + incoming fields), not
        // just the incoming patch. Because this handler is an UPSERT (ON
        // CONFLICT DO UPDATE), a partial payload like {tts_model: "X"}
        // would otherwise land on an existing row and leave it in an
        // impossible state (e.g. previously-set tts_provider="fish" with
        // an ElevenLabs-only model). The client-side fix writes
        // tts_provider atomically with voice / model, but the Lambda
        // should enforce merged-state consistency defensively for any
        // caller (scripts, CLI, future clients).
        const existing = body.agent_id
          ? (await pool.query('SELECT * FROM voice_agent_drafts WHERE agent_id = $1', [body.agent_id])).rows[0]
          : null;
        const vErrors = validateDraftMerged(existing, body);
        if (vErrors.length) return respond(400, { error: 'Validation failed', details: vErrors });
        const newId = uuid();
        body.id = newId;
        body.created_at = new Date().toISOString();
        body.updated_at = new Date().toISOString();
        for (const k of Object.keys(body)) {
          if (typeof body[k] === 'object' && body[k] !== null) body[k] = JSON.stringify(body[k]);
        }
        const cols = Object.keys(body).filter(k => /^[a-z_]+$/.test(k));
        const vals = cols.map(k => body[k]);
        const placeholders = cols.map((_, i) => `$${i + 1}`).join(', ');
        await pool.query(
          `INSERT INTO voice_agent_drafts (${cols.join(', ')}) VALUES (${placeholders})
           ON CONFLICT (agent_id) DO UPDATE SET ${cols.filter(c => c !== 'id' && c !== 'agent_id').map((c, i) => `${c} = EXCLUDED.${c}`).join(', ')}`,
          vals
        );
        return respond(201, { ...body, id: newId });
      }
      // PUT /api/agent-drafts/:agent_id — update draft
      if (method === 'PUT' && id) {
        // Load the existing row and validate the MERGED state, not just
        // the incoming patch. A single-field update (e.g.
        // {tts_model: "X"}) can otherwise leave the draft in an
        // impossible combination with a previously-set field (e.g.
        // existing tts_provider="fish" with an ElevenLabs-only model).
        // The client-side fix (writing tts_provider atomically with
        // voice / model) prevents the UI from producing these, but the
        // Lambda should enforce consistency defensively for any caller
        // (scripts, CLI, future clients).
        const existing = await pool.query(
          'SELECT * FROM voice_agent_drafts WHERE agent_id = $1', [id]
        );
        const vErrors = validateDraftMerged(existing.rows[0], body);
        if (vErrors.length) return respond(400, { error: 'Validation failed', details: vErrors });
        body.updated_at = new Date().toISOString();
        for (const k of Object.keys(body)) {
          if (typeof body[k] === 'object' && body[k] !== null) body[k] = JSON.stringify(body[k]);
        }
        const cols = Object.keys(body).filter(k => /^[a-z_]+$/.test(k) && k !== 'id' && k !== 'agent_id');
        const vals = cols.map(k => body[k]);
        vals.push(id);
        const sets = cols.map((c, i) => `${c} = $${i + 1}`).join(', ');
        await pool.query(`UPDATE voice_agent_drafts SET ${sets} WHERE agent_id = $${vals.length}`, vals);
        const r = await pool.query('SELECT * FROM voice_agent_drafts WHERE agent_id = $1', [id]);
        return respond(200, r.rows[0]);
      }
    }

    // ═══ Agent Versions (replaces Supabase) ═══
    if (resource === 'agent-versions') {
      // GET /api/agent-versions/:agent_id
      if (method === 'GET' && id) {
        const r = await pool.query('SELECT * FROM voice_agent_versions WHERE agent_id = $1 ORDER BY version_number DESC', [id]);
        return respond(200, r.rows);
      }
      // POST /api/agent-versions
      if (method === 'POST') {
        const newId = uuid();
        const { agent_id, version_number, version_name, description, config_snapshot, phone_assignments, published_by } = body;
        await pool.query(
          `INSERT INTO voice_agent_versions (id, agent_id, version_number, version_name, description, config_snapshot, phone_assignments, published_at, published_by)
           VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8)`,
          [newId, agent_id, version_number, version_name || '', description || '', JSON.stringify(config_snapshot || {}), JSON.stringify(phone_assignments || []), published_by || null]
        );
        return respond(201, { id: newId, ...body });
      }
    }

    // ═══ Calls ═══
    if (resource === 'calls') {
      // GET /api/calls/agents — distinct agent names
      if (id === 'agents' && method === 'GET') {
        const r = await pool.query('SELECT DISTINCT agent_name, agent_display_name FROM voice_calls WHERE agent_name IS NOT NULL ORDER BY agent_display_name');
        return respond(200, { agents: r.rows.map(row => ({ agent_name: row.agent_name, display_name: row.agent_display_name || row.agent_name })) });
      }
      // GET /api/calls/:id/recording-url
      //
      // Frontend-v2 alignment audit B10 (2026-05-19):
      // recording_path is stored as either an "s3://bucket/key" URI
      // or a bare S3 key. The frontend's <audio> player can't load
      // either of those; it needs a real HTTPS URL. presignS3Url
      // turns the stored value into a 15-minute presigned URL on
      // every call. Pre-existing HTTPS values pass through
      // unchanged for backwards compatibility with any legacy rows.
      if (id && sub === 'recording-url' && method === 'GET') {
        const r = await pool.query('SELECT recording_path FROM voice_calls WHERE id = $1', [id]);
        if (!r.rows.length || !r.rows[0].recording_path) return respond(200, { url: null });
        const url = await presignS3Url(r.rows[0].recording_path);
        return respond(200, { url: url || null });
      }
      // POST /api/calls — upsert call record (used by calling engine)
      //
      // Phase 7E PR 1 wired this from the ECS pipeline. Phase 7D
      // follow-up adds three things on top:
      //
      //   1. batch_row_index column in the INSERT — was dropped
      //      in PR #25 (small typo: column was in the table schema
      //      but missing from the INSERT column list, so every
      //      batch-call row had NULL batch_row_index. Frontend
      //      can't join to voice_batch_rows without it).
      //
      //   2. Mirror status into voice_batch_rows when batch_id +
      //      batch_row_index are present. The pipeline writes
      //      voice_calls; voice_batch_rows is the dispatcher's
      //      view — it needs the terminal status too so the row
      //      doesn't sit at 'dialing' forever.
      //
      //   3. Auto-finalize the parent voice_batches row when all
      //      child rows reach a terminal status. Ports OG's
      //      _check_batch_completion behavior so the UI dashboard
      //      doesn't show "running" forever.
      if (method === 'POST' && !id) {
        const c = body;
        const callId = c.id || uuid();
        await pool.query(
          `INSERT INTO voice_calls (id, agent_name, agent_display_name, from_number, target_number, direction, status, started_at, ended_at, duration_secs, case_data, transcript, recording_path, post_call_analyses, error, batch_id, batch_row_index, session_id, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
           ON CONFLICT (id) DO UPDATE SET
             status = COALESCE(EXCLUDED.status, voice_calls.status),
             ended_at = COALESCE(EXCLUDED.ended_at, voice_calls.ended_at),
             duration_secs = COALESCE(EXCLUDED.duration_secs, voice_calls.duration_secs),
             transcript = CASE WHEN EXCLUDED.transcript::text != '[]' THEN EXCLUDED.transcript ELSE voice_calls.transcript END,
             recording_path = COALESCE(EXCLUDED.recording_path, voice_calls.recording_path),
             post_call_analyses = CASE WHEN EXCLUDED.post_call_analyses::text != '{}' THEN EXCLUDED.post_call_analyses ELSE voice_calls.post_call_analyses END,
             error = COALESCE(EXCLUDED.error, voice_calls.error),
             batch_row_index = COALESCE(EXCLUDED.batch_row_index, voice_calls.batch_row_index),
             session_id = COALESCE(EXCLUDED.session_id, voice_calls.session_id),
             updated_at = EXCLUDED.updated_at`,
          [callId, c.agent_name, c.agent_display_name, c.from_number, c.target_number||'', c.direction||'outbound', c.status||'completed', c.started_at, c.ended_at, c.duration_secs, JSON.stringify(c.case_data||{}), JSON.stringify(c.transcript||[]), c.recording_path, JSON.stringify(c.post_call_analyses||{}), c.error||null, c.batch_id||null, c.batch_row_index ?? null, c.session_id || null, c.created_at||new Date().toISOString(), c.updated_at||new Date().toISOString()]
        );

        // Best-effort batch-row + batch finalization. None of these
        // updates are the call-record write's responsibility, but
        // they're cheap enough to do inline and avoid a second
        // round-trip from the pipeline. Wrapped in try/catch so a
        // failure here never breaks the primary upsert.
        if (c.batch_id && c.batch_row_index !== undefined && c.batch_row_index !== null) {
          try {
            // Map voice_calls.status → voice_batch_rows.status. Both
            // tables share most vocabulary (completed/failed/etc),
            // so this is largely a passthrough. Gate explicit
            // unknown values to a safe default.
            const callStatus = c.status || 'completed';
            const ROW_STATUS_MAP = {
              completed: 'completed',
              failed: 'failed',
              no_answer: 'no_answer',
              busy: 'busy',
              cancelled: 'cancelled',
              skipped: 'skipped',
            };
            const rowStatus = ROW_STATUS_MAP[callStatus] || 'completed';

            await pool.query(
              `UPDATE voice_batch_rows
                  SET status = $1,
                      call_id = $2,
                      error_message = COALESCE($3, error_message),
                      updated_at = NOW()
                WHERE batch_id = $4 AND row_index = $5`,
              [rowStatus, callId, c.error || null, c.batch_id, c.batch_row_index]
            );

            // Auto-finalize. Mirrors OG voiceagent's
            // _check_batch_completion: if no rows are still in a
            // non-terminal state, flip the batch to 'completed'.
            // The IS DISTINCT FROM check makes this UPDATE a no-op
            // when there's nothing to do, so it's safe to fire on
            // every after-call write.
            await pool.query(
              `UPDATE voice_batches
                  SET status = 'completed',
                      completed_rows = (SELECT COUNT(*) FROM voice_batch_rows WHERE batch_id = $1 AND status = 'completed'),
                      failed_rows    = (SELECT COUNT(*) FROM voice_batch_rows WHERE batch_id = $1 AND status IN ('failed','no_answer','busy')),
                      updated_at = NOW()
                WHERE id = $1
                  AND status IS DISTINCT FROM 'completed'
                  AND status IS DISTINCT FROM 'cancelled'
                  AND NOT EXISTS (
                    SELECT 1 FROM voice_batch_rows
                     WHERE batch_id = $1
                       AND status NOT IN ('completed','failed','no_answer','busy','cancelled','skipped')
                  )`,
              [c.batch_id]
            );
          } catch (e) {
            console.error('[voice-api] post-call batch update failed', { batch_id: c.batch_id, batch_row_index: c.batch_row_index, error: e.message });
          }
        }

        return respond(201, { id: callId });
      }
      // PUT /api/calls/:id/hide — soft-delete a call
      if (method === 'PUT' && id && sub === 'hide') {
        const r = await pool.query('UPDATE voice_calls SET hidden = true WHERE id = $1 RETURNING id', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Call not found' });
        return respond(200, { status: 'hidden', id: r.rows[0].id });
      }

      // DELETE /api/calls/:id — convention-friendly alias for the
      // PUT :id/hide soft-delete. The frontend uses DELETE which
      // matches REST norms; the old PUT shape is preserved for any
      // legacy callers. Both produce the same DB state.
      // Frontend-v2 alignment audit B5 (2026-05-19).
      if (method === 'DELETE' && id && !sub) {
        const r = await pool.query('UPDATE voice_calls SET hidden = true WHERE id = $1 RETURNING id', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Call not found' });
        return respond(200, { status: 'hidden', id: r.rows[0].id, deleted: true });
      }
      // POST /api/calls/recording-update
      //
      // Phase 7E PR 3: Daily's recording.ready-to-download webhook
      // arrives with `room_name` (= our session_id) and `s3_key`. The
      // bot-runner's webhook handler calls this endpoint to patch
      // voice_calls.recording_path. We look up by session_id, not by
      // primary key — the webhook doesn't know voice_calls.id.
      //
      // Idempotent: if the row's recording_path is already set,
      // overwrite (Daily can re-emit the event after a retry; the
      // s3_key is stable so the value doesn't change). If no row
      // matches the session_id, return 404 — that's a programming
      // bug (Daily room name out of sync with our session_id), not
      // a transient that should be retried.
      if (method === 'POST' && id === 'recording-update') {
        const sessionId = body.session_id || body.room_name;
        const recordingPath = body.recording_path;
        if (!sessionId) return respond(400, { detail: 'session_id (or room_name) required' });
        if (!recordingPath) return respond(400, { detail: 'recording_path required' });
        const r = await pool.query(
          `UPDATE voice_calls
              SET recording_path = $1,
                  updated_at = NOW()
            WHERE session_id = $2
            RETURNING id, session_id, recording_path`,
          [recordingPath, sessionId]
        );
        if (!r.rows.length) {
          return respond(404, {
            detail: 'No voice_calls row matches that session_id',
            session_id: sessionId,
          });
        }
        return respond(200, r.rows[0]);
      }
      // GET /api/calls/:id
      if (method === 'GET' && id && !sub) {
        const r = await pool.query('SELECT * FROM voice_calls WHERE id = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Call not found' });
        return respond(200, r.rows[0]);
      }
      // GET /api/calls — paginated list
      //
      // Filter params:
      //   agent_name           — match voice_calls.agent_name exactly (slug)
      //   agent_display_name   — match voice_calls.agent_display_name exactly.
      //                          Frontend-v2 alignment audit B9
      //                          (2026-05-19): voiceagentfront sends
      //                          this query key from its agent filter
      //                          dropdown; the previous handler only
      //                          accepted agent_name and silently
      //                          dropped the filter. Now both work.
      //   status / direction   — exact-match filters
      //   sort_by / sort_order — allowlist-enforced sort
      if (method === 'GET' && !id) {
        const page = parseInt(qs.page) || 1;
        const pageSize = Math.min(parseInt(qs.page_size) || 25, 100);
        const offset = (page - 1) * pageSize;
        let where = 'WHERE (hidden IS NULL OR hidden = false)';
        const params = [];
        let idx = 1;
        if (qs.agent_name) { where += ` AND agent_name = $${idx++}`; params.push(qs.agent_name); }
        if (qs.agent_display_name) { where += ` AND agent_display_name = $${idx++}`; params.push(qs.agent_display_name); }
        if (qs.status) { where += ` AND status = $${idx++}`; params.push(qs.status); }
        if (qs.direction) { where += ` AND direction = $${idx++}`; params.push(qs.direction); }
        const sortBy = ['created_at', 'duration_secs', 'status'].includes(qs.sort_by) ? qs.sort_by : 'created_at';
        const sortOrder = qs.sort_order === 'asc' ? 'ASC' : 'DESC';
        const countR = await pool.query(`SELECT COUNT(*) FROM voice_calls ${where}`, params);
        params.push(pageSize, offset);
        const r = await pool.query(`SELECT * FROM voice_calls ${where} ORDER BY ${sortBy} ${sortOrder} LIMIT $${idx++} OFFSET $${idx}`, params);
        return respond(200, { calls: r.rows, total: parseInt(countR.rows[0].count), page, page_size: pageSize });
      }
    }

    // ═══ Batches ═══
    if (resource === 'batches') {
      // GET /api/batches
      if (method === 'GET' && !id) {
        const r = await pool.query('SELECT * FROM voice_batches ORDER BY created_at DESC LIMIT 100');
        return respond(200, { batches: r.rows });
      }
      // GET /api/batches/:id — returns batch + calls + derived progress
      if (method === 'GET' && id && !sub) {
        const b = await pool.query('SELECT * FROM voice_batches WHERE id = $1', [id]);
        if (!b.rows.length) return respond(404, { detail: 'Batch not found' });
        const calls = await pool.query('SELECT * FROM voice_calls WHERE batch_id = $1 ORDER BY batch_row_index', [id]);
        const progress = await pool.query(`
          SELECT
            COUNT(*)::int as total,
            COUNT(*) FILTER (WHERE status = 'completed')::int as completed,
            COUNT(*) FILTER (WHERE status IN ('failed','no_answer','busy'))::int as failed,
            COUNT(*) FILTER (WHERE status = 'pending')::int as pending,
            COUNT(*) FILTER (WHERE status = 'dialing')::int as dialing,
            COUNT(*) FILTER (WHERE status = 'queued')::int as queued,
            COUNT(*) FILTER (WHERE status = 'skipped')::int as skipped,
            COUNT(*) FILTER (WHERE status = 'cancelled')::int as cancelled
          FROM voice_batch_rows WHERE batch_id = $1
        `, [id]);
        return respond(200, { batch: b.rows[0], calls: calls.rows, progress: progress.rows[0] });
      }
      // POST /api/batches — create batch record (metadata only; rows go to voice_batch_rows)
      if (method === 'POST' && !id) {
        const batchId = body.id || uuid();
        await pool.query(
          `INSERT INTO voice_batches (id, name, agent_name, agent_display_name, from_number, status, input_file_path, total_rows)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           ON CONFLICT (id) DO UPDATE SET total_rows = $8, updated_at = NOW()`,
          [batchId, body.name || '', body.agent_name || '', body.agent_display_name || '', body.from_number || '', body.status || 'draft', body.input_file_path || null, body.total_rows || 0]
        );
        const r = await pool.query('SELECT * FROM voice_batches WHERE id = $1', [batchId]);
        return respond(201, r.rows[0] || { id: batchId });
      }
      // POST /api/batches/upload
      //
      // Frontend-v2 alignment audit B7 (2026-05-19): the frontend
      // dropzone sends a multipart/form-data POST with `file`,
      // `agent_name`, and `from_number` fields. Previously this
      // handler expected `application/json` and silently created
      // an empty draft from {}. Now:
      //   - multipart/form-data → parse the file (CSV or .xlsx),
      //     build a normalized row list, persist a draft batch
      //   - application/json (legacy)  → accept {rows: [...]} as
      //     before so existing scripts keep working
      //
      // Either content type produces the same response shape so
      // the frontend's UploadResponse handling is unchanged:
      //   { batch_id, columns, summary, rows }
      //
      // The handler validates phone numbers using normalizeE164 to
      // produce `phone_e164` + a `validation` flag on each row.
      // The frontend's editable grid surfaces these as
      // valid/fixable/invalid.
      if (method === 'POST' && id === 'upload') {
        const contentType =
          (event.headers?.['content-type']) ||
          (event.headers?.['Content-Type']) ||
          '';
        const isMultipart = contentType.toLowerCase().startsWith('multipart/form-data');

        let agent_name;
        let from_number;
        let batchName;
        let columns = [];
        let rows = [];

        if (isMultipart) {
          let parsed;
          try {
            parsed = await parseMultipart(event);
          } catch (e) {
            return respond(400, { detail: `multipart parse failed: ${e.message}` });
          }
          agent_name = parsed.fields.agent_name || parsed.fields.agentName;
          from_number = parsed.fields.from_number || parsed.fields.fromNumber || '';
          batchName = parsed.fields.name || parsed.fields.batch_name;
          const file = parsed.files[0];
          if (!file) {
            return respond(400, { detail: 'file field is required' });
          }
          try {
            const sheet = await parseSpreadsheet(file);
            columns = sheet.columns;
            rows = sheet.rows;
          } catch (e) {
            return respond(400, { detail: `file parse failed: ${e.message}` });
          }
        } else {
          agent_name = body.agent_name;
          from_number = body.from_number || '';
          batchName = body.name;
          rows = Array.isArray(body.rows) ? body.rows : [];
          columns = Object.keys(rows[0] || {});
        }

        if (!agent_name) return respond(400, { detail: 'agent_name is required' });

        // Validate + classify rows. The frontend's UploadResponse
        // expects per-row { row_index, phone, phone_e164, validation }
        // plus original case_data fields so the editable grid can
        // show the parsed cells.
        //
        // Classification rules:
        //   valid    — raw input already starts with `+` and is a
        //              well-formed E.164 number
        //   fixable  — raw input is 10 digits (or 11 starting with
        //              1) without a `+`; we add +1 (or +) and the
        //              result is valid E.164. UI should let the
        //              user confirm/edit before sending.
        //   invalid  — empty, garbled, or no fix yields a valid
        //              E.164 number.
        const PHONE_COLUMNS = ['phone', 'phone_number', 'phoneNumber', 'Phone', 'Phone Number', 'tel'];
        const normalizedRows = rows.map((raw, i) => {
          const rawPhone = (
            raw.phone_e164 ||
            raw.phone ||
            PHONE_COLUMNS.map(c => raw[c]).find(v => v) ||
            ''
          ).toString().trim();

          let validation = 'invalid';
          let phoneE164 = rawPhone;

          if (!rawPhone) {
            validation = 'invalid';
            phoneE164 = '';
          } else if (rawPhone.startsWith('+')) {
            const normalized = normalizeE164(rawPhone);
            if (normalized) {
              validation = 'valid';
              phoneE164 = normalized;
            } else {
              validation = 'invalid';
            }
          } else {
            // No `+` prefix. Try to coerce to US E.164 by stripping
            // non-digits and prepending +1 (or +) so the user can
            // confirm. Mark as fixable so the editable grid surfaces
            // the row for review.
            const digits = rawPhone.replace(/\D/g, '');
            let candidate = null;
            if (digits.length === 10) candidate = `+1${digits}`;
            else if (digits.length === 11 && digits.startsWith('1')) candidate = `+${digits}`;
            else if (digits.length >= 7 && digits.length <= 15) candidate = `+${digits}`;
            const normalized = candidate ? normalizeE164(candidate) : null;
            if (normalized) {
              validation = 'fixable';
              phoneE164 = normalized;
            } else {
              validation = 'invalid';
            }
          }

          return {
            row_index: i,
            phone: rawPhone,
            phone_e164: phoneE164,
            validation,
            data: raw,
          };
        });

        const summary = {
          total: normalizedRows.length,
          valid: normalizedRows.filter(r => r.validation === 'valid').length,
          fixable: normalizedRows.filter(r => r.validation === 'fixable').length,
          invalid: normalizedRows.filter(r => r.validation === 'invalid').length,
        };

        const batchId = uuid();
        const today = new Date().toISOString().slice(0, 10);
        await pool.query(
          `INSERT INTO voice_batches (id, name, agent_name, from_number, status, total_rows, rows)
           VALUES ($1, $2, $3, $4, 'draft', $5, $6)`,
          [
            batchId,
            batchName || `Batch ${today}`,
            agent_name,
            from_number,
            normalizedRows.length,
            JSON.stringify(normalizedRows),
          ],
        );

        return respond(201, {
          batch_id: batchId,
          columns: columns.length ? columns : Object.keys(rows[0] || {}),
          summary,
          rows: normalizedRows,
        });
      }
      // PUT /api/batches/:id — update batch fields (status, counters, etc.)
      if (method === 'PUT' && id && !sub) {
        const updates = []; const vals = []; let idx = 1;
        const allowed = ['status', 'total_rows', 'completed_rows', 'failed_rows', 'output_file_path', 'concurrency', 'timezone', 'calling_window_start', 'calling_window_end'];
        for (const col of allowed) {
          if (body[col] !== undefined) { updates.push(`${col} = $${idx++}`); vals.push(body[col]); }
        }
        if (body.calling_window_days !== undefined) {
          updates.push(`calling_window_days = $${idx++}`);
          vals.push(Array.isArray(body.calling_window_days) ? JSON.stringify(body.calling_window_days) : body.calling_window_days);
        }
        if (updates.length) {
          updates.push(`updated_at = NOW()`);
          vals.push(id);
          await pool.query(`UPDATE voice_batches SET ${updates.join(', ')} WHERE id = $${idx}`, vals);
        }
        const r = await pool.query('SELECT * FROM voice_batches WHERE id = $1', [id]);
        return respond(200, r.rows[0] || { id });
      }
      // PUT /api/batches/:id/rows — replace all rows in voice_batch_rows for this batch
      if (method === 'PUT' && id && sub === 'rows') {
        const rawRows = body.rows || [];
        const normalized = rawRows
          .filter(r => !r.excluded)
          .map((r, i) => ({
            row_index: r.row_index ?? r.index ?? i,
            phone_e164: r.phone_e164 || r.phone || '',
            validation: r.validation || (r.phone_e164 || r.phone ? 'valid' : 'invalid'),
            case_data: r.case_data || r.data || {},
          }));
        const valid = normalized.filter(r => r.validation === 'valid');

        // Replace rows for this batch — clean slate
        await pool.query('DELETE FROM voice_batch_rows WHERE batch_id = $1', [id]);

        if (valid.length) {
          const placeholders = [];
          const vals = [];
          let i = 1;
          for (const r of valid) {
            placeholders.push(`($${i}, $${i+1}, $${i+2}, $${i+3})`);
            vals.push(id, r.row_index, r.phone_e164, JSON.stringify(r.case_data || {}));
            i += 4;
          }
          await pool.query(
            `INSERT INTO voice_batch_rows (batch_id, row_index, phone_e164, case_data) VALUES ${placeholders.join(', ')}`,
            vals
          );
        }

        await pool.query(
          `UPDATE voice_batches SET column_mapping = $1, total_rows = $2, status = 'ready', updated_at = NOW() WHERE id = $3`,
          [JSON.stringify(body.mapping || {}), valid.length, id]
        );
        return respond(200, { ok: true, total_rows: valid.length, ready: valid.length });
      }
      // POST /api/batches/:id/start
      //
      // Phase 7D: this used to be a TODO. Now wired to the JS port
      // of OG voiceagent's server/batch_queue.py:start_batch — apply
      // any caller-supplied schedule overrides, then fan out
      // pending rows to SQS via lib/batch-enqueuer.mjs. The
      // dispatcher Lambda picks them up from the queue.
      if (method === 'POST' && id && sub === 'start') {
        // Apply schedule + window overrides FIRST so the row-builder
        // (which reads from voice_batches) sees the latest values.
        await pool.query(
          `UPDATE voice_batches SET status = $1, concurrency = $2, timezone = $3, calling_window_start = $4, calling_window_end = $5, calling_window_days = $6, updated_at = NOW() WHERE id = $7`,
          [body.schedule_mode === 'scheduled' ? 'scheduled' : 'running', body.concurrency || 1, body.timezone || null, body.calling_window_start || null, body.calling_window_end || null, body.calling_window_days ? JSON.stringify(body.calling_window_days) : null, id]
        );

        // Scheduled batches don't fire enqueue immediately — the
        // batch sits at status='scheduled' and a future cron / API
        // call should re-POST start without schedule_mode to fan
        // out. Match OG behavior.
        if (body.schedule_mode === 'scheduled') {
          return respond(200, { status: 'scheduled', batch_id: id });
        }

        const enqueuer = await import('./lib/batch-enqueuer.mjs');
        const result = await enqueuer.start(pool, sqsClient, id, CALL_QUEUE_URL);
        const httpStatus = result.http_status || 200;
        // strip the http_status field from the public response — it
        // was only the channel for the validator's intent.
        const { http_status, ...payload } = result;
        return respond(httpStatus, payload);
      }
      // POST /api/batches/:id/pause|resume|cancel
      if (method === 'POST' && id && ['pause', 'resume', 'cancel'].includes(sub)) {
        const statusMap = { pause: 'paused', resume: 'running', cancel: 'canceled' };
        await pool.query('UPDATE voice_batches SET status = $1, updated_at = NOW() WHERE id = $2', [statusMap[sub], id]);
        return respond(200, { status: statusMap[sub] });
      }
      // GET /api/batches/:id/status
      if (method === 'GET' && id && sub === 'status') {
        const r = await pool.query('SELECT id, status, total_rows, completed_rows, failed_rows FROM voice_batches WHERE id = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Batch not found' });
        return respond(200, { batch_id: id, ...r.rows[0] });
      }
      // GET /api/batches/:id/download-url
      //
      // Frontend-v2 alignment audit B6 (2026-05-19): turns the
      // stored output_file_path (typically "s3://bucket/key.xlsx")
      // into a 15-minute presigned URL before returning. Frontend's
      // existing `window.open(url)` flow now actually downloads the
      // file. Returns { url: null } when no results exist yet.
      if (method === 'GET' && id && sub === 'download-url') {
        const r = await pool.query('SELECT output_file_path FROM voice_batches WHERE id = $1', [id]);
        const rawPath = r.rows[0]?.output_file_path;
        if (!rawPath) return respond(200, { url: null });
        const url = await presignS3Url(rawPath);
        return respond(200, { url: url || null });
      }

      // GET /api/batches/:id/results
      //
      // Frontend-v2 alignment audit B6 (2026-05-19): companion route
      // to /download-url. Returns a 302 redirect to the presigned
      // URL so a plain `<a href=".../results" download>` link works
      // without JS — the frontend uses fetch + blob-download today
      // but the redirect shape is the more browser-native pattern
      // and is what the audit recommended.
      // Returns 404 when no output is available yet.
      if (method === 'GET' && id && sub === 'results') {
        const r = await pool.query('SELECT output_file_path, status FROM voice_batches WHERE id = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Batch not found' });
        const rawPath = r.rows[0].output_file_path;
        if (!rawPath) {
          return respond(404, { detail: 'Batch results not yet available', status: r.rows[0].status });
        }
        const url = await presignS3Url(rawPath);
        if (!url) {
          return respond(500, { detail: 'Failed to generate download URL' });
        }
        return redirect(url);
      }
      // DELETE /api/batches/:id/draft
      if (method === 'DELETE' && id && sub === 'draft') {
        await pool.query('DELETE FROM voice_batches WHERE id = $1 AND status = $2', [id, 'draft']);
        return respond(200, { deleted: true });
      }
    }

    // ═══ Phone Numbers ═══
    if (resource === 'phone-numbers') {
      // GET /api/phone-numbers — list all. Returns provider + daily_number_id
      // via `pn.*`; the frontend renders a provider badge on each row.
      if (method === 'GET' && !id) {
        const r = await pool.query(`
          SELECT pn.*, 
            json_build_object('id', ia.id, 'name', ia.name, 'display_name', ia.display_name) AS inbound_agent,
            json_build_object('id', oa.id, 'name', oa.name, 'display_name', oa.display_name) AS outbound_agent
          FROM voice_phone_numbers pn
          LEFT JOIN voice_agents ia ON pn.inbound_agent_id = ia.id
          LEFT JOIN voice_agents oa ON pn.outbound_agent_id = oa.id
          ORDER BY pn.friendly_name
        `);
        return respond(200, { phone_numbers: r.rows });
      }
      // GET /api/phone-numbers/lookup?number=+12098075018
      //
      // Phase 7B: bot-runner Lambda hits this on every inbound Daily
      // webhook to resolve the dialed number → inbound_agent_id. Kept
      // intentionally narrow (only the fields the bot-runner actually
      // needs) so caching + observability stay cheap and we're not
      // leaking full DB rows to every PSTN call. Returns 404 when the
      // number isn't in voice_phone_numbers at all — callers are
      // expected to fall back to a DEFAULT_INBOUND_AGENT env var.
      if (method === 'GET' && id === 'lookup') {
        const raw = qs.number || '';
        const number = normalizeE164(raw);
        if (!number) {
          return respond(400, {
            error: `Invalid E.164 number: ${raw}. Expected +<country><subscriber>.`,
          });
        }
        const r = await pool.query(
          `SELECT pn.id, pn.number, pn.friendly_name, pn.provider,
                  pn.is_active,
                  json_build_object(
                    'id', ia.id, 'name', ia.name, 'display_name', ia.display_name
                  ) AS inbound_agent,
                  json_build_object(
                    'id', oa.id, 'name', oa.name, 'display_name', oa.display_name
                  ) AS outbound_agent
           FROM voice_phone_numbers pn
           LEFT JOIN voice_agents ia ON pn.inbound_agent_id = ia.id
           LEFT JOIN voice_agents oa ON pn.outbound_agent_id = oa.id
           WHERE pn.number = $1
           LIMIT 1`,
          [number]
        );
        if (!r.rows.length) {
          return respond(404, { detail: 'Phone number not provisioned' });
        }
        const row = r.rows[0];
        // Null out the agent objects when id is null so callers can
        // just check `inbound_agent === null` rather than inspecting
        // a row with all-null fields.
        if (row.inbound_agent && row.inbound_agent.id === null) row.inbound_agent = null;
        if (row.outbound_agent && row.outbound_agent.id === null) row.outbound_agent = null;
        return respond(200, row);
      }
      // POST /api/phone-numbers (upsert by number — used for manual adds).
      // Accepts provider + daily_number_id when a number was bought via the
      // /buy route; /buy itself also inserts via this code path so we get
      // the same conflict handling. Defaults to provider='twilio' for
      // hand-added rows that don't specify, matching DB default.
      if (method === 'POST' && !id) {
        const pErrors = validatePhoneData(body);
        if (pErrors.length) return respond(400, { error: 'Validation failed', details: pErrors });
        const newId = uuid();
        const provider = body.provider || 'twilio';
        if (!['twilio', 'daily'].includes(provider)) {
          return respond(400, { error: `Invalid provider: ${provider}. Valid: twilio, daily` });
        }
        const r = await pool.query(
          `INSERT INTO voice_phone_numbers (id, number, friendly_name, provider, daily_number_id)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (number) DO UPDATE SET
             friendly_name = COALESCE(NULLIF($3, ''), voice_phone_numbers.friendly_name),
             provider = EXCLUDED.provider,
             daily_number_id = EXCLUDED.daily_number_id,
             updated_at = NOW()
           RETURNING *`,
          [newId, body.number, body.friendly_name || '', provider, body.daily_number_id || null]
        );
        return respond(201, r.rows[0]);
      }
      // PUT /api/phone-numbers/:id — assignment + name updates only. Provider
      // and daily_number_id are set at purchase time and aren't mutable.
      if (method === 'PUT' && id && !sub) {
        const pErrors = validatePhoneData(body);
        if (pErrors.length) return respond(400, { error: 'Validation failed', details: pErrors });
        const updates = []; const vals = []; let idx = 1;
        for (const col of ['friendly_name', 'inbound_agent_id', 'outbound_agent_id', 'is_active']) {
          if (body[col] !== undefined) { updates.push(`${col} = $${idx++}`); vals.push(body[col]); }
        }
        if (updates.length) { vals.push(id); await pool.query(`UPDATE voice_phone_numbers SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $${idx}`, vals); }
        const r = await pool.query('SELECT * FROM voice_phone_numbers WHERE id = $1', [id]);
        return respond(200, r.rows[0]);
      }
      // POST /api/phone-numbers/sync-twilio — still a stub; Twilio sync is
      // out of scope for the Daily migration. Kept so the existing
      // frontend button doesn't 404.
      if (method === 'POST' && id === 'sync-twilio') {
        return respond(200, { total: 0, message: 'Twilio sync not yet configured' });
      }
      // GET /api/phone-numbers/search?provider=daily&areaCode=209&limit=20
      //                       ?provider=twilio&country=US&area_code=209  (stub)
      // Provider-aware search. `provider` defaults to 'daily' going forward;
      // callers that still pass Twilio-style params fall through to the
      // Twilio stub (which returns an empty list until implemented).
      if (method === 'GET' && id === 'search') {
        const provider = (qs.provider || 'daily').toLowerCase();
        if (provider === 'daily') {
          const areaCode = qs.areaCode || qs.area_code;
          const region = qs.region;
          const limit = qs.limit ? Math.min(parseInt(qs.limit, 10) || 30, 100) : 30;
          if (!areaCode && !region) {
            return respond(400, { error: 'areaCode (or region) is required for Daily search' });
          }
          try {
            const numbers = await dailyPhoneClient.searchAvailable({ areaCode, region, limit });
            // Shape matches what the frontend normalizer handles: it accepts
            // data.available_numbers | data.numbers | data.results | data.
            // Using `numbers` and including `provider` so the UI can render
            // the right badge on results.
            return respond(200, { provider: 'daily', numbers });
          } catch (err) {
            const status = err?.statusCode || 500;
            return respond(status, { error: err.message || 'Daily search failed' });
          }
        }
        if (provider === 'twilio') {
          // Twilio search is still a stub. Return empty list with a hint so
          // the frontend can show "no results" without a hard error.
          return respond(200, {
            provider: 'twilio',
            numbers: [],
            note: 'Twilio search not implemented. Use provider=daily.',
          });
        }
        return respond(400, { error: `Invalid provider: ${provider}. Valid: twilio, daily` });
      }
      // POST /api/phone-numbers/buy
      //   body: { provider, number, area_code?, friendly_name? }
      // Also accepts /purchase as a backcompat alias (frontend cuts over to
      // /buy in the same PR, but we keep /purchase for one deploy cycle so
      // a rollback doesn't break the frontend).
      if (method === 'POST' && (id === 'buy' || id === 'purchase')) {
        const provider = (body.provider || 'daily').toLowerCase();
        if (!['twilio', 'daily'].includes(provider)) {
          return respond(400, { error: `Invalid provider: ${provider}. Valid: twilio, daily` });
        }
        if (!body.number) {
          return respond(400, { error: 'number is required' });
        }
        if (provider === 'twilio') {
          return respond(501, { error: 'Twilio purchase not implemented. Use provider=daily.' });
        }
        // Daily: buy the number, then upsert into voice_phone_numbers so
        // future releases can find the daily_number_id.
        let purchased;
        try {
          purchased = await dailyPhoneClient.buy({ number: body.number });
        } catch (err) {
          const status = err?.statusCode || 500;
          return respond(status, { error: err.message || 'Daily purchase failed' });
        }
        const newId = uuid();
        const r = await pool.query(
          `INSERT INTO voice_phone_numbers (id, number, friendly_name, provider, daily_number_id)
           VALUES ($1, $2, $3, 'daily', $4)
           ON CONFLICT (number) DO UPDATE SET
             friendly_name = COALESCE(NULLIF($3, ''), voice_phone_numbers.friendly_name),
             provider = 'daily',
             daily_number_id = $4,
             updated_at = NOW()
           RETURNING *`,
          [newId, purchased.number, body.friendly_name || '', purchased.id]
        );
        return respond(201, r.rows[0]);
      }
      // DELETE /api/phone-numbers/:id — provider-aware release.
      // For Daily numbers, call the Daily API to release the external
      // resource first; Daily 404 is treated as "already released" and
      // allows the DB row deletion to proceed (idempotent).
      // Twilio numbers have no external API call today (sync not impl),
      // so we just drop the row.
      if (method === 'DELETE' && id && !sub) {
        const r = await pool.query('SELECT * FROM voice_phone_numbers WHERE id = $1', [id]);
        if (!r.rows.length) return respond(404, { error: 'Phone number not found' });
        const row = r.rows[0];
        if (row.provider === 'daily' && row.daily_number_id) {
          try {
            await dailyPhoneClient.release({ dailyNumberId: row.daily_number_id });
          } catch (err) {
            const status = err?.statusCode || 500;
            return respond(status, { error: err.message || 'Daily release failed' });
          }
        }
        await pool.query('DELETE FROM voice_phone_numbers WHERE id = $1', [id]);
        return respond(200, { released: true, provider: row.provider });
      }
      // POST /api/phone-numbers/:id/release — backcompat alias. Inlines the
      // DELETE path above so a rollback of the frontend-PR-then-Lambda-PR
      // order still works. Mark as deprecated in logs so we notice if
      // anything still hits it after both PRs merge.
      if (method === 'POST' && id && sub === 'release') {
        console.warn('[deprecated] POST /api/phone-numbers/:id/release — use DELETE /api/phone-numbers/:id');
        const r = await pool.query('SELECT * FROM voice_phone_numbers WHERE id = $1', [id]);
        if (!r.rows.length) return respond(404, { error: 'Phone number not found' });
        const row = r.rows[0];
        if (row.provider === 'daily' && row.daily_number_id) {
          try {
            await dailyPhoneClient.release({ dailyNumberId: row.daily_number_id });
          } catch (err) {
            const status = err?.statusCode || 500;
            return respond(status, { error: err.message || 'Daily release failed' });
          }
        }
        await pool.query('DELETE FROM voice_phone_numbers WHERE id = $1', [id]);
        return respond(200, { released: true, provider: row.provider });
      }
    }

    // ═══ Voices ═══
    if (resource === 'voices') {
      // GET /api/voices
      if (method === 'GET' && !id) {
        const r = await pool.query('SELECT * FROM voice_voices ORDER BY name');
        return respond(200, { voices: r.rows });
      }
      // POST /api/voices/sync
      //
      // Frontend-v2 alignment audit B11 (2026-05-19): bulk-refresh
      // the library from ElevenLabs. Walks ElevenLabs's /v1/voices
      // response and upserts each entry into voice_voices using the
      // same column set as /voices/add. The `custom_name` and
      // `labels` columns on existing rows are preserved (the upsert
      // only overwrites name / description / preview_url / gender /
      // accent / age / category).
      //
      // Degrades to a stub-style response when ELEVENLABS_API_KEY
      // is unset so the frontend's "Sync" button doesn't error
      // pre-launch.
      if (method === 'POST' && id === 'sync') {
        if (!elevenlabs.isConfigured()) {
          return respond(200, {
            count: 0,
            message: 'ElevenLabs sync not yet configured (ELEVENLABS_API_KEY missing)',
          });
        }
        const r = await elevenlabs.listVoices();
        if (!r.ok) {
          return respond(502, {
            detail: `ElevenLabs API error: ${r.error}`,
          });
        }
        let count = 0;
        for (const elVoice of r.voices) {
          const row = elevenlabs.elToVoiceRow(elVoice);
          if (!row || !row.voice_id) continue;
          await pool.query(
            `INSERT INTO voice_voices (voice_id, name, description, preview_url, gender, accent, age, category, labels)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (voice_id) DO UPDATE SET
               name = COALESCE(EXCLUDED.name, voice_voices.name),
               description = COALESCE(EXCLUDED.description, voice_voices.description),
               preview_url = COALESCE(EXCLUDED.preview_url, voice_voices.preview_url),
               gender = COALESCE(EXCLUDED.gender, voice_voices.gender),
               accent = COALESCE(EXCLUDED.accent, voice_voices.accent),
               age = COALESCE(EXCLUDED.age, voice_voices.age),
               category = COALESCE(EXCLUDED.category, voice_voices.category),
               labels = COALESCE(EXCLUDED.labels, voice_voices.labels)`,
            [
              row.voice_id,
              row.name,
              row.description,
              row.preview_url,
              row.gender,
              row.accent,
              row.age,
              row.category,
              row.labels ? JSON.stringify(row.labels) : null,
            ],
          );
          count++;
        }
        return respond(200, { count, message: `Synced ${count} voices from ElevenLabs` });
      }
      // POST /api/voices/lookup
      if (method === 'POST' && id === 'lookup') {
        const r = await pool.query('SELECT * FROM voice_voices WHERE voice_id = $1', [body.voice_id]);
        if (r.rows.length) return respond(200, r.rows[0]);
        // TODO: Look up from ElevenLabs API
        return respond(404, { detail: 'Voice not found in local library' });
      }
      // POST /api/voices/add — upsert full voice metadata
      if (method === 'POST' && id === 'add') {
        if (!body.voice_id) return respond(400, { detail: 'voice_id is required' });
        await pool.query(
          `INSERT INTO voice_voices (voice_id, name, custom_name, description, preview_url, gender, accent, age, category, labels)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
           ON CONFLICT (voice_id) DO UPDATE SET
             name = COALESCE(EXCLUDED.name, voice_voices.name),
             custom_name = COALESCE(EXCLUDED.custom_name, voice_voices.custom_name),
             description = COALESCE(EXCLUDED.description, voice_voices.description),
             preview_url = COALESCE(EXCLUDED.preview_url, voice_voices.preview_url),
             gender = COALESCE(EXCLUDED.gender, voice_voices.gender),
             accent = COALESCE(EXCLUDED.accent, voice_voices.accent),
             age = COALESCE(EXCLUDED.age, voice_voices.age),
             category = COALESCE(EXCLUDED.category, voice_voices.category),
             labels = COALESCE(EXCLUDED.labels, voice_voices.labels)`,
          [
            body.voice_id,
            body.name || body.voice_id,
            body.custom_name || null,
            body.description || null,
            body.preview_url || null,
            body.gender || null,
            body.accent || null,
            body.age || null,
            body.category || null,
            body.labels ? JSON.stringify(body.labels) : null,
          ]
        );
        const r = await pool.query('SELECT * FROM voice_voices WHERE voice_id = $1', [body.voice_id]);
        return respond(200, r.rows[0]);
      }
      // POST /api/voices/:id/refresh
      //
      // Frontend-v2 alignment audit B11 (2026-05-19): hit ElevenLabs
      // for the single voice's latest metadata and upsert into
      // voice_voices. When ELEVENLABS_API_KEY is unset, falls back
      // to the previous behavior of returning the cached DB row
      // (no error) so the frontend's "Refresh" menu item still
      // works pre-launch.
      if (method === 'POST' && id && sub === 'refresh') {
        if (!elevenlabs.isConfigured()) {
          const r = await pool.query('SELECT * FROM voice_voices WHERE voice_id = $1', [id]);
          return respond(200, r.rows[0] || {});
        }
        const r = await elevenlabs.getVoice(id);
        if (!r.ok) {
          // 404 / 401 / network errors all roll up here. Return the
          // cached row + a non-fatal message so the UI can show a
          // toast without blocking other refresh attempts.
          const cached = await pool.query('SELECT * FROM voice_voices WHERE voice_id = $1', [id]);
          return respond(r.status === 404 ? 404 : 200, {
            voice: cached.rows[0] || null,
            warning: `ElevenLabs refresh failed: ${r.error}`,
          });
        }
        const row = elevenlabs.elToVoiceRow(r.voice);
        if (!row?.voice_id) {
          return respond(502, { detail: 'ElevenLabs returned an unrecognized voice payload' });
        }
        await pool.query(
          `INSERT INTO voice_voices (voice_id, name, description, preview_url, gender, accent, age, category, labels)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (voice_id) DO UPDATE SET
             name = COALESCE(EXCLUDED.name, voice_voices.name),
             description = COALESCE(EXCLUDED.description, voice_voices.description),
             preview_url = COALESCE(EXCLUDED.preview_url, voice_voices.preview_url),
             gender = COALESCE(EXCLUDED.gender, voice_voices.gender),
             accent = COALESCE(EXCLUDED.accent, voice_voices.accent),
             age = COALESCE(EXCLUDED.age, voice_voices.age),
             category = COALESCE(EXCLUDED.category, voice_voices.category),
             labels = COALESCE(EXCLUDED.labels, voice_voices.labels)`,
          [
            row.voice_id,
            row.name,
            row.description,
            row.preview_url,
            row.gender,
            row.accent,
            row.age,
            row.category,
            row.labels ? JSON.stringify(row.labels) : null,
          ],
        );
        const after = await pool.query('SELECT * FROM voice_voices WHERE voice_id = $1', [id]);
        return respond(200, after.rows[0]);
      }
      // DELETE /api/voices/:id
      if (method === 'DELETE' && id && !sub) {
        await pool.query('DELETE FROM voice_voices WHERE voice_id = $1', [id]);
        return respond(200, { deleted: true });
      }
      // GET /api/voices/:id/agents
      if (method === 'GET' && id && sub === 'agents') {
        const r = await pool.query('SELECT id, name, display_name, description FROM voice_agents WHERE tts_voice_id = $1', [id]);
        return respond(200, { agents: r.rows });
      }
    }

    // ─── PAYER KNOWLEDGE GRAPH ─────────────────────────────────────────
    if (resource === 'payers') {
      // GET /api/payers — list all payers
      if (method === 'GET' && !id) {
        const r = await pool.query('SELECT * FROM voice_payer_knowledge ORDER BY payer_name');
        return respond(200, { payers: r.rows, total: r.rows.length });
      }
      // GET /api/payers/:id
      if (method === 'GET' && id) {
        const r = await pool.query('SELECT * FROM voice_payer_knowledge WHERE id = $1 OR payer_id = $1', [id]);
        return respond(r.rows.length ? 200 : 404, r.rows[0] || { detail: 'Not found' });
      }
      // POST /api/payers/seed — seed top 10 US payers
      if (method === 'POST' && id === 'seed') {
        const payers = [
          { payer_name: 'Aetna', payer_id: 'aetna', phone_claims: '+18002423539', phone_auth: '+18008242898', phone_eligibility: '+18008242898', phone_appeals: '+18002423539', fax_claims: '1-859-455-8650', fax_appeals: '1-859-455-8650', portal_url: 'https://availity.com', timely_filing_days: 90, appeal_deadline_days: 180, payer_type: 'commercial', ivr_path_claims: [{ step: 1, press: '1', label: 'Provider' }, { step: 2, press: '3', label: 'Claims status' }, { step: 3, action: 'enter_npi', label: 'Enter NPI' }, { step: 4, action: 'enter_claim', label: 'Enter claim number' }] },
          { payer_name: 'UnitedHealthcare', payer_id: 'uhc', phone_claims: '+18008428440', phone_auth: '+18008007898', phone_eligibility: '+18008428440', phone_appeals: '+18008428440', fax_claims: '1-801-994-1349', fax_appeals: '1-801-994-1349', portal_url: 'https://uhcprovider.com', timely_filing_days: 90, appeal_deadline_days: 180, payer_type: 'commercial', ivr_path_claims: [{ step: 1, press: '1', label: 'Claims' }, { step: 2, press: '2', label: 'Claim status' }, { step: 3, action: 'enter_tax_id', label: 'Enter Tax ID' }] },
          { payer_name: 'Blue Cross Blue Shield', payer_id: 'bcbs', phone_claims: '+18009019999', phone_auth: '+18009019999', phone_eligibility: '+18009019999', phone_appeals: '+18009019999', fax_claims: '1-800-447-4930', portal_url: 'https://availity.com', timely_filing_days: 180, appeal_deadline_days: 120, payer_type: 'commercial', ivr_path_claims: [{ step: 1, press: '1', label: 'Provider services' }, { step: 2, press: '1', label: 'Claims' }] },
          { payer_name: 'Cigna', payer_id: 'cigna', phone_claims: '+18005541212', phone_auth: '+18007263477', phone_eligibility: '+18005541212', phone_appeals: '+18005541212', fax_claims: '1-859-410-1579', portal_url: 'https://cignaforhcp.cigna.com', timely_filing_days: 120, appeal_deadline_days: 180, payer_type: 'commercial', ivr_path_claims: [{ step: 1, press: '2', label: 'Provider' }, { step: 2, press: '1', label: 'Claims inquiry' }] },
          { payer_name: 'Humana', payer_id: 'humana', phone_claims: '+18007721213', phone_auth: '+18004574708', phone_eligibility: '+18007721213', phone_appeals: '+18007721213', fax_claims: '1-800-949-2961', portal_url: 'https://availity.com', timely_filing_days: 90, appeal_deadline_days: 180, payer_type: 'commercial', ivr_path_claims: [{ step: 1, press: '4', label: 'Claims' }, { step: 2, action: 'enter_npi', label: 'Enter NPI' }] },
          { payer_name: 'Medicare Part B', payer_id: 'medicare_b', phone_claims: '+18009994334', phone_auth: '+18009994334', phone_eligibility: '+18009994334', phone_appeals: '+18009994334', fax_claims: 'N/A — electronic only', portal_url: 'https://www.cms.gov/medicare', timely_filing_days: 365, appeal_deadline_days: 120, payer_type: 'government', ivr_path_claims: [{ step: 1, press: '1', label: 'English' }, { step: 2, press: '3', label: 'Provider' }, { step: 3, press: '1', label: 'Claims' }] },
          { payer_name: 'Medicaid (CA Medi-Cal)', payer_id: 'medicaid_ca', phone_claims: '+19163268092', phone_eligibility: '+19163268092', phone_appeals: '+19163268092', portal_url: 'https://www.medi-cal.ca.gov', timely_filing_days: 180, appeal_deadline_days: 90, payer_type: 'government', ivr_path_claims: [{ step: 1, press: '2', label: 'Provider' }] },
          { payer_name: 'Tricare', payer_id: 'tricare', phone_claims: '+18444TRICARE', phone_eligibility: '+18444TRICARE', portal_url: 'https://www.tricare.mil', timely_filing_days: 365, appeal_deadline_days: 90, payer_type: 'government' },
          { payer_name: 'Anthem (Elevance)', payer_id: 'anthem', phone_claims: '+18003115990', phone_auth: '+18003115990', phone_eligibility: '+18003115990', fax_claims: '1-844-398-1498', portal_url: 'https://availity.com', timely_filing_days: 90, appeal_deadline_days: 180, payer_type: 'commercial', ivr_path_claims: [{ step: 1, press: '1', label: 'Provider' }, { step: 2, press: '2', label: 'Claims status' }] },
          { payer_name: 'Kaiser Permanente', payer_id: 'kaiser', phone_claims: '+18004647247', phone_eligibility: '+18004647247', portal_url: 'https://healthy.kaiserpermanente.org', timely_filing_days: 90, appeal_deadline_days: 180, payer_type: 'commercial' },
        ];
        let seeded = 0;
        for (const p of payers) {
          const res = await pool.query(
            `INSERT INTO voice_payer_knowledge (payer_name, payer_id, phone_claims, phone_auth, phone_eligibility, phone_appeals, fax_claims, fax_appeals, portal_url, ivr_path_claims, timely_filing_days, appeal_deadline_days, payer_type)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (payer_id) DO NOTHING`,
            [p.payer_name, p.payer_id, p.phone_claims||null, p.phone_auth||null, p.phone_eligibility||null, p.phone_appeals||null, p.fax_claims||null, p.fax_appeals||null, p.portal_url||null, JSON.stringify(p.ivr_path_claims||[]), p.timely_filing_days, p.appeal_deadline_days, p.payer_type]
          );
          if (res.rowCount) seeded++;
        }
        return respond(200, { seeded, total: payers.length });
      }
    }

    // ─── CALL FLOW TEMPLATES ───────────────────────────────────────────
    if (resource === 'call-templates') {
      if (method === 'GET' && !id) {
        const r = await pool.query('SELECT * FROM voice_call_templates ORDER BY use_case, display_name');
        return respond(200, { templates: r.rows });
      }
      if (method === 'GET' && id) {
        const r = await pool.query('SELECT * FROM voice_call_templates WHERE id = $1 OR name = $1', [id]);
        return respond(r.rows.length ? 200 : 404, r.rows[0] || { detail: 'Not found' });
      }
      // POST /api/call-templates/seed — seed 5 RCM use case templates
      if (method === 'POST' && id === 'seed') {
        const templates = [
          { name: 'denial-followup', display_name: 'Denial Follow-up', use_case: 'denial_followup',
            description: 'Call payer to investigate denial reason, get resubmission instructions, deadline, and reference number.',
            system_prompt: `You are Chris, an AI calling agent for {{practice_name}}. You are calling {{payer_name}} about a denied claim.\n\nClaim details:\n- Claim ID: {{claim_id}}\n- Patient: {{patient_name}}\n- DOS: {{dos}}\n- Billed: {{billed_amount}}\n- Denial code: {{denial_code}}\n- Denial reason: {{denial_reason}}\n- Provider: {{provider_name}}, NPI: {{npi}}\n\nYour goals (collect ALL of these):\n1. Confirm the exact denial reason\n2. Ask what is needed to get the claim reprocessed\n3. Get the submission method (fax number, portal, or mail address)\n4. Get the timely filing deadline for resubmission\n5. Get a call reference number\n\nRules:\n- Be professional and concise\n- Navigate IVR menus by pressing the appropriate keys\n- If put on hold, wait patiently\n- If the rep gives incorrect info, politely push back with what you know\n- Always confirm what you heard before ending the call\n- Never reveal you are an AI`,
            first_message: 'Hi, this is Chris calling from {{practice_name}} regarding a denied claim. I am calling about claim number {{claim_id}} for patient {{patient_name}}, date of service {{dos}}. May I speak with someone in your claims department?',
            required_fields: ['denial_reason_confirmed','action_required','deadline','fax_or_portal','call_reference'],
            success_criteria: ['Got specific denial reason','Got resubmission instructions','Got deadline','Got reference number'],
            post_call_analyses: { model: 'us.anthropic.claude-haiku-4-5-20251001', fields: [
              { name: 'call_outcome', type: 'selector', choices: ['resolved','unresolved','voicemail','transferred','callback_requested'] },
              { name: 'denial_reason_confirmed', type: 'text', description: 'Exact reason payer gave for denial' },
              { name: 'action_required', type: 'text', description: 'What needs to happen to resolve' },
              { name: 'deadline', type: 'text', description: 'Timely filing deadline (date)' },
              { name: 'submission_method', type: 'selector', choices: ['fax','portal','mail','electronic'] },
              { name: 'fax_number', type: 'text' },
              { name: 'call_reference', type: 'text' },
              { name: 'payer_sentiment', type: 'selector', choices: ['cooperative','neutral','unhelpful','hostile'] },
              { name: 'resolution_confidence', type: 'selector', choices: ['high','medium','low'] },
            ]}},
          { name: 'claims-status', display_name: 'Claims Status Inquiry', use_case: 'claims_status',
            description: 'Check payment status on a submitted claim — is it processed, pending, or needs additional info.',
            system_prompt: `You are Chris, an AI calling agent for {{practice_name}}. You are calling {{payer_name}} to check the status of a submitted claim.\n\nClaim: {{claim_id}}, Patient: {{patient_name}}, DOS: {{dos}}, Billed: {{billed_amount}}\n\nGoals:\n1. Get current claim status (processed/pending/denied/in review)\n2. If processed: get payment amount, check number, payment date\n3. If pending: get expected processing date and what might be needed\n4. If denied: get denial reason and appeal instructions\n5. Get call reference number\n\nBe concise and professional.`,
            first_message: 'Hi, this is Chris calling from {{practice_name}}. I am checking the status of claim {{claim_id}} for patient {{patient_name}}, date of service {{dos}}.',
            required_fields: ['claim_status','payment_amount','call_reference'],
            success_criteria: ['Got claim status','Got payment details if paid','Got reference number'] },
          { name: 'prior-auth-check', display_name: 'Prior Auth Status Check', use_case: 'prior_auth',
            description: 'Check if a prior authorization was approved, denied, or is still pending.',
            system_prompt: `You are Chris, an AI calling agent for {{practice_name}}. Calling {{payer_name}} to check prior authorization status.\n\nPatient: {{patient_name}}, Auth #: {{auth_number}}, Procedure: {{procedure_code}}\n\nGoals:\n1. Confirm PA is on file\n2. Get PA status (approved/denied/pending/expired)\n3. If approved: get auth number, effective dates, approved units\n4. If denied: get reason and appeal options\n5. Get call reference\n\nBe professional. Push back if the rep says no PA is on file when you submitted one.`,
            first_message: 'Hi, this is Chris from {{practice_name}}. I am checking the status of a prior authorization for patient {{patient_name}}.',
            required_fields: ['auth_status','auth_number','effective_dates','call_reference'] },
          { name: 'eligibility-verification', display_name: 'Eligibility Verification', use_case: 'eligibility',
            description: 'Verify patient insurance coverage, benefits, copay, deductible, and network status by phone.',
            system_prompt: `You are Chris, an AI calling agent for {{practice_name}}. Calling {{payer_name}} to verify benefits for a patient.\n\nPatient: {{patient_name}}, DOB: {{dob}}, Member ID: {{member_id}}, Group: {{group_number}}\n\nGoals:\n1. Confirm patient is active on the plan\n2. Get effective dates\n3. Get copay amount for office visit\n4. Get deductible (individual/family) and amount met\n5. Get coinsurance percentage\n6. Confirm provider is in-network\n7. Get call reference\n\nBe thorough. These details prevent claim denials.`,
            first_message: 'Hi, this is Chris from {{practice_name}}. I need to verify benefits for patient {{patient_name}}, member ID {{member_id}}.',
            required_fields: ['active','copay','deductible','deductible_met','coinsurance','in_network','call_reference'] },
          { name: 'patient-collections', display_name: 'Patient Balance Follow-up', use_case: 'patient_collections',
            description: 'Call patients about outstanding balances — offer payment plans, collect payment, or update contact info.',
            system_prompt: `You are Cindy, a friendly billing assistant from {{practice_name}}. Calling patient {{patient_name}} about an outstanding balance.\n\nBalance: {{balance_due}}, Last DOS: {{dos}}, Last Statement: {{last_statement_date}}\n\nGoals:\n1. Confirm you are speaking with the correct person\n2. Inform them of the outstanding balance\n3. Offer payment options: pay in full, payment plan, or financial hardship application\n4. If they want a payment plan: minimum $25/month or 20% of balance\n5. If financial hardship: explain the application process\n6. Update their preferred contact method\n\nBe empathetic and professional. Never be aggressive. HIPAA: confirm identity before discussing balance.`,
            first_message: 'Hi, this is Cindy calling from {{practice_name}}. May I please speak with {{patient_name}}? I have a quick update about your account.',
            required_fields: ['identity_confirmed','payment_outcome','payment_plan_amount','next_contact_date'] },
        ];
        let seeded = 0;
        for (const t of templates) {
          const res = await pool.query(
            `INSERT INTO voice_call_templates (name, display_name, use_case, description, system_prompt, first_message, required_fields, success_criteria, post_call_analyses, llm_model)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (name) DO NOTHING`,
            [t.name, t.display_name, t.use_case, t.description, t.system_prompt, t.first_message, JSON.stringify(t.required_fields||[]), JSON.stringify(t.success_criteria||[]), JSON.stringify(t.post_call_analyses||{}), t.llm_model||'us.anthropic.claude-sonnet-4-6']
          );
          if (res.rowCount) seeded++;
        }
        return respond(200, { seeded, total: templates.length });
      }
    }

    // ─── POST-CALL AUTO-ACTIONS ────────────────────────────────────────
    if (resource === 'auto-actions') {
      // POST /api/auto-actions — trigger post-call actions for a call
      if (method === 'POST' && !id) {
        const callId = body.call_id;
        if (!callId) return respond(400, { detail: 'call_id required' });
        const cr = await pool.query('SELECT * FROM voice_calls WHERE id = $1', [callId]);
        if (!cr.rows.length) return respond(404, { detail: 'Call not found' });
        const call = cr.rows[0];
        const pa = call.post_call_analyses || {};
        const cd = call.case_data || {};
        const actions = [];

        // Auto-action 1: Create follow-up task if action_required exists
        if (pa.action_required) {
          const taskId = uuid();
          actions.push({ action_type: 'create_task', target_table: 'tasks', target_id: taskId, payload: {
            title: `[Voice AI] ${pa.action_required}`,
            description: `Payer: ${cd.payer}\nPatient: ${cd.patient_name}\nClaim: ${cd.claim_id}\nDeadline: ${pa.deadline || 'N/A'}\nRef: ${pa.call_reference || 'N/A'}\nSubmit via: ${pa.submission_method || 'N/A'} ${pa.fax_number || ''}`,
            due_date: pa.deadline || null,
            priority: pa.resolution_confidence === 'high' ? 'medium' : 'urgent',
            source: 'voice_ai',
            claim_id: cd.claim_id,
          }});
        }

        // Auto-action 2: Log to AR call log
        if (cd.claim_id) {
          actions.push({ action_type: 'log_ar_call', target_table: 'ar_calls', payload: {
            claim_id: cd.claim_id,
            payer: cd.payer,
            outcome: pa.call_outcome || call.status,
            reference: pa.call_reference,
            notes: pa.action_required,
            follow_up_date: pa.deadline,
            source: 'voice_ai_auto',
            call_id: callId,
          }});
        }

        // Auto-action 3: Route to appeals if denial confirmed
        if (pa.call_outcome === 'resolved' && pa.denial_reason_confirmed && cd.claim_id) {
          actions.push({ action_type: 'route_denial', target_table: 'denials', payload: {
            claim_id: cd.claim_id,
            denial_reason: pa.denial_reason_confirmed,
            action_required: pa.action_required,
            deadline: pa.deadline,
            source: 'voice_ai',
          }});
        }

        // Auto-action 4: Calculate and store call cost
        const dur = call.duration_secs || 0;
        const costTelephony = Math.ceil(dur / 60) * 0.013;
        const transcriptLen = JSON.stringify(call.transcript || []).length;
        const costStt = (transcriptLen / 1000) * 0.004;
        const costLlm = (transcriptLen / 4000) * 0.003 + (transcriptLen / 4000) * 0.015;
        const costTts = (transcriptLen / 2000) * 0.015;
        const totalCost = costTelephony + costStt + costLlm + costTts;
        actions.push({ action_type: 'track_cost', target_table: 'voice_call_costs', payload: {
          telephony_cost: costTelephony.toFixed(4), stt_cost: costStt.toFixed(4),
          llm_cost: costLlm.toFixed(4), tts_cost: costTts.toFixed(4),
          total_cost: totalCost.toFixed(4), telephony_minutes: (dur/60).toFixed(2),
        }});
        await pool.query(
          `INSERT INTO voice_call_costs (call_id, telephony_cost, stt_cost, llm_cost, tts_cost, total_cost, telephony_minutes)
           VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (call_id) DO UPDATE SET total_cost = EXCLUDED.total_cost`,
          [callId, costTelephony, costStt, costLlm, costTts, totalCost, dur/60]
        );

        // Auto-action 5: Score call quality
        const score = {
          collected_reference: !!pa.call_reference,
          collected_deadline: !!pa.deadline,
          collected_action: !!pa.action_required,
          collected_fax_portal: !!(pa.fax_number || pa.submission_method),
          confirmed_denial_reason: !!pa.denial_reason_confirmed,
        };
        const points = Object.values(score).filter(Boolean).length;
        const overall = Math.round((points / Object.keys(score).length) * 100);
        await pool.query(
          `INSERT INTO voice_call_scores (call_id, overall_score, collected_reference, collected_deadline, collected_action, collected_fax_portal, confirmed_denial_reason)
           VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (call_id) DO UPDATE SET overall_score = EXCLUDED.overall_score`,
          [callId, overall, score.collected_reference, score.collected_deadline, score.collected_action, score.collected_fax_portal, score.confirmed_denial_reason]
        );

        // Log all actions
        for (const a of actions) {
          await pool.query(
            'INSERT INTO voice_auto_actions (call_id, action_type, target_table, target_id, payload) VALUES ($1,$2,$3,$4,$5)',
            [callId, a.action_type, a.target_table, a.target_id || null, JSON.stringify(a.payload)]
          );
        }

        return respond(200, { call_id: callId, actions_taken: actions.length, actions: actions.map(a => ({ type: a.action_type, table: a.target_table })), cost: totalCost.toFixed(4), quality_score: overall });
      }
      // GET /api/auto-actions?call_id=... — get actions for a call
      if (method === 'GET') {
        const cid = qs.call_id || id;
        const r = cid
          ? await pool.query('SELECT * FROM voice_auto_actions WHERE call_id = $1 ORDER BY created_at', [cid])
          : await pool.query('SELECT * FROM voice_auto_actions ORDER BY created_at DESC LIMIT 50');
        return respond(200, { actions: r.rows });
      }
    }

    // ─── CLAIM CONTEXT LOADER ──────────────────────────────────────────
    if (resource === 'claim-context') {
      // GET /api/claim-context/:claimId — load full claim data for voice agent prompt
      if (method === 'GET' && id) {
        // Try to load from claims table (public schema for cross-tenant)
        const queries = [
          pool.query(`SELECT * FROM public.claims WHERE claim_number = $1 OR id::text = $1 LIMIT 1`, [id]).catch(() => ({ rows: [] })),
          pool.query(`SELECT * FROM public.patients WHERE id = (SELECT patient_id FROM public.claims WHERE claim_number = $1 LIMIT 1)`, [id]).catch(() => ({ rows: [] })),
          pool.query(`SELECT * FROM public.encounters WHERE claim_id = (SELECT id FROM public.claims WHERE claim_number = $1 LIMIT 1)`, [id]).catch(() => ({ rows: [] })),
          pool.query(`SELECT * FROM public.denials WHERE claim_id = (SELECT id FROM public.claims WHERE claim_number = $1 LIMIT 1) ORDER BY created_at DESC LIMIT 1`, [id]).catch(() => ({ rows: [] })),
        ];
        const [claimR, patientR, encounterR, denialR] = await Promise.all(queries);
        const claim = claimR.rows[0] || null;
        const patient = patientR.rows[0] || null;
        const encounter = encounterR.rows[0] || null;
        const denial = denialR.rows[0] || null;
        // Look up payer in knowledge graph
        let payer = null;
        if (claim?.payer_name || claim?.payer_id) {
          const pr = await pool.query('SELECT * FROM voice_payer_knowledge WHERE payer_name ILIKE $1 OR payer_id = $2 LIMIT 1', [`%${claim.payer_name || ''}%`, claim.payer_id || '']);
          payer = pr.rows[0] || null;
        }
        return respond(200, { claim, patient, encounter, denial, payer, has_data: !!claim });
      }
    }

    // ─── COST & ROI DASHBOARD ──────────────────────────────────────────
    if (resource === 'roi') {
      if (method === 'GET') {
        const costsR = await pool.query('SELECT SUM(total_cost) as total_spent, COUNT(*) as calls_costed, AVG(total_cost) as avg_cost FROM voice_call_costs');
        const callsR = await pool.query('SELECT COUNT(*) as total_calls, COUNT(*) FILTER (WHERE status = \'completed\') as completed, COUNT(*) FILTER (WHERE status = \'no_answer\') as no_answer FROM voice_calls');
        const scoresR = await pool.query('SELECT AVG(overall_score) as avg_quality, COUNT(*) FILTER (WHERE overall_score >= 80) as high_quality, COUNT(*) as scored FROM voice_call_scores');
        const costs = costsR.rows[0];
        const calls = callsR.rows[0];
        const scores = scoresR.rows[0];
        // Estimate $ recovered: avg resolved claim = $1,500, completion rate * count
        const completionRate = calls.total_calls > 0 ? parseInt(calls.completed) / parseInt(calls.total_calls) : 0;
        const estimatedRecovered = parseInt(calls.completed) * 1500;
        const roi = parseFloat(costs.total_spent) > 0 ? Math.round(estimatedRecovered / parseFloat(costs.total_spent)) : 0;
        return respond(200, {
          calls: { total: parseInt(calls.total_calls), completed: parseInt(calls.completed), no_answer: parseInt(calls.no_answer), completion_rate: Math.round(completionRate * 100) },
          costs: { total_spent: parseFloat(costs.total_spent || 0).toFixed(2), avg_per_call: parseFloat(costs.avg_cost || 0).toFixed(4), calls_costed: parseInt(costs.calls_costed || 0) },
          quality: { avg_score: Math.round(parseFloat(scores.avg_quality || 0)), high_quality_pct: scores.scored > 0 ? Math.round((parseInt(scores.high_quality) / parseInt(scores.scored)) * 100) : 0 },
          roi: { estimated_recovered: estimatedRecovered, total_spent: parseFloat(costs.total_spent || 0).toFixed(2), roi_multiple: roi + 'x' },
        });
      }
    }

    // ─── DENIAL PATTERN DETECTION ──────────────────────────────────────
    if (resource === 'denial-patterns') {
      if (method === 'GET') {
        const r = await pool.query(`
          SELECT
            (case_data->>'payer')::text as payer,
            (case_data->>'denial_reason')::text as denial_reason,
            COUNT(*) as call_count,
            COUNT(*) FILTER (WHERE (post_call_analyses->>'call_outcome') = 'resolved') as resolved,
            ARRAY_AGG(DISTINCT (post_call_analyses->>'action_required')) FILTER (WHERE (post_call_analyses->>'action_required') IS NOT NULL) as common_actions,
            ROUND(AVG(duration_secs)) as avg_duration
          FROM voice_calls
          WHERE case_data->>'payer' IS NOT NULL AND case_data->>'denial_reason' IS NOT NULL
          GROUP BY (case_data->>'payer'), (case_data->>'denial_reason')
          ORDER BY call_count DESC
          LIMIT 50
        `);
        return respond(200, { patterns: r.rows, total: r.rows.length });
      }
    }

    // ─── INBOUND ROUTES ────────────────────────────────────────────────
    if (resource === 'inbound-routes') {
      if (method === 'GET' && !id) {
        const r = await pool.query('SELECT ir.*, va.display_name as agent_display_name FROM voice_inbound_routes ir LEFT JOIN voice_agents va ON ir.agent_name = va.name ORDER BY ir.created_at');
        return respond(200, { routes: r.rows });
      }
      if (method === 'POST' && !id) {
        const rid = uuid();
        await pool.query('INSERT INTO voice_inbound_routes (id, phone_number, agent_name, greeting, active) VALUES ($1,$2,$3,$4,$5)', [rid, body.phone_number, body.agent_name, body.greeting||'', body.active !== false]);
        return respond(201, { id: rid });
      }
      if (method === 'DELETE' && id) {
        await pool.query('DELETE FROM voice_inbound_routes WHERE id = $1', [id]);
        return respond(200, { deleted: true });
      }
    }

    // ─── CALL REQUESTS (async call queue) ──────────────────────────────
    if (resource === 'call-requests') {
      // POST /api/call-requests — queue a call
      if (method === 'POST' && !id) {
        const errors = [];
        if (!body.agent_name) errors.push('agent_name is required');
        if (!body.to_number) errors.push('to_number is required');
        if (!body.from_number) errors.push('from_number is required');
        if (body.to_number && !/^\+[1-9]\d{6,14}$/.test(body.to_number)) errors.push('to_number must be E.164 format');
        if (body.from_number && !/^\+[1-9]\d{6,14}$/.test(body.from_number)) errors.push('from_number must be E.164 format');

        if (body.agent_name) {
          const ac = await pool.query('SELECT name FROM voice_agents WHERE name = $1', [body.agent_name]);
          if (!ac.rows.length) errors.push(`Agent "${body.agent_name}" not found`);
        }

        if (body.dedup_key) {
          const dc = await pool.query(
            `SELECT id, status FROM voice_call_requests WHERE dedup_key = $1 AND status NOT IN ('failed','dead_letter','skipped') AND created_at > NOW() - INTERVAL '24 hours'`,
            [body.dedup_key]
          );
          if (dc.rows.length) return respond(200, { id: dc.rows[0].id, status: dc.rows[0].status, message: 'Duplicate — already queued or processed' });
        }

        if (errors.length) return respond(400, { error: 'Validation failed', details: errors });

        const r = await pool.query(
          `INSERT INTO voice_call_requests (agent_name, to_number, from_number, case_data, trigger_source, dedup_key, calling_window_start, calling_window_end, calling_window_timezone, calling_window_days, max_attempts, scheduled_after)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id, status, created_at`,
          [body.agent_name, body.to_number, body.from_number, JSON.stringify(body.case_data || {}), body.trigger_source || null, body.dedup_key || null,
           body.calling_window_start || '09:00', body.calling_window_end || '17:00', body.calling_window_timezone || 'America/New_York',
           JSON.stringify(body.calling_window_days || ['Mon','Tue','Wed','Thu','Fri']), body.max_attempts || 3, body.scheduled_after || null]
        );

        // Send to SQS so the EC2 consumer can process it
        try {
          await sqsClient.send(new SendMessageCommand({
            QueueUrl: CALL_QUEUE_URL,
            MessageBody: JSON.stringify({
              request_id: r.rows[0].id,
              agent_name: body.agent_name,
              to_number: body.to_number,
              from_number: body.from_number,
              case_data: body.case_data || {},
              trigger_source: body.trigger_source || null,
              dedup_key: body.dedup_key || null,
              attempt_count: 0,
              max_attempts: body.max_attempts || 3,
              calling_window_start: body.calling_window_start || '09:00',
              calling_window_end: body.calling_window_end || '17:00',
              calling_window_timezone: body.calling_window_timezone || 'America/New_York',
              calling_window_days: body.calling_window_days || ['Mon','Tue','Wed','Thu','Fri'],
            }),
          }));
        } catch (sqsErr) {
          // Non-fatal — row is saved, consumer can fall back to Aurora polling
          console.error('[voice-api] SQS send failed:', sqsErr.message);
        }

        return respond(201, { id: r.rows[0].id, status: 'queued', created_at: r.rows[0].created_at, message: 'Call request queued' });
      }

      // GET /api/call-requests — list with filters
      if (method === 'GET' && !id) {
        let q = 'SELECT * FROM voice_call_requests';
        const conds = []; const vals = []; let idx = 1;
        if (qs.status) { conds.push(`status = $${idx++}`); vals.push(qs.status); }
        if (qs.agent_name) { conds.push(`agent_name = $${idx++}`); vals.push(qs.agent_name); }
        if (qs.trigger_source) { conds.push(`trigger_source = $${idx++}`); vals.push(qs.trigger_source); }
        if (conds.length) q += ' WHERE ' + conds.join(' AND ');
        q += ' ORDER BY created_at DESC LIMIT 100';
        const r = await pool.query(q, vals);
        return respond(200, { call_requests: r.rows, total: r.rows.length });
      }

      // GET /api/call-requests/:id
      if (method === 'GET' && id && !sub) {
        const r = await pool.query('SELECT * FROM voice_call_requests WHERE id = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Call request not found' });
        return respond(200, r.rows[0]);
      }

      // PUT /api/call-requests/:id — update status/fields
      if (method === 'PUT' && id && !sub) {
        const allowed = ['status', 'attempt_count', 'call_id', 'error_message', 'scheduled_after'];
        const updates = []; const vals = []; let idx = 1;
        for (const col of allowed) {
          if (body[col] !== undefined) { updates.push(`${col} = $${idx++}`); vals.push(body[col]); }
        }
        if (!updates.length) return respond(400, { detail: 'No valid fields to update' });
        updates.push('updated_at = NOW()');
        vals.push(id);
        const r = await pool.query(`UPDATE voice_call_requests SET ${updates.join(', ')} WHERE id = $${idx} RETURNING *`, vals);
        if (!r.rows.length) return respond(404, { detail: 'Call request not found' });
        return respond(200, r.rows[0]);
      }
    }

    // ─── BATCH ROWS (per-row state for batches) ─────────────────────────
    if (resource === 'batch-rows') {
      // GET /api/batch-rows/progress?batch_id=X — derived counts
      if (method === 'GET' && id === 'progress') {
        if (!qs.batch_id) return respond(400, { detail: 'batch_id required' });
        const r = await pool.query(`
          SELECT
            COUNT(*)::int as total,
            COUNT(*) FILTER (WHERE status = 'completed')::int as completed,
            COUNT(*) FILTER (WHERE status IN ('failed','no_answer','busy'))::int as failed,
            COUNT(*) FILTER (WHERE status = 'pending')::int as pending,
            COUNT(*) FILTER (WHERE status = 'dialing')::int as dialing,
            COUNT(*) FILTER (WHERE status = 'queued')::int as queued,
            COUNT(*) FILTER (WHERE status = 'skipped')::int as skipped,
            COUNT(*) FILTER (WHERE status = 'cancelled')::int as cancelled
          FROM voice_batch_rows WHERE batch_id = $1
        `, [qs.batch_id]);
        return respond(200, r.rows[0]);
      }

      // POST /api/batch-rows — bulk insert
      if (method === 'POST' && !id) {
        if (!body.batch_id) return respond(400, { detail: 'batch_id required' });
        if (!Array.isArray(body.rows)) return respond(400, { detail: 'rows array required' });
        if (body.rows.length === 0) return respond(200, { inserted: 0, rows: [] });

        const placeholders = [];
        const vals = [];
        let i = 1;
        for (const row of body.rows) {
          placeholders.push(`($${i}, $${i+1}, $${i+2}, $${i+3}, $${i+4})`);
          vals.push(
            body.batch_id,
            row.row_index,
            row.phone_e164,
            JSON.stringify(row.case_data || {}),
            row.max_attempts || 3,
          );
          i += 5;
        }
        const r = await pool.query(
          `INSERT INTO voice_batch_rows (batch_id, row_index, phone_e164, case_data, max_attempts)
           VALUES ${placeholders.join(', ')}
           ON CONFLICT (batch_id, row_index) DO UPDATE SET
             phone_e164 = EXCLUDED.phone_e164,
             case_data = EXCLUDED.case_data,
             max_attempts = EXCLUDED.max_attempts,
             updated_at = NOW()
           RETURNING id, row_index, phone_e164, status`,
          vals
        );
        return respond(201, { inserted: r.rows.length, rows: r.rows });
      }

      // GET /api/batch-rows?batch_id=X
      if (method === 'GET' && !id) {
        if (!qs.batch_id) return respond(400, { detail: 'batch_id required' });
        let q = 'SELECT * FROM voice_batch_rows WHERE batch_id = $1';
        const v = [qs.batch_id];
        let i = 2;
        if (qs.status) { q += ` AND status = $${i++}`; v.push(qs.status); }
        q += ' ORDER BY row_index ASC';
        if (qs.limit) q += ` LIMIT ${parseInt(qs.limit, 10)}`;
        const r = await pool.query(q, v);
        return respond(200, { rows: r.rows, total: r.rows.length });
      }

      // GET /api/batch-rows/:id — single row
      if (method === 'GET' && id && !sub) {
        const r = await pool.query('SELECT * FROM voice_batch_rows WHERE id = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Row not found' });
        return respond(200, r.rows[0]);
      }

      // PUT /api/batch-rows/:id
      if (method === 'PUT' && id && !sub) {
        const allowed = ['status', 'call_id', 'error_message', 'attempt_count', 'scheduled_after'];
        const updates = []; const vals = []; let i = 1;
        for (const col of allowed) {
          if (body[col] !== undefined) { updates.push(`${col} = $${i++}`); vals.push(body[col]); }
        }
        if (!updates.length) return respond(400, { detail: 'No valid fields' });
        updates.push('updated_at = NOW()');
        vals.push(id);
        const r = await pool.query(`UPDATE voice_batch_rows SET ${updates.join(', ')} WHERE id = $${i} RETURNING *`, vals);
        if (!r.rows.length) return respond(404, { detail: 'Row not found' });
        return respond(200, r.rows[0]);
      }
    }

    // ─── SYSTEM CONFIG ──────────────────────────────────────────────────
    if (resource === 'system-config') {
      if (method === 'GET' && id) {
        const r = await pool.query('SELECT * FROM voice_system_config WHERE key = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Config not found' });
        return respond(200, r.rows[0]);
      }
      if (method === 'PUT' && id) {
        const val = body.value !== undefined ? body.value : body;
        const r = await pool.query(
          `INSERT INTO voice_system_config (key, value) VALUES ($1, $2)
           ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()
           RETURNING *`,
          [id, JSON.stringify(val)]
        );
        return respond(200, r.rows[0]);
      }
    }

    return respond(404, { detail: `Route not found: ${method} ${path}` });

  } catch (err) {
    console.error('[voice-api] Error:', err.message, err.stack?.substring(0, 300));
    return respond(500, { detail: err.message || 'Internal server error' });
  }
}
