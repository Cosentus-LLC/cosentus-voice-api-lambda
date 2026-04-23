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
 *   GET/POST     /api/agent-versions/:id  — Version history (replaces Supabase)
 *   GET          /api/calls               — Call history (paginated, filterable)
 *   GET          /api/calls/:id           — Call detail
 *   GET          /api/calls/:id/recording-url — Signed recording URL
 *   GET          /api/calls/agents        — Distinct agent names from calls
 *   GET          /api/batches             — List batches
 *   GET          /api/batches/:id         — Batch detail + calls
 *   POST         /api/batches/upload      — Upload CSV
 *   PUT          /api/batches/:id/rows    — Update mapped rows
 *   POST         /api/batches/:id/start   — Start batch
 *   POST         /api/batches/:id/pause   — Pause batch
 *   POST         /api/batches/:id/resume  — Resume batch
 *   POST         /api/batches/:id/cancel  — Cancel batch
 *   GET          /api/batches/:id/status  — Batch progress
 *   GET          /api/batches/:id/download-url — Results download
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
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`,
    `CREATE INDEX IF NOT EXISTS idx_vc_agent ON voice_calls(agent_name)`,
    `CREATE INDEX IF NOT EXISTS idx_vc_status ON voice_calls(status)`,
    `CREATE INDEX IF NOT EXISTS idx_vc_batch ON voice_calls(batch_id)`,
    `CREATE INDEX IF NOT EXISTS idx_vc_created ON voice_calls(created_at DESC)`,
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

const AGENT_COLS = `id, name, display_name, description, llm_provider, llm_model, temperature, max_tokens, enable_prompt_caching, tts_provider, tts_voice_id, tts_model, tts_stability, tts_similarity_boost, tts_style, tts_use_speaker_boost, tts_speed, stt_provider, stt_language, stt_keywords, tools, system_prompt, first_message, ivr_goal, recording_enabled, recording_channels, post_call_analyses, idle_timeout_secs, idle_message, max_call_duration_secs, voicemail_action, voicemail_message, max_retries, retry_delay_secs, default_concurrency, calling_window_start, calling_window_end, calling_window_days, is_active, created_at, updated_at`;

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
    if (resource === 'health') {
      const r = await pool.query('SELECT 1 as ok');
      return respond(200, { status: 'healthy', db: r.rows[0]?.ok === 1, version: 'voice-api-v1' });
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
        const cols = ['id', 'name', 'display_name', 'description', 'llm_provider', 'llm_model', 'temperature', 'max_tokens', 'system_prompt', 'first_message', 'tts_provider', 'tts_voice_id', 'tts_model', 'tools', 'post_call_analyses', 'stt_provider', 'stt_language'];
        const vals = [newId, name, body.display_name || name, body.description || '', body.llm_provider || 'anthropic', body.llm_model || 'us.anthropic.claude-sonnet-4-6', body.temperature ?? 0.7, body.max_tokens ?? 1024, body.system_prompt || '', body.first_message || '', body.tts_provider || 'elevenlabs', body.tts_voice_id || '', body.tts_model || 'eleven_turbo_v2_5', JSON.stringify(body.tools || []), JSON.stringify(body.post_call_analyses || { model: 'us.anthropic.claude-haiku-4-5-20251001', fields: [] }), body.stt_provider || 'elevenlabs', body.stt_language || 'en'];
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
        const allowed = ['display_name', 'description', 'llm_provider', 'llm_model', 'temperature', 'max_tokens', 'enable_prompt_caching', 'tts_provider', 'tts_voice_id', 'tts_model', 'tts_stability', 'tts_similarity_boost', 'tts_style', 'tts_use_speaker_boost', 'tts_speed', 'stt_provider', 'stt_language', 'stt_keywords', 'tools', 'system_prompt', 'first_message', 'ivr_goal', 'recording_enabled', 'recording_channels', 'post_call_analyses', 'idle_timeout_secs', 'idle_message', 'max_call_duration_secs', 'voicemail_action', 'voicemail_message', 'max_retries', 'retry_delay_secs', 'default_concurrency', 'calling_window_start', 'calling_window_end', 'calling_window_days', 'is_active'];
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
          `INSERT INTO voice_agents (id, name, display_name, description, llm_provider, llm_model, temperature, max_tokens, system_prompt, first_message, tts_provider, tts_voice_id, tts_model, tools, post_call_analyses, stt_provider, stt_language)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`,
          [newId, newName, body.display_name || newName, agent.description, agent.llm_provider, agent.llm_model, agent.temperature, agent.max_tokens, agent.system_prompt, agent.first_message, agent.tts_provider, agent.tts_voice_id, agent.tts_model, JSON.stringify(agent.tools), JSON.stringify(agent.post_call_analyses), agent.stt_provider, agent.stt_language]
        );
        const r = await pool.query(`SELECT ${AGENT_COLS} FROM voice_agents WHERE id = $1`, [newId]);
        return respond(201, r.rows[0]);
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
      if (id && sub === 'recording-url' && method === 'GET') {
        const r = await pool.query('SELECT recording_path FROM voice_calls WHERE id = $1', [id]);
        if (!r.rows.length || !r.rows[0].recording_path) return respond(200, { url: null });
        // TODO: Generate S3 presigned URL for the recording
        return respond(200, { url: r.rows[0].recording_path });
      }
      // POST /api/calls — upsert call record (used by calling engine)
      if (method === 'POST' && !id) {
        const c = body;
        const callId = c.id || uuid();
        await pool.query(
          `INSERT INTO voice_calls (id, agent_name, agent_display_name, from_number, target_number, direction, status, started_at, ended_at, duration_secs, case_data, transcript, recording_path, post_call_analyses, error, batch_id, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
           ON CONFLICT (id) DO UPDATE SET
             status = COALESCE(EXCLUDED.status, voice_calls.status),
             ended_at = COALESCE(EXCLUDED.ended_at, voice_calls.ended_at),
             duration_secs = COALESCE(EXCLUDED.duration_secs, voice_calls.duration_secs),
             transcript = CASE WHEN EXCLUDED.transcript::text != '[]' THEN EXCLUDED.transcript ELSE voice_calls.transcript END,
             recording_path = COALESCE(EXCLUDED.recording_path, voice_calls.recording_path),
             post_call_analyses = CASE WHEN EXCLUDED.post_call_analyses::text != '{}' THEN EXCLUDED.post_call_analyses ELSE voice_calls.post_call_analyses END,
             error = COALESCE(EXCLUDED.error, voice_calls.error),
             updated_at = EXCLUDED.updated_at`,
          [callId, c.agent_name, c.agent_display_name, c.from_number, c.target_number||'', c.direction||'outbound', c.status||'completed', c.started_at, c.ended_at, c.duration_secs, JSON.stringify(c.case_data||{}), JSON.stringify(c.transcript||[]), c.recording_path, JSON.stringify(c.post_call_analyses||{}), c.error||null, c.batch_id||null, c.created_at||new Date().toISOString(), c.updated_at||new Date().toISOString()]
        );
        return respond(201, { id: callId });
      }
      // PUT /api/calls/:id/hide — soft-delete a call
      if (method === 'PUT' && id && sub === 'hide') {
        const r = await pool.query('UPDATE voice_calls SET hidden = true WHERE id = $1 RETURNING id', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Call not found' });
        return respond(200, { status: 'hidden', id: r.rows[0].id });
      }
      // GET /api/calls/:id
      if (method === 'GET' && id && !sub) {
        const r = await pool.query('SELECT * FROM voice_calls WHERE id = $1', [id]);
        if (!r.rows.length) return respond(404, { detail: 'Call not found' });
        return respond(200, r.rows[0]);
      }
      // GET /api/calls — paginated list
      if (method === 'GET' && !id) {
        const page = parseInt(qs.page) || 1;
        const pageSize = Math.min(parseInt(qs.page_size) || 25, 100);
        const offset = (page - 1) * pageSize;
        let where = 'WHERE (hidden IS NULL OR hidden = false)';
        const params = [];
        let idx = 1;
        if (qs.agent_name) { where += ` AND agent_name = $${idx++}`; params.push(qs.agent_name); }
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
      // POST /api/batches/upload (legacy — accepts JSON with rows directly)
      if (method === 'POST' && id === 'upload') {
        // TODO: Parse multipart form data (CSV file)
        // For now, accept JSON with rows
        const batchId = uuid();
        const rows = body.rows || [];
        await pool.query(
          `INSERT INTO voice_batches (id, name, agent_name, from_number, status, total_rows, rows)
           VALUES ($1, $2, $3, $4, 'draft', $5, $6)`,
          [batchId, body.name || `Batch ${new Date().toISOString().slice(0, 10)}`, body.agent_name, body.from_number, rows.length, JSON.stringify(rows)]
        );
        return respond(201, { batch_id: batchId, columns: Object.keys(rows[0] || {}), summary: { total: rows.length, valid: rows.length, fixable: 0, invalid: 0 }, rows });
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
      if (method === 'POST' && id && sub === 'start') {
        await pool.query(
          `UPDATE voice_batches SET status = $1, concurrency = $2, timezone = $3, calling_window_start = $4, calling_window_end = $5, calling_window_days = $6, updated_at = NOW() WHERE id = $7`,
          [body.schedule_mode === 'scheduled' ? 'scheduled' : 'running', body.concurrency || 1, body.timezone || null, body.calling_window_start || null, body.calling_window_end || null, body.calling_window_days ? JSON.stringify(body.calling_window_days) : null, id]
        );
        // TODO: Trigger actual batch calling via voice engine
        return respond(200, { status: 'started', batch_id: id });
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
      if (method === 'GET' && id && sub === 'download-url') {
        const r = await pool.query('SELECT output_file_path FROM voice_batches WHERE id = $1', [id]);
        return respond(200, { url: r.rows[0]?.output_file_path || null });
      }
      // DELETE /api/batches/:id/draft
      if (method === 'DELETE' && id && sub === 'draft') {
        await pool.query('DELETE FROM voice_batches WHERE id = $1 AND status = $2', [id, 'draft']);
        return respond(200, { deleted: true });
      }
    }

    // ═══ Phone Numbers ═══
    if (resource === 'phone-numbers') {
      // GET /api/phone-numbers
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
      // POST /api/phone-numbers (upsert by number)
      if (method === 'POST' && !id) {
        const pErrors = validatePhoneData(body);
        if (pErrors.length) return respond(400, { error: 'Validation failed', details: pErrors });
        const newId = uuid();
        const r = await pool.query(
          `INSERT INTO voice_phone_numbers (id, number, friendly_name)
           VALUES ($1, $2, $3)
           ON CONFLICT (number) DO UPDATE SET friendly_name = COALESCE(NULLIF($3, ''), voice_phone_numbers.friendly_name), updated_at = NOW()
           RETURNING *`,
          [newId, body.number, body.friendly_name || '']
        );
        return respond(201, r.rows[0]);
      }
      // PUT /api/phone-numbers/:id
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
      // POST /api/phone-numbers/sync-twilio
      if (method === 'POST' && id === 'sync-twilio') {
        // TODO: Call Twilio API to sync numbers
        return respond(200, { total: 0, message: 'Twilio sync not yet configured' });
      }
      // GET /api/phone-numbers/search
      if (method === 'GET' && id === 'search') {
        // TODO: Search Twilio for available numbers
        return respond(200, { numbers: [] });
      }
      // POST /api/phone-numbers/purchase
      if (method === 'POST' && id === 'purchase') {
        // TODO: Purchase via Twilio API
        return respond(200, { message: 'Twilio purchase not yet configured' });
      }
      // POST /api/phone-numbers/:id/release
      if (method === 'POST' && id && sub === 'release') {
        await pool.query('DELETE FROM voice_phone_numbers WHERE id = $1', [id]);
        return respond(200, { released: true });
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
      if (method === 'POST' && id === 'sync') {
        // TODO: Call ElevenLabs API to sync voices
        return respond(200, { count: 0, message: 'ElevenLabs sync not yet configured' });
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
      // POST /api/voices/:id/refresh — EC2 proxy fetches from ElevenLabs and POSTs to /voices/add
      if (method === 'POST' && id && sub === 'refresh') {
        const r = await pool.query('SELECT * FROM voice_voices WHERE voice_id = $1', [id]);
        return respond(200, r.rows[0] || {});
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
