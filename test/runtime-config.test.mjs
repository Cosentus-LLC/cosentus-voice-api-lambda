/**
 * Unit tests for buildRuntimeConfig — the shape contract with the fork's
 * Fargate voice pipeline (Phase 7A).
 *
 * The fork parses this response directly into its PipelineConfig dataclass,
 * so every field mapping + default behavior + type coercion here is part
 * of the public contract. If a test here fails because the shape changed,
 * the fork needs a corresponding update.
 */

import { describe, it, expect } from 'vitest';
import { buildRuntimeConfig } from '../index.mjs';

/**
 * Build a realistic voice_agents row. Every field gets an explicit value
 * so we test the happy path fully; individual tests override specific
 * fields to exercise defaults and edge cases.
 */
function makeRow(overrides = {}) {
  return {
    id: '11111111-2222-3333-4444-555555555555',
    name: 'chris-claim-status',
    display_name: 'Chris — claim status',
    description: 'Outbound claim status inquiries',
    llm_provider: 'anthropic',
    llm_model: 'claude-sonnet-4-6',
    // pg returns NUMERIC as string — tests MUST use strings here to catch
    // the coercion bug (pg default parses numeric columns to "0.25" not
    // 0.25). Tests that forget this won't surface the regression.
    temperature: '0.25',
    max_tokens: 200,
    enable_prompt_caching: true,
    tts_provider: 'elevenlabs',
    tts_voice_id: 'vW1NxlzqX8WROgpQAghR',
    tts_model: 'eleven_flash_v2_5',
    tts_stability: '0.75',
    tts_similarity_boost: '0.80',
    tts_style: '0.00',
    tts_use_speaker_boost: false,
    tts_speed: '1.10',
    stt_provider: 'deepgram',
    stt_language: 'en',
    stt_keywords: ['NPI', 'EOB', 'Tax ID'],
    tools: [
      { type: 'end_call', description: 'hang up', settings: {} },
      { type: 'press_digit', settings: { pace_ms: 150 } },
      { type: 'transfer_call', settings: { targets: { billing: '+15551234567' } } },
    ],
    system_prompt: 'You are Chris, calling about a claim.',
    first_message: 'Hi, this is Chris.',
    ivr_goal: 'Navigate to claims department.',
    recording_enabled: true,
    recording_channels: 2,
    post_call_analyses: {
      model: 'claude-haiku-4-5-20251001',
      fields: [
        { name: 'disposition', type: 'selector', choices: ['paid', 'promise', 'refuse'] },
        { name: 'notes', type: 'text' },
      ],
    },
    idle_timeout_secs: 30,
    max_call_duration_secs: 1800,
    voicemail_action: 'leave_message',
    is_active: true,
    updated_at: '2026-04-23T22:52:22.369Z',
    ...overrides,
  };
}

describe('buildRuntimeConfig — happy path', () => {
  it('produces the full OG-shape AgentConfig for a Chris row', () => {
    const cfg = buildRuntimeConfig(makeRow());
    expect(cfg).toMatchObject({
      name: 'chris-claim-status',
      display_name: 'Chris — claim status',
      description: 'Outbound claim status inquiries',
      system_prompt: 'You are Chris, calling about a claim.',
      first_message: 'Hi, this is Chris.',
      ivr_goal: 'Navigate to claims department.',
      llm: {
        provider: 'anthropic',
        model: 'claude-sonnet-4-6',
        max_tokens: 200,
        temperature: 0.25,
        enable_prompt_caching: true,
      },
      tts: {
        provider: 'elevenlabs',
        voice_id: 'vW1NxlzqX8WROgpQAghR',
        model: 'eleven_flash_v2_5',
        settings: {
          stability: 0.75,
          similarity_boost: 0.8,
          style: 0,
          use_speaker_boost: false,
          speed: 1.1,
        },
      },
      stt: {
        provider: 'deepgram',
        language: 'en',
        keywords: ['NPI', 'EOB', 'Tax ID'],
      },
      recording: { enabled: true, channels: 2 },
      _meta: {
        agent_id: '11111111-2222-3333-4444-555555555555',
      },
    });
  });

  it('preserves tools array shape exactly (type, description, settings)', () => {
    const cfg = buildRuntimeConfig(makeRow());
    expect(cfg.tools).toEqual([
      { type: 'end_call', description: 'hang up', settings: {} },
      { type: 'press_digit', description: '', settings: { pace_ms: 150 } },
      {
        type: 'transfer_call',
        description: '',
        settings: { targets: { billing: '+15551234567' } },
      },
    ]);
  });

  it('includes post_call_analyses with fields normalized', () => {
    const cfg = buildRuntimeConfig(makeRow());
    expect(cfg.post_call_analyses).toEqual({
      model: 'claude-haiku-4-5-20251001',
      fields: [
        {
          name: 'disposition',
          type: 'selector',
          description: '',
          format_examples: [],
          choices: ['paid', 'promise', 'refuse'],
        },
        {
          name: 'notes',
          type: 'text',
          description: '',
          format_examples: [],
          choices: [],
        },
      ],
    });
  });

  it('exposes updated_at as unix millis in _meta.version', () => {
    const cfg = buildRuntimeConfig(makeRow({ updated_at: '2026-04-23T22:52:22.369Z' }));
    expect(cfg._meta.version).toBe(new Date('2026-04-23T22:52:22.369Z').getTime());
  });
});

describe('buildRuntimeConfig — NUMERIC-as-string coercion', () => {
  it('converts pg string numerics to floats for every numeric field', () => {
    const row = makeRow({
      temperature: '0.9',
      tts_stability: '0.5',
      tts_similarity_boost: '0.6',
      tts_style: '0.1',
      tts_speed: '1.25',
      max_tokens: '500',
      recording_channels: '1',
    });
    const cfg = buildRuntimeConfig(row);
    expect(cfg.llm.temperature).toBe(0.9);
    expect(cfg.llm.max_tokens).toBe(500);
    expect(cfg.tts.settings.stability).toBe(0.5);
    expect(cfg.tts.settings.similarity_boost).toBe(0.6);
    expect(cfg.tts.settings.style).toBe(0.1);
    expect(cfg.tts.settings.speed).toBe(1.25);
    expect(cfg.recording.channels).toBe(1);
  });

  it('accepts real numbers too (defensive)', () => {
    const row = makeRow({ temperature: 0.2, max_tokens: 390 });
    const cfg = buildRuntimeConfig(row);
    expect(cfg.llm.temperature).toBe(0.2);
    expect(cfg.llm.max_tokens).toBe(390);
  });

  it('falls back to sane defaults when numeric fields are null', () => {
    const row = makeRow({
      temperature: null,
      max_tokens: null,
      recording_channels: null,
      tts_stability: null,
    });
    const cfg = buildRuntimeConfig(row);
    expect(cfg.llm.temperature).toBe(0.7);
    expect(cfg.llm.max_tokens).toBe(200);
    expect(cfg.recording.channels).toBe(2);
    expect(cfg.tts.settings.stability).toBeNull();
  });
});

describe('buildRuntimeConfig — empty / missing fields fall back correctly', () => {
  it('handles empty system_prompt + first_message + ivr_goal as empty string', () => {
    const cfg = buildRuntimeConfig(
      makeRow({ system_prompt: null, first_message: null, ivr_goal: null })
    );
    expect(cfg.system_prompt).toBe('');
    expect(cfg.first_message).toBe('');
    expect(cfg.ivr_goal).toBe('');
  });

  it('defaults display_name to name when display_name empty', () => {
    const cfg = buildRuntimeConfig(
      makeRow({ display_name: '', name: 'chris-claim-status' })
    );
    expect(cfg.display_name).toBe('chris-claim-status');
  });

  it('uses anthropic/claude-sonnet-4-6 fallback when llm_provider/model missing', () => {
    const cfg = buildRuntimeConfig(makeRow({ llm_provider: null, llm_model: null }));
    expect(cfg.llm.provider).toBe('anthropic');
    expect(cfg.llm.model).toBe('claude-sonnet-4-6');
  });

  it('uses elevenlabs/turbo fallback when tts_provider/model missing', () => {
    const cfg = buildRuntimeConfig(makeRow({ tts_provider: null, tts_model: null }));
    expect(cfg.tts.provider).toBe('elevenlabs');
    expect(cfg.tts.model).toBe('eleven_turbo_v2_5');
  });

  it('uses deepgram/en fallback when stt_provider/language missing', () => {
    const cfg = buildRuntimeConfig(makeRow({ stt_provider: null, stt_language: null }));
    expect(cfg.stt.provider).toBe('deepgram');
    expect(cfg.stt.language).toBe('en');
  });

  it('treats unset stt_keywords as empty array (not null)', () => {
    const cfg = buildRuntimeConfig(makeRow({ stt_keywords: null }));
    expect(cfg.stt.keywords).toEqual([]);
  });

  it('defaults recording.enabled to true even when column missing', () => {
    const cfg = buildRuntimeConfig(makeRow({ recording_enabled: undefined }));
    expect(cfg.recording.enabled).toBe(true);
  });

  it('recording.enabled = false when explicitly disabled', () => {
    const cfg = buildRuntimeConfig(makeRow({ recording_enabled: false }));
    expect(cfg.recording.enabled).toBe(false);
  });
});

describe('buildRuntimeConfig — use_speaker_boost nullability', () => {
  it('preserves null distinct from false (fork differentiates)', () => {
    const cfg = buildRuntimeConfig(makeRow({ tts_use_speaker_boost: null }));
    expect(cfg.tts.settings.use_speaker_boost).toBeNull();
  });

  it('preserves undefined as null', () => {
    const cfg = buildRuntimeConfig(makeRow({ tts_use_speaker_boost: undefined }));
    expect(cfg.tts.settings.use_speaker_boost).toBeNull();
  });

  it('returns true / false when explicitly set', () => {
    expect(buildRuntimeConfig(makeRow({ tts_use_speaker_boost: true })).tts.settings.use_speaker_boost).toBe(true);
    expect(buildRuntimeConfig(makeRow({ tts_use_speaker_boost: false })).tts.settings.use_speaker_boost).toBe(false);
  });
});

describe('buildRuntimeConfig — tools + post_call_analyses edge cases', () => {
  it('filters out malformed tools (missing type, non-objects)', () => {
    const cfg = buildRuntimeConfig(
      makeRow({
        tools: [
          { type: 'end_call' },
          {}, // no type — should drop
          null, // not an object — should drop
          { description: 'no type here' }, // no type — should drop
          { type: 'press_digit', settings: null }, // null settings → {} default
        ],
      })
    );
    expect(cfg.tools).toEqual([
      { type: 'end_call', description: '', settings: {} },
      { type: 'press_digit', description: '', settings: {} },
    ]);
  });

  it('returns empty tools array when column is null or non-array', () => {
    expect(buildRuntimeConfig(makeRow({ tools: null })).tools).toEqual([]);
    expect(buildRuntimeConfig(makeRow({ tools: 'not an array' })).tools).toEqual([]);
  });

  it('post_call_analyses is null when fields missing', () => {
    expect(
      buildRuntimeConfig(makeRow({ post_call_analyses: null })).post_call_analyses
    ).toBeNull();
    expect(
      buildRuntimeConfig(makeRow({ post_call_analyses: {} })).post_call_analyses
    ).toBeNull();
    expect(
      buildRuntimeConfig(makeRow({ post_call_analyses: { model: 'x', fields: null } }))
        .post_call_analyses
    ).toBeNull();
  });

  it('post_call_analyses normalizes field shape (fills format_examples + choices)', () => {
    const cfg = buildRuntimeConfig(
      makeRow({
        post_call_analyses: {
          fields: [{ name: 'notes' }], // only name — rest should default
        },
      })
    );
    expect(cfg.post_call_analyses).toEqual({
      model: 'claude-haiku-4-5-20251001',
      fields: [
        {
          name: 'notes',
          type: 'text',
          description: '',
          format_examples: [],
          choices: [],
        },
      ],
    });
  });
});

describe('buildRuntimeConfig — _meta', () => {
  it('version is 0 when updated_at missing (new agent, never modified)', () => {
    const cfg = buildRuntimeConfig(makeRow({ updated_at: null }));
    expect(cfg._meta.version).toBe(0);
  });

  it('agent_id is the row UUID (passed through verbatim)', () => {
    const cfg = buildRuntimeConfig(makeRow({ id: 'abc-123' }));
    expect(cfg._meta.agent_id).toBe('abc-123');
  });
});
