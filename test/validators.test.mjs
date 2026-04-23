/**
 * Unit tests for validateAgentData.
 *
 * This is the single biggest bug surface in the Lambda (2-of-2 out-of-band
 * patches we deployed in March-April 2026 touched it). Every field this
 * function validates gets at least one happy-path and one rejection test
 * here, plus edge cases where we've actually been burned.
 */

import { describe, it, expect } from 'vitest';
import {
  validateAgentData,
  FIELD_RANGES,
  VALID_LLM_MODELS,
  VALID_TTS_PROVIDERS,
  TTS_MODELS_BY_PROVIDER,
  VALID_STT_PROVIDERS,
  VALID_STT_LANGUAGES,
  VALID_TOOL_TYPES,
  VALID_VOICEMAIL_ACTIONS,
  VALID_POST_CALL_FIELD_TYPES,
} from '../index.mjs';

describe('validateAgentData — happy paths', () => {
  it('accepts an empty object (partial patch)', () => {
    // Important because the UI sends partial patches constantly.
    expect(validateAgentData({})).toEqual([]);
  });

  it('accepts a fully-populated valid agent', () => {
    expect(
      validateAgentData({
        name: 'chris-claim-status',
        display_name: 'Chris — claim status',
        system_prompt: 'You are a helpful assistant.',
        first_message: 'Hi there!',
        llm_model: VALID_LLM_MODELS[0],
        temperature: 0.2,
        max_tokens: 390,
        tts_provider: 'elevenlabs',
        tts_model: 'eleven_flash_v2_5',
        tts_voice_id: 'vW1NxlzqX8WROgpQAghR',
        tts_stability: 0.75,
        tts_similarity_boost: 0.8,
        tts_style: 0,
        tts_speed: 1.0,
        stt_provider: 'deepgram',
        stt_language: 'en',
        voicemail_action: 'leave_message',
        recording_channels: 2,
        idle_timeout_secs: 30,
        max_call_duration_secs: 1800,
        max_retries: 3,
      }),
    ).toEqual([]);
  });
});

describe('validateAgentData — numeric ranges (FIELD_RANGES)', () => {
  it('rejects temperature above its max', () => {
    const errors = validateAgentData({ temperature: 1.5 });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/temperature must be between 0 and 1/);
  });

  it('rejects max_tokens above its max (regression for the CHECK-constraint workaround)', () => {
    // FIELD_RANGES sets max_tokens.max = 1000, but the DB CHECK is tighter
    // (observed to be 400-ish). Either way, 50,000 is rejected by the range.
    const errors = validateAgentData({ max_tokens: 50000 });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/max_tokens must be between 50 and 1000/);
  });

  it('accepts NUMERIC values that arrive as strings (pg returns strings)', () => {
    // This is the NUMERIC-as-string issue from TECH-DEBT §2.2. Validator
    // uses parseFloat so strings still work — but client code downstream
    // needs to keep casting.
    expect(validateAgentData({ temperature: '0.2' })).toEqual([]);
    expect(validateAgentData({ tts_stability: '0.75' })).toEqual([]);
  });

  it('rejects NaN for numeric fields', () => {
    const errors = validateAgentData({ temperature: 'not-a-number' });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/temperature must be a number/);
  });

  it('allows undefined / null for numeric fields (partial patches)', () => {
    expect(validateAgentData({ temperature: undefined })).toEqual([]);
    expect(validateAgentData({ temperature: null })).toEqual([]);
  });

  it('validates every field declared in FIELD_RANGES', () => {
    // Regression guard: if someone adds a field to FIELD_RANGES but
    // forgets to hook it into validateAgentData (it's already iterated
    // automatically, so this is a structural check) — or accidentally
    // renames the field in one place but not the other.
    const knownFields = Object.keys(FIELD_RANGES);
    for (const field of knownFields) {
      const range = FIELD_RANGES[field];
      const tooHigh = range.max + (range.step || 1) * 100;
      const errors = validateAgentData({ [field]: tooHigh });
      expect(errors.length).toBeGreaterThanOrEqual(1);
      expect(errors.some((e) => e.includes(field))).toBe(true);
    }
  });
});

describe('validateAgentData — name format (AGENT_NAME_RE)', () => {
  it('accepts valid hyphenated lowercase name', () => {
    expect(validateAgentData({ name: 'chris-claim-status' })).toEqual([]);
  });

  it('accepts a single-word name', () => {
    expect(validateAgentData({ name: 'chris' })).toEqual([]);
  });

  it('rejects pre-migration slash-delimited name (chris/claim_status)', () => {
    // This exact format was used pre-Aurora. Leaving it through would
    // break the ECS routing.
    const errors = validateAgentData({ name: 'chris/claim_status' });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/lowercase alphanumeric with hyphens/);
  });

  it('rejects uppercase characters', () => {
    const errors = validateAgentData({ name: 'Chris-Claim-Status' });
    expect(errors).toHaveLength(1);
  });

  it('rejects name starting with hyphen', () => {
    const errors = validateAgentData({ name: '-leading-hyphen' });
    expect(errors).toHaveLength(1);
  });
});

describe('validateAgentData — LLM model', () => {
  it('accepts a model in VALID_LLM_MODELS', () => {
    expect(validateAgentData({ llm_model: VALID_LLM_MODELS[0] })).toEqual([]);
  });

  it('rejects an unknown model', () => {
    const errors = validateAgentData({ llm_model: 'gpt-4' });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/Invalid llm_model: gpt-4/);
  });
});

describe('validateAgentData — TTS provider + model (P1 regression)', () => {
  it('accepts elevenlabs provider', () => {
    expect(validateAgentData({ tts_provider: 'elevenlabs' })).toEqual([]);
  });

  it('rejects fish provider (retired 2026-04-20 via patch P1)', () => {
    // If this test starts failing it means someone re-added 'fish' to
    // VALID_TTS_PROVIDERS. Check TECH-DEBT.md §3 / CHANGELOG.md before
    // changing the assertion.
    const errors = validateAgentData({ tts_provider: 'fish' });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/Invalid tts_provider: fish/);
    expect(VALID_TTS_PROVIDERS).not.toContain('fish');
    expect(Object.keys(TTS_MODELS_BY_PROVIDER)).not.toContain('fish');
  });

  it('rejects tts_model when combined with a provider that does not support it', () => {
    const errors = validateAgentData({
      tts_provider: 'elevenlabs',
      tts_model: 's2-pro', // Fish model, not ElevenLabs
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/Invalid tts_model: s2-pro for provider elevenlabs/);
  });

  it('accepts tts_model on its own using the flat model list', () => {
    // When no provider is given in the incoming patch, we fall back to
    // the flat union of models. This is how the UI's "just update the
    // model" path works — see TECH-DEBT.md §2.1 on the provider +
    // model atomic-write bug that patch P2 hardens against.
    expect(validateAgentData({ tts_model: 'eleven_flash_v2_5' })).toEqual([]);
  });

  it('rejects tts_voice_id that does not match the voice ID regex', () => {
    const errors = validateAgentData({ tts_voice_id: 'short' });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/Invalid tts_voice_id format/);
  });

  it('accepts empty string tts_voice_id (UI clears the field)', () => {
    expect(validateAgentData({ tts_voice_id: '' })).toEqual([]);
  });
});

describe('validateAgentData — STT', () => {
  it('accepts both supported providers', () => {
    expect(validateAgentData({ stt_provider: 'deepgram' })).toEqual([]);
    expect(validateAgentData({ stt_provider: 'elevenlabs' })).toEqual([]);
  });

  it('rejects unknown STT provider', () => {
    const errors = validateAgentData({ stt_provider: 'whisper' });
    expect(errors[0]).toMatch(/Invalid stt_provider: whisper/);
  });

  it('rejects unsupported language', () => {
    const errors = validateAgentData({ stt_language: 'klingon' });
    expect(errors[0]).toMatch(/Invalid stt_language: klingon/);
  });

  it('VALID_STT_LANGUAGES includes at least English', () => {
    expect(VALID_STT_LANGUAGES).toContain('en');
  });
});

describe('validateAgentData — tools', () => {
  it('accepts a valid tools array', () => {
    expect(
      validateAgentData({
        tools: [
          { type: 'end_call' },
          { type: 'press_digit' },
          { type: 'transfer_call', settings: { targets: { billing: '+15551234567' } } },
        ],
      }),
    ).toEqual([]);
  });

  it('rejects tool missing type', () => {
    const errors = validateAgentData({ tools: [{}] });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/tools\[0\]: missing type/);
  });

  it('rejects unknown tool type', () => {
    const errors = validateAgentData({ tools: [{ type: 'send_sms' }] });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/invalid type "send_sms"/);
  });

  it('rejects non-E.164 transfer target', () => {
    const errors = validateAgentData({
      tools: [{ type: 'transfer_call', settings: { targets: { billing: '555-1234' } } }],
    });
    expect(errors.some((e) => /not valid E\.164 format/.test(e))).toBe(true);
  });

  it('accumulates errors from multiple invalid tools', () => {
    const errors = validateAgentData({
      tools: [{ type: 'unknown_a' }, { type: 'unknown_b' }, {}],
    });
    expect(errors).toHaveLength(3);
  });
});

describe('validateAgentData — post_call_analyses', () => {
  it('accepts valid text + selector fields', () => {
    expect(
      validateAgentData({
        post_call_analyses: {
          fields: [
            { name: 'disposition', type: 'selector', choices: ['paid', 'promise', 'refuse'] },
            { name: 'notes', type: 'text' },
          ],
        },
      }),
    ).toEqual([]);
  });

  it('rejects selector with empty choices', () => {
    const errors = validateAgentData({
      post_call_analyses: { fields: [{ name: 'x', type: 'selector', choices: [] }] },
    });
    expect(errors[0]).toMatch(/selector type requires non-empty choices array/);
  });

  it('rejects unknown field type', () => {
    const errors = validateAgentData({
      post_call_analyses: { fields: [{ name: 'x', type: 'boolean' }] },
    });
    expect(errors[0]).toMatch(/invalid type "boolean"/);
  });

  it('rejects field missing name', () => {
    const errors = validateAgentData({
      post_call_analyses: { fields: [{ type: 'text' }] },
    });
    expect(errors[0]).toMatch(/missing name/);
  });
});

describe('validateAgentData — string length limits', () => {
  it('accepts long but under-limit system_prompt', () => {
    expect(validateAgentData({ system_prompt: 'x'.repeat(49_999) })).toEqual([]);
  });

  it('rejects system_prompt > 50,000 chars', () => {
    const errors = validateAgentData({ system_prompt: 'x'.repeat(50_001) });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/system_prompt must be 50,000 characters or fewer/);
  });

  it('rejects first_message > 2,000 chars', () => {
    const errors = validateAgentData({ first_message: 'x'.repeat(2_001) });
    expect(errors).toHaveLength(1);
  });

  it('rejects display_name > 200 chars', () => {
    const errors = validateAgentData({ display_name: 'x'.repeat(201) });
    expect(errors).toHaveLength(1);
  });
});

describe('validateAgentData — voicemail + recording', () => {
  it('accepts valid voicemail_action', () => {
    expect(validateAgentData({ voicemail_action: 'hang_up' })).toEqual([]);
    expect(validateAgentData({ voicemail_action: 'leave_message' })).toEqual([]);
  });

  it('rejects unknown voicemail_action', () => {
    const errors = validateAgentData({ voicemail_action: 'escalate' });
    expect(errors[0]).toMatch(/Invalid voicemail_action: escalate/);
  });

  it('accepts recording_channels 1 or 2 only', () => {
    expect(validateAgentData({ recording_channels: 1 })).toEqual([]);
    expect(validateAgentData({ recording_channels: 2 })).toEqual([]);
  });

  it('rejects recording_channels 0 or 3', () => {
    // 0 is below FIELD_RANGES.recording_channels.min (1) → range error.
    // 3 is above the range max (2) → range error.
    expect(validateAgentData({ recording_channels: 0 })).not.toEqual([]);
    expect(validateAgentData({ recording_channels: 3 })).not.toEqual([]);
  });
});

describe('constant export surface', () => {
  it('exports all the enum lists a test is likely to need', () => {
    expect(Array.isArray(VALID_LLM_MODELS)).toBe(true);
    expect(Array.isArray(VALID_TTS_PROVIDERS)).toBe(true);
    expect(typeof TTS_MODELS_BY_PROVIDER).toBe('object');
    expect(Array.isArray(VALID_STT_PROVIDERS)).toBe(true);
    expect(Array.isArray(VALID_STT_LANGUAGES)).toBe(true);
    expect(Array.isArray(VALID_TOOL_TYPES)).toBe(true);
    expect(Array.isArray(VALID_VOICEMAIL_ACTIONS)).toBe(true);
    expect(Array.isArray(VALID_POST_CALL_FIELD_TYPES)).toBe(true);
  });
});
