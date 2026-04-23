/**
 * Unit tests for validateDraftMerged — the merged-state validator used by
 * both POST /api/agent-drafts (upsert) and PUT /api/agent-drafts/:id (update).
 *
 * This is the logic introduced by the 2026-04-20 merged-state validation
 * patch (P2 in the CHANGELOG). The patch was extracted into a pure helper
 * during the repo creation refactor so both handlers share one call site
 * and this test file doesn't need to mock the DB pool.
 */

import { describe, it, expect } from 'vitest';
import { validateDraftMerged, validateAgentData } from '../index.mjs';

describe('validateDraftMerged — merge semantics', () => {
  it('returns same result as validateAgentData when existing row is null', () => {
    const body = { name: 'chris-claim-status', tts_provider: 'elevenlabs' };
    expect(validateDraftMerged(null, body)).toEqual(validateAgentData(body));
  });

  it('accepts undefined existing row the same as null', () => {
    expect(validateDraftMerged(undefined, { name: 'ok' })).toEqual([]);
  });

  it('accepts undefined incoming body (partial patch with nothing)', () => {
    // This is a defensive path — callers shouldn't actually do this but
    // we shouldn't crash if they do.
    expect(validateDraftMerged({ name: 'existing' }, undefined)).toEqual([]);
    expect(validateDraftMerged({ name: 'existing' }, null)).toEqual([]);
  });

  it('incoming fields override existing fields (spread order)', () => {
    const existing = {
      name: 'chris-claim-status',
      tts_provider: 'elevenlabs',
      tts_model: 'eleven_flash_v2_5',
    };
    const incoming = { tts_provider: 'deepgram' }; // invalid — deepgram is STT
    const errors = validateDraftMerged(existing, incoming);
    // The merged state has tts_provider=deepgram (incoming wins)
    expect(errors.some((e) => /Invalid tts_provider: deepgram/.test(e))).toBe(true);
  });

  it('existing fields survive when incoming patch omits them', () => {
    const existing = { tts_provider: 'elevenlabs' };
    const incoming = { tts_model: 'eleven_flash_v2_5' };
    // Merged: tts_provider=elevenlabs + tts_model=eleven_flash_v2_5 → valid
    expect(validateDraftMerged(existing, incoming)).toEqual([]);
  });
});

describe('validateDraftMerged — the bug P2 was designed to prevent', () => {
  it('rejects a draft whose existing provider is incompatible with incoming model', () => {
    // Scenario from the pre-P2 bug: a stored draft was accidentally left
    // with tts_provider="fish" (from the Fish experiment) and the UI
    // subsequently sent a lone {tts_model: "eleven_flash_v2_5"} patch.
    //
    // Pre-P2: would pass (validator only saw the incoming patch), save
    //         would persist an invalid combo, Publish would reject.
    // Post-P2: rejects at save time with the combined-state error.
    const existing = { tts_provider: 'fish' }; // hypothetical — fish is gone but the bug class applies
    const incoming = { tts_model: 'eleven_flash_v2_5' };
    const errors = validateDraftMerged(existing, incoming);

    // Two errors expected:
    //   1. Invalid tts_provider: fish  (the merged state still has fish)
    //   2. Invalid tts_model: eleven_flash_v2_5 for provider fish
    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors.some((e) => /Invalid tts_provider: fish/.test(e))).toBe(true);
  });

  it('accepts the correct atomic write (provider + model together)', () => {
    // What the frontend fix ships today: tts_provider is written
    // atomically with voice_id and model on every TTS edit.
    const existing = { tts_provider: 'fish' }; // stale
    const incoming = {
      tts_provider: 'elevenlabs',
      tts_model: 'eleven_flash_v2_5',
      tts_voice_id: 'vW1NxlzqX8WROgpQAghR',
    };
    expect(validateDraftMerged(existing, incoming)).toEqual([]);
  });

  it('rejects individual-field update that would break existing draft state', () => {
    // Future-proofing: any new provider paired with an incompatible model
    // gets caught at save time, not at publish time.
    const existing = {
      tts_provider: 'elevenlabs',
      tts_model: 'eleven_flash_v2_5',
    };
    const incoming = { tts_model: 'INVALID_MODEL_NAME' };
    const errors = validateDraftMerged(existing, incoming);
    expect(errors.some((e) => /Invalid tts_model/.test(e))).toBe(true);
  });
});

describe('validateDraftMerged — three save-draft paths', () => {
  // The three paths referenced in the repo-creation task:
  //   1. POST /api/agent-drafts — upsert (client-provided agent_id)
  //   2. PUT /api/agent-drafts/:id — update existing
  //   3. POST /api/agents / PUT /api/agents/:name — live agent record
  //      (uses validateAgentData directly, not validateDraftMerged)
  //
  // We cover path 3 indirectly via validators.test.mjs because the live
  // agent handlers call validateAgentData(body) without merging. Here we
  // lock behavior for paths 1 and 2.

  it('path 1: POST /api/agent-drafts — fresh row (no existing)', () => {
    const body = {
      agent_id: 'chris-claim-status',
      name: 'chris-claim-status',
      tts_provider: 'elevenlabs',
      tts_model: 'eleven_flash_v2_5',
    };
    // POST flow: existing is null when agent_id not in DB yet
    expect(validateDraftMerged(null, body)).toEqual([]);
  });

  it('path 1: POST /api/agent-drafts — upsert onto existing row', () => {
    const existing = {
      agent_id: 'chris-claim-status',
      name: 'chris-claim-status',
      tts_provider: 'elevenlabs',
      tts_model: 'eleven_flash_v2_5',
      tts_voice_id: 'vW1NxlzqX8WROgpQAghR',
    };
    const patch = { tts_speed: 1.1 };
    expect(validateDraftMerged(existing, patch)).toEqual([]);
  });

  it('path 2: PUT /api/agent-drafts/:id — partial update lands on existing row', () => {
    const existing = {
      agent_id: 'chris-claim-status',
      name: 'chris-claim-status',
      tts_provider: 'elevenlabs',
      tts_model: 'eleven_flash_v2_5',
    };
    const patch = { first_message: 'Hi, this is Chris.' };
    expect(validateDraftMerged(existing, patch)).toEqual([]);
  });

  it('path 2: PUT /api/agent-drafts/:id — rejects when patch violates range check', () => {
    const existing = { name: 'test', temperature: 0.2 };
    const patch = { temperature: 2.5 }; // > FIELD_RANGES.temperature.max
    const errors = validateDraftMerged(existing, patch);
    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0]).toMatch(/temperature must be between/);
  });
});
