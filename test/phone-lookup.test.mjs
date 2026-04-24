/**
 * Unit tests for the phone-number lookup path (Phase 7B).
 *
 * Tests the pure helper `normalizeE164` exhaustively. The full
 * `GET /api/phone-numbers/lookup` handler is a thin wrapper around
 * this helper + a SQL query; integration tests against Aurora cover
 * the end-to-end path.
 *
 * If normalizeE164 changes behavior here, the bot-runner Lambda's
 * phone-resolver HTTP call breaks — keep this suite strict.
 */

import { describe, it, expect } from 'vitest';
import { normalizeE164 } from '../index.mjs';

describe('normalizeE164', () => {
  describe('happy path — already E.164', () => {
    it('accepts +12098075018 unchanged', () => {
      expect(normalizeE164('+12098075018')).toBe('+12098075018');
    });
    it('accepts short country codes', () => {
      // Ireland mobile, 12 digits including +353
      expect(normalizeE164('+353851234567')).toBe('+353851234567');
    });
    it('accepts min-length E.164 (8 digits total)', () => {
      expect(normalizeE164('+1234567')).toBe('+1234567');
    });
    it('accepts max-length E.164 (15 digits total)', () => {
      expect(normalizeE164('+123456789012345')).toBe('+123456789012345');
    });
  });

  describe('Daily webhook quirk — missing leading +', () => {
    it('adds + prefix to bare digits', () => {
      expect(normalizeE164('12098075018')).toBe('+12098075018');
    });
    it('adds + prefix to bare UK number', () => {
      expect(normalizeE164('447700900123')).toBe('+447700900123');
    });
  });

  describe('whitespace handling', () => {
    it('trims surrounding whitespace', () => {
      expect(normalizeE164('  +12098075018  ')).toBe('+12098075018');
    });
    it('trims surrounding whitespace with leading-+ variant', () => {
      expect(normalizeE164('\t+12098075018\n')).toBe('+12098075018');
    });
  });

  describe('rejects invalid input', () => {
    it('rejects empty string', () => {
      expect(normalizeE164('')).toBeNull();
    });
    it('rejects whitespace-only string', () => {
      expect(normalizeE164('   ')).toBeNull();
    });
    it('rejects null/undefined', () => {
      expect(normalizeE164(null)).toBeNull();
      expect(normalizeE164(undefined)).toBeNull();
    });
    it('rejects non-string types', () => {
      expect(normalizeE164(12098075018)).toBeNull();
      expect(normalizeE164({ number: '+12098075018' })).toBeNull();
    });
    it('rejects leading-zero country code', () => {
      // +0 violates E.164 — country codes start at 1.
      expect(normalizeE164('+02098075018')).toBeNull();
    });
    it('rejects sub-minimum digit count', () => {
      // 7 digits after + = too few.
      expect(normalizeE164('+1234')).toBeNull();
      expect(normalizeE164('+1')).toBeNull();
    });
    it('rejects over-maximum digit count', () => {
      // 16 digits after + = too many (E.164 max is 15).
      expect(normalizeE164('+1234567890123456')).toBeNull();
    });
    it('rejects letters', () => {
      expect(normalizeE164('+1-800-FLOWERS')).toBeNull();
      expect(normalizeE164('call-me')).toBeNull();
    });
    it('rejects formatted numbers with hyphens/parens', () => {
      // These are common in user input but NOT E.164. Caller is
      // expected to clean before sending; we don't silently accept.
      expect(normalizeE164('+1 (209) 807-5018')).toBeNull();
      expect(normalizeE164('1-209-807-5018')).toBeNull();
    });
    it('rejects double +', () => {
      expect(normalizeE164('++12098075018')).toBeNull();
    });
  });

  describe('the specific Daily dev number round-trip', () => {
    // Regression pin — this is the number used for all Phase 7A/B
    // smoke tests. If this test fails, the bot-runner cannot route.
    it('matches Daily dev number both with and without +', () => {
      expect(normalizeE164('+12098075018')).toBe('+12098075018');
      expect(normalizeE164('12098075018')).toBe('+12098075018');
    });
  });
});
