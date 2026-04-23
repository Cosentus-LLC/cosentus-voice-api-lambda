# Changelog

Reverse-chronological. Every deploy gets an entry. Format loosely
follows [keep-a-changelog](https://keepachangelog.com/) but keeps
the focus on *deploy events* since that's the unit of tracking here.

---

## Unreleased

_Nothing deployed yet since the repo was created — this section
accumulates merged PRs until the next deploy._

---

## 1.0.0 — 2026-04-23 (repo creation, no functional change)

**Deployed**: n/a — this is the repo seeding. The deployed Lambda at
`medcloud-voice-api` is unchanged; the repo state now matches it.

### Added

- Git repository `Cosentus-LLC/cosentus-voice-api-lambda` seeded from
  the live Lambda zip pulled on 2026-04-23 via
  `aws lambda get-function --function-name medcloud-voice-api`.
- `package.json` — added `engines.node: "22.x"` (matches deployed
  runtime), `vitest` + `eslint` as devDependencies. Runtime deps
  unchanged (just `pg ^8.13.0`). Deploy zip is byte-equivalent to the
  currently-deployed artifact modulo Node resolver noise.
- `package-lock.json`, `.nvmrc`, `.gitignore`.
- Vitest unit tests for `validateAgentData` + draft-save merged-state
  logic (see `test/`).
- GitHub Actions CI running `npm test` on every PR.
- `scripts/deploy.sh` — zip + `aws lambda update-function-code` +
  `publish-version` + `update-alias live`. Replaces the manual
  paper-trail workflow used from Jan–Apr 2026.
- `scripts/pull-live.sh` — snapshot the deployed zip into a local
  `/tmp/` directory for drift-check against the repo HEAD.
- `infrastructure/alarms.yml` — CloudFormation template for three
  CloudWatch alarms (error rate, p95 duration, throttles).

### Historical — included in the seed, not a change

These were deployed out-of-band while there was no source repo. They
are part of the initial seed, not separate commits. Full patch
artefacts live in the old paper-trail folder at
[voice-engine: `docs/lambda-patches/`](https://github.com/akcosentus/voice-agent/tree/main/docs/lambda-patches)
until that folder is cleaned up.

#### 2026-04-20 21:55 UTC — remove Fish from TTS provider + model validation lists

`VALID_TTS_PROVIDERS` dropped `'fish'`; `TTS_MODELS_BY_PROVIDER` lost
its `fish:` key. Part of the Fish Audio retirement.

Reference: [`2026-04-20-remove-fish-from-tts-validation.patch`](https://github.com/akcosentus/voice-agent/blob/main/docs/lambda-patches/2026-04-20-remove-fish-from-tts-validation.patch)

#### 2026-04-20 21:46 UTC — validate merged draft state on PUT + POST /api/agent-drafts

Both handlers load the existing draft row by `agent_id`, merge the
incoming patch, and validate the merged object. Prevents inconsistent
hybrid states (e.g. `tts_provider="fish"` with an ElevenLabs-only
model) that single-field updates used to allow.

Reference: [`2026-04-20-validate-merged-draft-state.patch`](https://github.com/akcosentus/voice-agent/blob/main/docs/lambda-patches/2026-04-20-validate-merged-draft-state.patch)
