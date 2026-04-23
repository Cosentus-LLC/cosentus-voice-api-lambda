# `medcloud-voice-api` — Lambda source

The Aurora-backed REST API powering the Cosentus voice platform
(agents, drafts, batches, calls, recordings). Deployed as a single
Node 22 AWS Lambda named `medcloud-voice-api`.

This repo is the **authoritative source** for that Lambda. Prior to
2026-04-23 the Lambda had no source repo that anyone could find, so
changes were deployed out-of-band and tracked as `.patch` files in
the voice-engine repo (`docs/lambda-patches/`, now archived). That
workflow is done — every future change ships via PR → CI → merge →
`scripts/deploy.sh`.

## TL;DR

```bash
# First time
nvm use                          # picks up Node 22 from .nvmrc
npm ci                           # install deps incl. devDeps for tests

# Daily loop
npm test                         # 55 unit tests, 300ms

# Deploy to dev (default — iterate freely, no prompt)
./scripts/deploy.sh

# Deploy to prod (explicit — requires typing "deploy prod" to confirm)
./scripts/deploy.sh --prod

# Snapshot what's actually live (drift check, stage-aware)
./scripts/pull-live.sh           # dev by default
LAMBDA_FUNCTION_NAME=medcloud-voice-api ./scripts/pull-live.sh   # prod

# Emergency rollback — deploy.sh prints the exact command after each
# deploy; pattern is:
aws lambda update-alias \
    --function-name medcloud-voice-api       # or -dev \
    --name live \
    --function-version <previous-version-number>
```

## Repo layout

```
.
├── .github/workflows/ci.yml       # npm test on every PR + push to main
├── infrastructure/alarms.yml      # CloudFormation for CloudWatch alarms
├── scripts/
│   ├── deploy.sh                  # build + push + version + alias
│   └── pull-live.sh               # download live zip, diff against HEAD
├── test/
│   ├── validators.test.mjs        # 43 tests
│   └── draft-save.test.mjs        # 12 tests
├── CHANGELOG.md                   # one entry per deploy
├── README.md                      # this file
├── index.mjs                      # the whole Lambda (~1757 lines)
├── package.json                   # Node 22, pg runtime, vitest dev
└── package-lock.json              # committed for reproducible installs
```

Intentionally flat: `index.mjs` sits at the repo root and at the zip
root because Lambda's handler config `index.handler` resolves relative
to the zip root. Any subdirectory structure would require reconfiguring
the handler, which we've avoided to match what's deployed.

## What the Lambda does

Handles the REST surface the frontend (`voice-agents-front`) and the
Python voice engine (`voiceagent`) call into. Routes at time of seed:

```
GET/POST     /api/agents               — List/create agents
GET/PUT/DEL  /api/agents/:name         — Agent CRUD
POST         /api/agents/:name/clone   — Clone agent
GET/PUT      /api/agents/:name/prompt  — System prompt
GET          /api/agent-schema         — Config schema (models, voices, etc.)
GET/POST/PUT /api/agent-drafts/:id     — Draft management
GET/POST     /api/agent-versions/:id   — Version history
GET          /api/calls                — Call history (paginated)
GET          /api/calls/:id            — Call detail
GET          /api/calls/:id/recording-url
GET          /api/calls/agents         — Distinct agent names
GET          /api/batches              — List batches
GET          /api/batches/:id          — Batch detail
POST         /api/batches/upload       — Upload CSV
PUT          /api/batches/:id/rows     — Update mapped rows
POST         /api/batches/:id/{start|pause|resume|cancel}
GET          /api/batches/:id/status   — Batch progress
GET          /api/batches/:id/download-url
DELETE       /api/batches/:id/draft
GET          /api/phone-numbers
```

See `index.mjs` for the authoritative list — the routes evolve.

## Deployed runtime (resolved 2026-04-23)

All captured via `aws lambda get-function-configuration --function-name medcloud-voice-api`.

| Setting | Value | Notes |
|---------|-------|-------|
| Runtime | `nodejs22.x` | Matches `.nvmrc` |
| Handler | `index.handler` | Matches `index.mjs` export |
| Memory | 512 MB | |
| Timeout | 120 s | Generous for an API Lambda — most routes complete <1s |
| Architecture | `x86_64` | |
| Reserved concurrency | unset | Shares the 1000 account-wide Lambda concurrency pool |
| Provisioned concurrency | unset | Cold-starts on first call after idle; see TECH-DEBT §4.2 |
| Build step | none | Zip ships `index.mjs` + `node_modules` verbatim |
| Zip size (deployed 2026-04-20) | 168 KB | Tiny — `pg` is the only runtime dep |
| Zip size (this repo, first deploy) | ~215 KB | Slightly bigger because of the export/refactor additions |
| VPC | `vpc-0d1d5235527c7e613` | Shared medcloud VPC, not the voice-engine one |
| Subnets | `subnet-0ca6883e2bd255a95`, `subnet-03211d6dcea8ecdcb` | Private, with NAT |
| Security group | `sg-0fa0c00f028a5a13a` | |
| Separate dev/staging Lambda? | `medcloud-voice-api-dev` exists | Clone of prod config; same Aurora DB for now — see Known Issue #3 / GH issue #7. |

## Environment variables

As of 2026-04-23 the Lambda has these env vars set directly (not via
Secrets Manager):

```
DB_HOST              medcloud-db.cc3aesquitw5.us-east-1.rds.amazonaws.com
DB_NAME              medcloud
DB_USER              medcloud_admin
DB_PASS              <plaintext — see Known Issues>
DB_SSL_STRICT        false
S3_BUCKET            medcloud-voice-us-prod-825
COGNITO_USER_POOL_ID us-east-1_OoYpBUHTa
COGNITO_CLIENT_ID    3gq5lg7igiilvmg66pj7t8lp7l
```

The Aurora connection pool (`index.mjs:61`):

```js
new Pool({
  host, database, user, password,
  port: 5432, max: 5,
  idleTimeoutMillis: 60000,
  ssl: host ? { rejectUnauthorized: DB_SSL_STRICT !== 'false' } : false,
})
```

`max: 5` per container is fine for the current traffic profile but is
on the low end if concurrent invocations scale up. Observable via the
`medcloud-voice-api-throttles` CloudWatch alarm.

## IAM role

Execution role: `arn:aws:iam::825269749545:role/medcloud-lambda-role`.

This is a **shared role** across *all* medcloud Lambda functions
(payments-api, claims-api, coding-api, etc.). Attached policies:

- `AWSLambdaBasicExecutionRole`     — CloudWatch Logs
- `AWSLambdaVPCAccessExecutionRole` — ENI create/delete for VPC mode
- `AmazonS3FullAccess`              — overscoped, see Known Issues
- `AmazonDynamoDBFullAccess`        — unused by voice-api; overscoped
- `AmazonBedrockFullAccess`         — unused by voice-api
- `AmazonTranscribeFullAccess`      — unused by voice-api
- `AmazonCognitoPowerUser`          — used for auth
- `MedCloudLambdaTextractPolicy-prod` — unused by voice-api

Inline policies:

- `ComprehendMedicalAccess` — unused by voice-api
- `LambdaSelfInvoke`        — allows the function to invoke itself
- `VoiceCallQueueAccess`    — `sqs:SendMessage` on `arn:aws:sqs:us-east-1:825269749545:medcloud-voice-call-requests`

Voice-api only *needs*: CloudWatch Logs + VPC ENI + the SQS
permission + Cognito (for token validation if any). Everything else
is legacy or cross-cutting. Narrowing this requires either forking the
role (preferred but touches every other medcloud Lambda's deploy) or
documenting the voice-api-specific subset for the eventual future
migration — tracked as a separate TECH-DEBT item.

## Secrets

**None** in AWS Secrets Manager. Every secret (including the Aurora
DB password) lives in the Lambda's plaintext environment variables.
See Known Issues below.

## Deploy workflow

1. Create a branch, make your changes.
2. `npm test` locally.
3. Open a PR. GitHub Actions runs `npm test` + syntax check +
   production-install smoke test.
4. Merge to `main` once CI is green.
5. On a clean checkout of main:
   - **Dev first** (default): `./scripts/deploy.sh` — deploys to
     `medcloud-voice-api-dev`, no prompt. Invoke
     `FunctionName=medcloud-voice-api-dev:live` to smoke-test.
   - **Promote to prod** once dev looks good:
     `./scripts/deploy.sh --prod`. Script prompts for a typed
     "deploy prod" confirmation before doing anything destructive.
6. deploy.sh prints the new version number + a rollback command
   pointing at the previous alias target. Save the rollback command
   where on-call can find it.

### Stage targets

| Flag | Function name | Prompt? | Who invokes it |
|------|---------------|---------|----------------|
| _(none)_ | `medcloud-voice-api-dev` | no | you, ad-hoc smoke tests, future CI deploy-to-dev |
| `--dev` | `medcloud-voice-api-dev` | no | explicit form of above |
| `--prod` | `medcloud-voice-api` | yes, typed `deploy prod` | only after dev verification |
| `--prod --force` | `medcloud-voice-api` | no | emergency-only, skips clean-tree + prompt |

The `LAMBDA_FUNCTION_NAME` env var overrides both (reserved for
future ad-hoc sandbox Lambdas).

### Versioning + alias semantics

- `$LATEST` always reflects the most recent `update-function-code`.
  Callers that invoke the Lambda unqualified (today: API Gateway
  integration + Python `boto3.invoke(FunctionName='medcloud-voice-api')`)
  follow `$LATEST` and pick up the new code immediately.

- `live` alias points at the currently-blessed immutable version
  (V1, V2, V3, …). Callers that invoke with
  `FunctionName=medcloud-voice-api:live` follow the alias and only
  move forward when the alias cuts over.

- To enable atomic rollback for callers, migrate them to the alias:
  - Python: change `FunctionName='medcloud-voice-api'` →
    `FunctionName='medcloud-voice-api:live'` in `core/lambda_client.py`.
  - API Gateway: update the Lambda integration to target the
    `live` alias in the API Gateway console.
  - Tracked as a follow-up issue; not required for the repo to work.

### Rollback

`deploy.sh` prints the exact rollback command at the end of every
successful deploy. Save it somewhere visible for on-call. Pattern:

```bash
aws lambda update-alias \
    --function-name medcloud-voice-api \
    --name live \
    --function-version <previous-version-number> \
    --region us-east-1
```

Only alias-qualified callers flip instantly. Callers still on
`$LATEST` need a redeploy of the previous version — still faster
than the old paper-trail re-edit workflow, because you have
`aws lambda get-function --qualifier <prev-version>` as a source of
truth for what to re-push.

## Deploying the CloudWatch alarms

One-time setup per account:

```bash
aws cloudformation deploy \
    --stack-name medcloud-voice-api-alarms \
    --template-file infrastructure/alarms.yml \
    --region us-east-1 \
    --parameter-overrides \
        AlertTopicArn=arn:aws:sns:us-east-1:825269749545:<existing-sns-topic>
```

Defaults: 1 error / 5 min, p95 Duration > 2000 ms over 15 min,
1 throttle / 5 min. Tune in the template parameters once you have
baseline telemetry.

## Known issues

### 1. Aurora DB password is stored in plaintext env vars

`DB_PASS` is a plaintext Lambda environment variable. Anyone with
`lambda:GetFunctionConfiguration` on this function can read the live
production DB password.

**Fix**: move to AWS Secrets Manager. The Lambda already has the
execution role attached to a VPC that can reach Secrets Manager via
VPC endpoint (the medcloud VPC has one configured — verified during
repo seeding). Code change is ~15 lines at module top: fetch the
secret once on cold start, cache it, use it in `new Pool(...)`.
Budget ~1 hour including testing.

### 2. IAM role is shared and overscoped

`medcloud-lambda-role` grants `AmazonS3FullAccess`,
`AmazonDynamoDBFullAccess`, `AmazonBedrockFullAccess`, and several
other full-access policies the voice-api never uses. Voice-api only
needs CloudWatch Logs + VPC ENI + one SQS queue + optional Cognito.

**Fix**: create `medcloud-voice-api-role` with a narrow policy set,
swap via `aws lambda update-function-configuration`. Affects *only*
this Lambda — other medcloud Lambdas keep using the shared role.

### 3. Dev Lambda exists but shares the production Aurora DB

Resolved partially: `medcloud-voice-api-dev` now exists. `deploy.sh`
defaults to dev, `--prod` is explicit with confirmation prompt. See
"Deploy workflow" below.

**Remaining gap**: the dev Lambda currently points at the SAME
Aurora DB as prod (host/db/credentials all cloned). Writes from dev
hit production tables. Until we fix this, dev deploys are safe for
structural testing (API shape, validators, /api/health) but NOT for
anything that inserts or mutates rows — use a throwaway agent-name
pattern and clean up after if you need data-path smoke tests.

Tracked as GitHub issue #7: "Isolate dev Lambda's Aurora DB from
prod."

### 4. Bigger refactors tracked as GitHub issues

The following items from `TECH-DEBT.md` (over in the voice-engine
repo until it's migrated here) are intentionally not addressed in
this repo-creation pass. Each has a GitHub issue; none block day-1
use:

- API redesign (POST/PUT duplication, NUMERIC-as-string coercion,
  `jsonb` vs `text[]` consistency, OpenAPI generation, unifying
  direct-invoke and API-Gateway entry points).
- Schema consolidation (enum lists triplicated across Python /
  Lambda / frontend, defaults-as-implicit-migration pattern,
  `recording_enabled` redundancy, `max_tokens` CHECK workaround,
  replacing hand-rolled `validateAgentData` with zod/Joi).
- Client coupling (stabilize Lambda return shape so
  `core/lambda_client.py` in voice-engine can drop its defensive
  `float()` casts and `not result` guards).

## Historical patches (applied in the seed)

Both of the out-of-band patches deployed before this repo existed
are **already part of the initial seed commit**. They are not
separate commits because the seed reflects the live deployed state
as of 2026-04-20. The original patch artifacts live in
[voice-engine: `docs/lambda-patches/`](https://github.com/akcosentus/voice-agent/tree/main/docs/lambda-patches)
and will be cleaned up once this repo is cutover.

Test coverage locks their behavior in:

- `test/validators.test.mjs` has explicit "Fish-retirement
  regression" assertion (`VALID_TTS_PROVIDERS` must not contain
  `fish`).
- `test/draft-save.test.mjs` covers the merged-state validation
  logic that patch P2 introduced, including the exact bug class P2
  was designed to prevent.

See `CHANGELOG.md` for the full patch history.

## Contact

Repo owner + on-call: Alex Kashkarian.
Original Lambda deployed by Lakshay (medcloud platform team),
source repo location confirmed non-existent 2026-04-23.
