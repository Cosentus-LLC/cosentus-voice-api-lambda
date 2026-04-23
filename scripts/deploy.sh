#!/bin/bash
# deploy.sh — build + deploy medcloud-voice-api Lambda with versioning.
#
# Replaces the out-of-band patch workflow that was in use before this
# repo existed. What this script does, in order:
#
#   1. Require the working tree to be clean and on main (unless --force).
#   2. Run tests locally (CI ran them on the PR, but belt-and-suspenders).
#   3. Build a production zip (source + pg node_modules, no devDeps).
#   4. aws lambda update-function-code — updates $LATEST.
#   5. aws lambda publish-version — creates an immutable snapshot Vn.
#   6. aws lambda update-alias --name live --function-version Vn.
#   7. Print rollback command pointing at the previous alias target.
#
# The `live` alias is the rollback target. Callers that invoke the
# Lambda with qualifier `live` (e.g. FunctionName=medcloud-voice-api:live)
# get atomic cutover + instant rollback. Callers invoking unqualified
# (today's Python client + API Gateway) follow $LATEST and get the new
# code immediately on step 4 — unchanged from the old workflow.
#
# Usage:
#   ./scripts/deploy.sh              # deploy to DEV (default target)
#   ./scripts/deploy.sh --prod       # deploy to PROD (with confirmation)
#   ./scripts/deploy.sh --force      # skip tree/branch checks + prod prompt
#   ./scripts/deploy.sh --dry-run    # build zip, list contents, don't deploy
#
# Stages:
#   --dev   (default)   → medcloud-voice-api-dev
#   --prod              → medcloud-voice-api (live traffic)
#
# Override by env var: LAMBDA_FUNCTION_NAME=<name> ./scripts/deploy.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ALIAS_NAME="${LAMBDA_ALIAS_NAME:-live}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-default}"
BUILD_DIR="$REPO_ROOT/.build"
ZIP_PATH="$REPO_ROOT/.build/function.zip"

# Stage-aware function name resolution. Default is `dev` — production
# deploys require an explicit --prod flag so nobody accidentally ships
# to prod. $LAMBDA_FUNCTION_NAME env var overrides both if set (e.g.
# for a future ad-hoc sandbox function).
STAGE_PROD_NAME="medcloud-voice-api"
STAGE_DEV_NAME="medcloud-voice-api-dev"

# Colors (only if stdout is a TTY)
if [ -t 1 ]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; CYAN=''; NC=''
fi

log()  { echo -e "${CYAN}→${NC} $*"; }
ok()   { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}!${NC} $*" >&2; }
err()  { echo -e "${RED}✗${NC} $*" >&2; }

# ====================
# Flags
# ====================
FORCE=false
DRY_RUN=false
STAGE="dev"  # default — --prod to hit production
for arg in "$@"; do
    case "$arg" in
        --force)   FORCE=true ;;
        --dry-run) DRY_RUN=true ;;
        --prod)    STAGE="prod" ;;
        --dev)     STAGE="dev" ;;
        -h|--help)
            sed -n '/^# deploy.sh/,/^$/p' "$0" | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            err "Unknown flag: $arg"
            exit 2
            ;;
    esac
done

# Resolve target function name from stage. LAMBDA_FUNCTION_NAME env var
# wins over stage flags — kept for future ad-hoc sandbox environments.
if [ -n "${LAMBDA_FUNCTION_NAME:-}" ]; then
    FUNCTION_NAME="$LAMBDA_FUNCTION_NAME"
elif [ "$STAGE" = "prod" ]; then
    FUNCTION_NAME="$STAGE_PROD_NAME"
else
    FUNCTION_NAME="$STAGE_DEV_NAME"
fi

# Production deploy requires explicit confirmation (unless --force).
# Dev deploys proceed without prompting — iterate freely.
if [ "$STAGE" = "prod" ] && ! $FORCE && ! $DRY_RUN; then
    echo
    warn "You are about to deploy to PRODUCTION: $FUNCTION_NAME"
    warn "This is the Lambda serving live voice platform traffic."
    echo
    echo "Type 'deploy prod' to proceed, anything else to abort:"
    read -r CONFIRMATION
    if [ "$CONFIRMATION" != "deploy prod" ]; then
        err "Aborted"
        exit 1
    fi
fi

# ====================
# Preflight
# ====================
cd "$REPO_ROOT"

if ! command -v aws >/dev/null; then
    err "aws CLI not found on PATH"
    exit 1
fi

if ! command -v node >/dev/null; then
    err "node not found on PATH"
    exit 1
fi

# Branch + cleanliness check (unless --force)
if ! $FORCE; then
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    if [ "$CURRENT_BRANCH" != "main" ]; then
        err "Must deploy from main (current: $CURRENT_BRANCH). Use --force to override."
        exit 1
    fi
    if [ -n "$(git status --porcelain)" ]; then
        err "Working tree has uncommitted changes. Commit or stash, or use --force."
        git status --short
        exit 1
    fi
fi

# Confirm identity
ACCOUNT=$(aws sts get-caller-identity --query Account --output text --region "$AWS_REGION" --profile "$AWS_PROFILE")
IDENTITY=$(aws sts get-caller-identity --query Arn --output text --region "$AWS_REGION" --profile "$AWS_PROFILE")
log "AWS account: $ACCOUNT"
log "Identity:    $IDENTITY"
log "Function:    $FUNCTION_NAME"
log "Region:      $AWS_REGION"
log "Alias:       $ALIAS_NAME"
echo

# ====================
# Test
# ====================
log "Running tests locally (belt-and-suspenders; CI ran them on the PR)..."
npm test
ok "Tests pass"
echo

# ====================
# Build
# ====================
log "Building production zip..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Fresh install with prod deps only.
STAGE_DIR="$BUILD_DIR/stage"
mkdir -p "$STAGE_DIR"
cp index.mjs package.json package-lock.json "$STAGE_DIR/"

# Copy any additional source directories (e.g. lib/ helpers). Anything that
# index.mjs imports relatively (./lib/foo.mjs, ./helpers/bar.mjs, etc.) must
# live under one of these directories or the Lambda runtime will ERR_MODULE_NOT_FOUND.
# When adding a new source dir, add it here.
for src_dir in lib; do
    if [ -d "$src_dir" ]; then
        cp -R "$src_dir" "$STAGE_DIR/"
    fi
done

(
    cd "$STAGE_DIR"
    npm ci --omit=dev --silent
)

# Zip contents from inside $STAGE_DIR so the archive is flat (index.mjs at root,
# not inside a stage/ subdirectory — Lambda expects Handler.functionName to
# resolve at zip root).
(
    cd "$STAGE_DIR"
    zip -qr "$ZIP_PATH" .
)

ZIP_SIZE=$(stat -f%z "$ZIP_PATH" 2>/dev/null || stat -c%s "$ZIP_PATH")
ZIP_SIZE_MB=$(awk "BEGIN{printf \"%.2f\", $ZIP_SIZE/1024/1024}")
ok "Zip built: $ZIP_PATH ($ZIP_SIZE bytes, ${ZIP_SIZE_MB}MB)"

if $DRY_RUN; then
    log "Dry-run: zip contents preview"
    unzip -l "$ZIP_PATH" | head -20
    echo "..."
    unzip -l "$ZIP_PATH" | tail -5
    log "Dry-run complete — no deploy"
    exit 0
fi

# Hard limit: AWS Lambda zip upload via update-function-code is capped at
# 50 MB (direct) / 250 MB (S3). Today's build is ~170 KB so we're fine,
# but add a guard so a future bloat (e.g. committing node_modules) doesn't
# silently start using the S3 path without noticing.
if [ "$ZIP_SIZE" -gt 52428800 ]; then
    err "Zip size ${ZIP_SIZE} exceeds 50 MB direct-upload limit."
    err "Switch deploy to S3 upload path (aws lambda update-function-code --s3-bucket ...)"
    exit 1
fi

# ====================
# Deploy
# ====================
CURRENT_SHA=$(git rev-parse --short HEAD)
CURRENT_MESSAGE=$(git log -1 --pretty=format:'%s')

log "Deploying zip to $FUNCTION_NAME (\$LATEST)..."
aws lambda update-function-code \
    --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$ZIP_PATH" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --no-publish \
    --output json \
    >/dev/null
ok "\$LATEST updated"

# Wait for the code update to propagate before updating config or
# publishing a version. Without this, subsequent calls race and fail
# with ResourceConflictException.
log "Waiting for code update to propagate..."
aws lambda wait function-updated-v2 \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE"

# Stamp BUILD_SHA into the Lambda's env so /api/health can report which
# commit is live. Preserves all existing env vars — this function is not
# the owner of its env var set (DB_PASS etc. live here too for now; see
# GitHub issue #1). We fetch-then-merge via python3 rather than the aws
# CLI's --environment shorthand because DB values can contain special
# characters that the shorthand parser mishandles.
log "Stamping BUILD_SHA=$CURRENT_SHA into Lambda env..."
ENV_PAYLOAD=$(aws lambda get-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --query 'Environment.Variables' \
    --output json \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" \
    | python3 -c "
import json, sys
current = json.load(sys.stdin) or {}
current['BUILD_SHA'] = '$CURRENT_SHA'
print(json.dumps({'Variables': current}))
")

aws lambda update-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --environment "$ENV_PAYLOAD" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" \
    --output json \
    >/dev/null

log "Waiting for env update to propagate..."
aws lambda wait function-updated-v2 \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE"
ok "BUILD_SHA env var updated"

log "Publishing immutable version..."
NEW_VERSION=$(aws lambda publish-version \
    --function-name "$FUNCTION_NAME" \
    --description "$CURRENT_SHA: $CURRENT_MESSAGE" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --query 'Version' \
    --output text)
ok "Published version $NEW_VERSION"

log "Updating alias '$ALIAS_NAME' → version $NEW_VERSION..."
# Record previous target BEFORE cutover so we can print a ready-to-run
# rollback command.
PREVIOUS_VERSION=$(aws lambda get-alias \
    --function-name "$FUNCTION_NAME" \
    --name "$ALIAS_NAME" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --query 'FunctionVersion' \
    --output text 2>/dev/null || echo "")

if [ -z "$PREVIOUS_VERSION" ]; then
    # First-time alias creation.
    aws lambda create-alias \
        --function-name "$FUNCTION_NAME" \
        --name "$ALIAS_NAME" \
        --function-version "$NEW_VERSION" \
        --description "Production traffic target. Update via deploy.sh; roll back via update-alias." \
        --region "$AWS_REGION" \
        --profile "$AWS_PROFILE" \
        --output json \
        >/dev/null
    ok "Created alias '$ALIAS_NAME' → $NEW_VERSION (first deploy)"
else
    aws lambda update-alias \
        --function-name "$FUNCTION_NAME" \
        --name "$ALIAS_NAME" \
        --function-version "$NEW_VERSION" \
        --region "$AWS_REGION" \
        --profile "$AWS_PROFILE" \
        --output json \
        >/dev/null
    ok "Alias '$ALIAS_NAME' cutover: $PREVIOUS_VERSION → $NEW_VERSION"
fi

echo
echo -e "${GREEN}Deploy complete.${NC}"
echo "  Version:  $NEW_VERSION"
echo "  Git SHA:  $CURRENT_SHA"
echo "  Invoke:   FunctionName=${FUNCTION_NAME}:${ALIAS_NAME} (alias-qualified)"
echo "            FunctionName=${FUNCTION_NAME}              (follows \$LATEST)"
echo
if [ -n "$PREVIOUS_VERSION" ] && [ "$PREVIOUS_VERSION" != "\$LATEST" ]; then
    echo -e "${YELLOW}Rollback command (if needed):${NC}"
    echo "  aws lambda update-alias \\"
    echo "      --function-name $FUNCTION_NAME \\"
    echo "      --name $ALIAS_NAME \\"
    echo "      --function-version $PREVIOUS_VERSION \\"
    echo "      --region $AWS_REGION --profile $AWS_PROFILE"
    echo
    echo "  (Rollback only affects alias-qualified callers. Callers on"
    echo "   \$LATEST stay on the newly-deployed code until you re-push"
    echo "   the previous version via update-function-code.)"
fi
