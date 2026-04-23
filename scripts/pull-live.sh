#!/bin/bash
# pull-live.sh — download the currently-deployed Lambda zip for drift check.
#
# Use this when you suspect the live Lambda has diverged from the repo HEAD
# (e.g. an out-of-band deploy slipped through, or you're auditing the live
# code for an incident). Writes the unzipped code to /tmp/live-lambda/ and
# prints a diff against repo HEAD for index.mjs.
#
# This is a read-only snapshot — it does NOT modify the repo.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:-medcloud-voice-api}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-default}"
OUT_DIR="${PULL_LIVE_DIR:-/tmp/live-lambda}"

echo "→ Fetching live zip URL for $FUNCTION_NAME..."
CODE_URL=$(aws lambda get-function \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" \
    --query 'Code.Location' --output text)

rm -rf "$OUT_DIR" "$OUT_DIR.zip"
mkdir -p "$OUT_DIR"
curl -sS -o "$OUT_DIR.zip" "$CODE_URL"
unzip -q "$OUT_DIR.zip" -d "$OUT_DIR"

echo "→ Live function configuration:"
aws lambda get-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" \
    --query '{Runtime:Runtime,Version:Version,LastModified:LastModified,CodeSize:CodeSize,Handler:Handler}' \
    --output table

echo
echo "→ Diff between live index.mjs and repo HEAD:"
if diff -q "$OUT_DIR/index.mjs" "$REPO_ROOT/index.mjs" >/dev/null; then
    echo "  ✓ Identical — live matches repo HEAD"
else
    echo "  ! Files differ"
    echo
    diff -u "$OUT_DIR/index.mjs" "$REPO_ROOT/index.mjs" | head -60
    echo
    echo "  Live code: $OUT_DIR/index.mjs"
    echo "  Repo HEAD: $REPO_ROOT/index.mjs"
fi
