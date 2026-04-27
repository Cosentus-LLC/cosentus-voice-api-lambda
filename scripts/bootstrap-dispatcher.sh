#!/bin/bash
# bootstrap-dispatcher.sh — one-time setup for the SQS-triggered call
# dispatcher Lambda. Phase 7D, voice_call_requests path.
#
# What this script does (idempotent — safe to re-run):
#
#   1. Create / update the IAM role (medcloud-voice-call-dispatcher-role)
#      with policies for: Aurora (via VPC), CloudWatch logs,
#      lambda:InvokeFunction on bot-runner, sqs:ReceiveMessage et al
#      on medcloud-voice-call-requests, and ec2 ENI management for VPC.
#
#   2. Create / update the Lambda function (medcloud-voice-call-dispatcher
#      and -dev). Both functions share the same code; env vars + reserved
#      concurrency are stage-specific.
#
#   3. Set the $live alias on the dev function pointing to $LATEST so
#      ./scripts/deploy.sh --function dispatcher works on first deploy.
#      (The alias gets updated to a real Vn on the first `deploy.sh`.)
#
#   4. Create / update the SQS event source mapping linking the queue
#      to the function with batch_size=1, ReportBatchItemFailures=true,
#      MaximumConcurrency=10.
#
#   5. Drop the queue's visibility timeout from 5400s (90 min, OG default)
#      to 600s (10 min) — fire-and-forget dispatcher doesn't need the
#      90-minute hold OG used for slot-based concurrency.
#
# Required AWS permissions to run:
#   iam:CreateRole, AttachRolePolicy, PutRolePolicy, GetRole
#   lambda:CreateFunction, UpdateFunctionConfiguration, UpdateFunctionCode,
#         PutFunctionConcurrency, PublishVersion, CreateAlias, GetFunction,
#         CreateEventSourceMapping, UpdateEventSourceMapping,
#         ListEventSourceMappings
#   sqs:GetQueueAttributes, SetQueueAttributes
#
# Usage:
#   ./scripts/bootstrap-dispatcher.sh             # bootstrap DEV (default)
#   ./scripts/bootstrap-dispatcher.sh --prod      # bootstrap PROD
#
# After this script runs once, use ./scripts/deploy.sh --function dispatcher
# to ship code changes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-default}"

STAGE="dev"
for arg in "$@"; do
    case "$arg" in
        --prod) STAGE="prod" ;;
        --dev)  STAGE="dev" ;;
        -h|--help)
            sed -n '/^# bootstrap-dispatcher/,/^$/p' "$0" | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            echo "Unknown flag: $arg" >&2
            exit 2
            ;;
    esac
done

if [ "$STAGE" = "prod" ]; then
    FUNCTION_NAME="medcloud-voice-call-dispatcher"
    PARENT_API_FUNCTION="medcloud-voice-api"
    RESERVED_CONCURRENCY=10
else
    FUNCTION_NAME="medcloud-voice-call-dispatcher-dev"
    PARENT_API_FUNCTION="medcloud-voice-api-dev"
    # Lower concurrency on dev — we don't need the full headroom on a
    # function that's only exercised by smoke tests until Daily approves
    # PSTN dial-out. Bump to 10 once we're past the gate.
    RESERVED_CONCURRENCY=2
fi

ROLE_NAME="medcloud-voice-call-dispatcher-role"
QUEUE_NAME="medcloud-voice-call-requests"
QUEUE_ARN="arn:aws:sqs:${AWS_REGION}:825269749545:${QUEUE_NAME}"
QUEUE_URL="https://sqs.${AWS_REGION}.amazonaws.com/825269749545/${QUEUE_NAME}"
DLQ_ARN="arn:aws:sqs:${AWS_REGION}:825269749545:${QUEUE_NAME}-dlq"

if [ -t 1 ]; then
    GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
else
    GREEN=''; YELLOW=''; CYAN=''; NC=''
fi

log()  { echo -e "${CYAN}→${NC} $*"; }
ok()   { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}!${NC} $*" >&2; }

# ────────────────────────────────────────────────────────────────────
# Resolve account + bot-runner ARN

ACCOUNT=$(aws sts get-caller-identity --query Account --output text \
    --region "$AWS_REGION" --profile "$AWS_PROFILE")
log "Account: $ACCOUNT"
log "Stage:   $STAGE"
log "Function: $FUNCTION_NAME"
log "Reserved concurrency: $RESERVED_CONCURRENCY"

BOT_RUNNER_ARN=$(aws lambda list-functions \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" \
    --query 'Functions[?starts_with(FunctionName, `VoiceAgentBotRunner-WebhookApi`)].FunctionArn | [0]' \
    --output text)

if [ -z "$BOT_RUNNER_ARN" ] || [ "$BOT_RUNNER_ARN" = "None" ]; then
    warn "Bot-runner Lambda not found. Deploy VoiceAgentBotRunner stack first."
    exit 1
fi
log "Bot-runner: $BOT_RUNNER_ARN"

# Pull DB + VPC config from the parent API Lambda. Both functions live
# in the same VPC + subnets and share the same Aurora; cloning ensures
# they stay in sync without drift.
log "Reading DB + VPC config from parent ($PARENT_API_FUNCTION)..."
PARENT_CONFIG=$(aws lambda get-function-configuration \
    --function-name "$PARENT_API_FUNCTION" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE")

DB_HOST=$(echo "$PARENT_CONFIG" | python3 -c "import json,sys; print(json.load(sys.stdin)['Environment']['Variables']['DB_HOST'])")
DB_NAME=$(echo "$PARENT_CONFIG" | python3 -c "import json,sys; print(json.load(sys.stdin)['Environment']['Variables']['DB_NAME'])")
DB_USER=$(echo "$PARENT_CONFIG" | python3 -c "import json,sys; print(json.load(sys.stdin)['Environment']['Variables']['DB_USER'])")
DB_PASS=$(echo "$PARENT_CONFIG" | python3 -c "import json,sys; print(json.load(sys.stdin)['Environment']['Variables']['DB_PASS'])")
SUBNET_IDS=$(echo "$PARENT_CONFIG" | python3 -c "import json,sys; print(','.join(json.load(sys.stdin)['VpcConfig']['SubnetIds']))")
SG_IDS=$(echo "$PARENT_CONFIG" | python3 -c "import json,sys; print(','.join(json.load(sys.stdin)['VpcConfig']['SecurityGroupIds']))")
ok "Parent config read"

# ────────────────────────────────────────────────────────────────────
# IAM role

log "Ensuring IAM role $ROLE_NAME exists..."
TRUST_DOC=$(cat <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "lambda.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF
)
if ! aws iam get-role --role-name "$ROLE_NAME" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" >/dev/null 2>&1; then
    log "Creating role..."
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$TRUST_DOC" \
        --description "Phase 7D dispatcher Lambda. Reads SQS, invokes bot-runner, writes Aurora." \
        --region "$AWS_REGION" --profile "$AWS_PROFILE" \
        --output json >/dev/null
    # IAM role propagation is eventually-consistent — wait a few
    # seconds before attaching policies and creating the function.
    sleep 8
    ok "Role created"
else
    ok "Role exists"
fi

# Managed: VPC ENI management for Aurora connectivity. Lambda needs
# this to attach to subnets. Using the AWS-managed policy avoids
# typing all the EC2 actions ourselves.
aws iam attach-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" >/dev/null 2>&1 || true

# Inline policy: SQS read on the call-requests queue + lambda:Invoke
# on bot-runner. Scoped narrow.
INLINE_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadSqsQueue",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:ChangeMessageVisibility"
      ],
      "Resource": "$QUEUE_ARN"
    },
    {
      "Sid": "InvokeBotRunner",
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "$BOT_RUNNER_ARN"
    }
  ]
}
EOF
)
aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "DispatcherInlinePolicy" \
    --policy-document "$INLINE_POLICY" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" >/dev/null
ok "Role policies attached"

ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${ROLE_NAME}"

# ────────────────────────────────────────────────────────────────────
# Lambda function — create or update

# Build a placeholder zip for first-time creation. After creation,
# real code is shipped via ./scripts/deploy.sh --function dispatcher.
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT
cat > "$TMP_DIR/dispatcher.mjs" <<'EOF'
export const handler = async () => ({ batchItemFailures: [] });
EOF
(cd "$TMP_DIR" && zip -q dispatcher-placeholder.zip dispatcher.mjs)

ENV_VARS_JSON=$(python3 -c "
import json, os
env = {
    'BOT_RUNNER_FUNCTION_NAME': '$BOT_RUNNER_ARN',
    'DB_HOST': '$DB_HOST',
    'DB_NAME': '$DB_NAME',
    'DB_USER': '$DB_USER',
    'DB_PASS': '$DB_PASS',
    'DB_SSL': 'true',
}
print(json.dumps({'Variables': env}))
")

if aws lambda get-function --function-name "$FUNCTION_NAME" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" >/dev/null 2>&1; then
    log "Lambda $FUNCTION_NAME exists — updating configuration only..."
    aws lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --role "$ROLE_ARN" \
        --handler "dispatcher.handler" \
        --runtime nodejs22.x \
        --timeout 60 \
        --memory-size 512 \
        --environment "$ENV_VARS_JSON" \
        --vpc-config "SubnetIds=${SUBNET_IDS},SecurityGroupIds=${SG_IDS}" \
        --region "$AWS_REGION" --profile "$AWS_PROFILE" \
        --output json >/dev/null
    aws lambda wait function-updated-v2 \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" --profile "$AWS_PROFILE"
    ok "Configuration updated"
else
    log "Creating Lambda $FUNCTION_NAME..."
    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --runtime nodejs22.x \
        --role "$ROLE_ARN" \
        --handler "dispatcher.handler" \
        --zip-file "fileb://$TMP_DIR/dispatcher-placeholder.zip" \
        --timeout 60 \
        --memory-size 512 \
        --environment "$ENV_VARS_JSON" \
        --vpc-config "SubnetIds=${SUBNET_IDS},SecurityGroupIds=${SG_IDS}" \
        --description "Phase 7D dispatcher: SQS → bot-runner /dial-out. Reads voice_call_requests rows." \
        --region "$AWS_REGION" --profile "$AWS_PROFILE" \
        --output json >/dev/null
    aws lambda wait function-active-v2 \
        --function-name "$FUNCTION_NAME" \
        --region "$AWS_REGION" --profile "$AWS_PROFILE"
    ok "Lambda created (placeholder code; deploy real code with ./scripts/deploy.sh --function dispatcher)"
fi

# Reserved concurrency — caps system-wide parallelism. Replaces the
# OG consumer-side semaphore.
log "Setting reserved concurrency to $RESERVED_CONCURRENCY..."
aws lambda put-function-concurrency \
    --function-name "$FUNCTION_NAME" \
    --reserved-concurrent-executions "$RESERVED_CONCURRENCY" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" \
    --output json >/dev/null
ok "Reserved concurrency set"

# Create the live alias on first run so deploy.sh has a target. We
# point it at $LATEST initially; the first deploy.sh run will publish
# version 1 and update the alias to that.
if ! aws lambda get-alias \
    --function-name "$FUNCTION_NAME" \
    --name "live" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" >/dev/null 2>&1; then
    log "Creating live alias pointing at \$LATEST..."
    aws lambda create-alias \
        --function-name "$FUNCTION_NAME" \
        --name "live" \
        --function-version "\$LATEST" \
        --description "Production traffic target. Update via deploy.sh." \
        --region "$AWS_REGION" --profile "$AWS_PROFILE" \
        --output json >/dev/null
    ok "Alias 'live' created"
else
    ok "Alias 'live' exists"
fi

# ────────────────────────────────────────────────────────────────────
# SQS event source mapping

log "Configuring SQS event source mapping..."

# Look for an existing mapping. We only attach one mapping per
# (function, queue) pair, so this is the natural identity.
EXISTING_UUID=$(aws lambda list-event-source-mappings \
    --function-name "$FUNCTION_NAME" \
    --event-source-arn "$QUEUE_ARN" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" \
    --query 'EventSourceMappings[0].UUID' --output text 2>/dev/null || echo "")

# ScalingConfig.MaximumConcurrency requires the function's reserved
# concurrency to be at least equal to it. We set both above.
SCALING_MAX=$RESERVED_CONCURRENCY

if [ -n "$EXISTING_UUID" ] && [ "$EXISTING_UUID" != "None" ]; then
    log "Mapping exists ($EXISTING_UUID) — updating..."
    aws lambda update-event-source-mapping \
        --uuid "$EXISTING_UUID" \
        --batch-size 1 \
        --maximum-batching-window-in-seconds 0 \
        --function-response-types ReportBatchItemFailures \
        --scaling-config "MaximumConcurrency=$SCALING_MAX" \
        --region "$AWS_REGION" --profile "$AWS_PROFILE" \
        --output json >/dev/null
    ok "Mapping updated"
else
    log "Creating new mapping..."
    aws lambda create-event-source-mapping \
        --function-name "$FUNCTION_NAME" \
        --event-source-arn "$QUEUE_ARN" \
        --batch-size 1 \
        --maximum-batching-window-in-seconds 0 \
        --function-response-types ReportBatchItemFailures \
        --scaling-config "MaximumConcurrency=$SCALING_MAX" \
        --region "$AWS_REGION" --profile "$AWS_PROFILE" \
        --output json >/dev/null
    ok "Mapping created"
fi

# ────────────────────────────────────────────────────────────────────
# Queue visibility timeout

log "Updating queue visibility timeout to 600s (was 5400s OG default)..."
aws sqs set-queue-attributes \
    --queue-url "$QUEUE_URL" \
    --attributes "VisibilityTimeout=600" \
    --region "$AWS_REGION" --profile "$AWS_PROFILE" \
    --output json >/dev/null
ok "Queue visibility timeout updated"

echo
echo -e "${GREEN}Bootstrap complete.${NC}"
echo "  Function:    $FUNCTION_NAME"
echo "  Role:        $ROLE_ARN"
echo "  Queue:       $QUEUE_URL"
echo "  Bot-runner:  $BOT_RUNNER_ARN"
echo
echo "Next steps:"
echo "  1. Deploy real dispatcher code:  ./scripts/deploy.sh --function dispatcher"
if [ "$STAGE" = "dev" ]; then
    echo "     (Pass --prod after dev verification.)"
fi
echo "  2. Verify with synthetic SQS message (see PR description for command)."
echo "  3. Apply CloudWatch alarms:"
echo "     aws cloudformation deploy --template-file infrastructure/dispatcher-alarms.yml \\"
echo "         --stack-name medcloud-voice-call-dispatcher-alarms \\"
echo "         --parameter-overrides FunctionName=$FUNCTION_NAME \\"
echo "             AlertTopicArn=arn:aws:sns:${AWS_REGION}:${ACCOUNT}:voice-api-oncall \\"
echo "         --region $AWS_REGION"
