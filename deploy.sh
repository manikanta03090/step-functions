#!/usr/bin/env bash
# Usage: ALERT_EMAIL=you@example.com ./deploy.sh [stack-name]
set -euo pipefail

STACK="${1:-csv-pipeline}"          # lowercase letters, digits, hyphens; 3-20 chars
: "${ALERT_EMAIL:?Set ALERT_EMAIL to the address that should receive alerts}"

sam build
sam deploy \
  --stack-name "$STACK" \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset \
  --parameter-overrides ProjectName="$STACK" AlertEmail="$ALERT_EMAIL"

BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" --output text)

# The Glue job's script lives in the data bucket (the bucket is created by the stack).
aws s3 cp glue/csv_to_postgres.py "s3://${BUCKET}/scripts/csv_to_postgres.py"

echo
echo "Deployed. Confirm the SNS subscription email sent to ${ALERT_EMAIL}, then test:"
echo "  aws s3 cp tests/data/orders_20261106.csv s3://${BUCKET}/incoming/"
