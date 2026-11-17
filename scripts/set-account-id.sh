#!/usr/bin/env bash
# Replace the ACCOUNT_ID / region placeholder in gitops/ with your real ECR registry.
# Usage: scripts/set-account-id.sh <aws-account-id> [region]
set -euo pipefail
ACCOUNT_ID="${1:?usage: $0 <aws-account-id> [region]}"
REGION="${2:-ap-south-1}"
cd "$(dirname "$0")/.."
grep -rl "ACCOUNT_ID.dkr.ecr.ap-south-1" gitops | while read -r f; do
  sed -i "s/ACCOUNT_ID\.dkr\.ecr\.ap-south-1/${ACCOUNT_ID}.dkr.ecr.${REGION}/g" "$f"
done
echo "gitops/ now points at ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com - commit and push it."
