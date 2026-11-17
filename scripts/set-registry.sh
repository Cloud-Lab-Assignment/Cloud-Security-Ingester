#!/usr/bin/env bash
# Point gitops/ at your Artifact Registry: replaces the PROJECT_ID placeholder
# (and the region, if not asia-south1) in every image reference.
# Usage: scripts/set-registry.sh <gcp-project-id> [region]
set -euo pipefail
PROJECT_ID="${1:?usage: $0 <gcp-project-id> [region]}"
REGION="${2:-asia-south1}"
cd "$(dirname "$0")/.."
grep -rl "asia-south1-docker.pkg.dev/PROJECT_ID" gitops | while read -r f; do
  sed -i "s#asia-south1-docker.pkg.dev/PROJECT_ID#${REGION}-docker.pkg.dev/${PROJECT_ID}#g" "$f"
done
echo "gitops/ now points at ${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-sec - commit and push it."
