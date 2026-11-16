#!/usr/bin/env bash
# Export the three core services as self-contained directories for the group repo.
#
#   scripts/export-group-repo.sh [--force] <dest>
#
# Writes <dest>/services/{event-store,incident-management,correlation-engine}/, each of
# which builds with its own directory as the Docker build context:
#   src/ test/ [migrations/]   copied from services/<name>/
#   lib/common/ [lib/tpc/]     vendored from packages/*/src, imports rewritten to relative paths
#   proto/                     only for services that load the gRPC contract
#   package.json               service deps minus @core/*, plus the vendored packages' deps
#   package-lock.json          npm install --package-lock-only inside the exported directory
#   Dockerfile .dockerignore .env.example README.md
#
# <dest> is usually a checkout of the group repo; nothing else in it is touched, and no
# git command is run. An existing non-empty <dest>/services/<name> is refused unless
# --force is given, in which case only those three directories are replaced. Each
# service is built in a staging directory first, so a failed run leaves <dest> as it was.
# The source tree is only read. Needs node 22 and npm (fetches registry metadata).
set -euo pipefail

SERVICES=(event-store incident-management correlation-engine)

usage() {
  echo "usage: $0 [--force] <dest>" >&2
  exit 2
}

force=0
dest=""
for arg in "$@"; do
  case "$arg" in
    --force) force=1 ;;
    -h | --help) usage ;;
    -*) echo "unknown option: $arg" >&2; usage ;;
    *) [[ -z "$dest" ]] || usage; dest="$arg" ;;
  esac
done
[[ -n "$dest" ]] || usage

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "$script_dir/.." && pwd -P)"
helper="$script_dir/lib/export-service.mjs"

refuse_in_repo() {
  case "$1/" in
    "$repo_root/"*) echo "refusing to export into the source repo ($1)" >&2; exit 1 ;;
  esac
}
# Check before creating anything, then again with symlinks resolved.
refuse_in_repo "$(node -e 'console.log(require("node:path").resolve(process.argv[1]))' "$dest")"
mkdir -p "$dest/services"
dest="$(cd "$dest" && pwd -P)"
refuse_in_repo "$dest"

for s in "${SERVICES[@]}"; do
  target="$dest/services/$s"
  if [[ -d "$target" && -n "$(ls -A "$target")" && $force -eq 0 ]]; then
    echo "$target exists and is not empty; re-run with --force to replace it" >&2
    exit 1
  fi
  if [[ -e "$target" && ! -d "$target" ]]; then
    echo "$target exists and is not a directory" >&2
    exit 1
  fi
done

staging="$(mktemp -d "$dest/services/.export-tmp.XXXXXX")"
trap 'rm -rf "$staging"' EXIT

for s in "${SERVICES[@]}"; do
  out="$staging/$s"
  mkdir -p "$out"
  node "$helper" export "$repo_root" "$s" "$out"
  echo "$s: generating package-lock.json"
  (cd "$out" && npm install --package-lock-only --no-audit --no-fund --ignore-scripts --loglevel=error >/dev/null)
  [[ -f "$out/package-lock.json" && ! -e "$out/node_modules" ]] || {
    echo "$s: npm did not produce a lock-only result" >&2
    exit 1
  }
  node "$helper" check "$out"
done

if grep -rn "@core/" "$staging"; then
  echo "export still references @core/ (see above)" >&2
  exit 1
fi

for s in "${SERVICES[@]}"; do
  rm -rf "${dest:?}/services/$s"
  mv "$staging/$s" "$dest/services/$s"
done

echo "exported to $dest/services/{$(IFS=,; echo "${SERVICES[*]}")}"
echo "build from $dest: docker build -t <name> services/<name>"
