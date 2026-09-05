#!/usr/bin/env bash
# Apply application resources only, after the image has passed checks and been pushed.
set -euo pipefail

cronos_sha="${1:?Usage: bash k8s/apps/deploy.sh COMMIT_SHA [MIGRATION_ID]}"
cronos_migration_id="${2:-manual-$(date +%s)}"
if [[ ! "$cronos_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Expected the full 40-character commit SHA" >&2
  exit 1
fi
if [[ ! "$cronos_migration_id" =~ ^[a-z0-9]([a-z0-9-]{0,39}[a-z0-9])?$ ]]; then
  echo "Invalid migration identifier" >&2
  exit 1
fi

cronos_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cronos_tmp="$(mktemp -d)"
trap 'rm -rf -- "$cronos_tmp"' EXIT
cronos_kubectl=(kubectl --context yc-gradius --namespace cronos-bot)
cronos_job="cronos-migrate-${cronos_migration_id}"

# Fail before making changes if a prerequisite is absent. No secret values are printed.
"${cronos_kubectl[@]}" get serviceaccount cronos-runtime -o name
"${cronos_kubectl[@]}" get secret cronos-infra cronos-app image-pull-secret -o name
"${cronos_kubectl[@]}" get service postgres rabbitmq redis -o name

kubectl kustomize "$cronos_dir" \
  | sed "s/:IMAGE_TAG/:${cronos_sha}/g" > "$cronos_tmp/apps.yaml"
sed -e "s/:IMAGE_TAG/:${cronos_sha}/g" -e "s/MIGRATION_ID/${cronos_migration_id}/g" \
  "$cronos_dir/migration-job.yaml" > "$cronos_tmp/migration.yaml"

"${cronos_kubectl[@]}" apply --dry-run=server --validate=strict -f "$cronos_tmp/apps.yaml"
"${cronos_kubectl[@]}" create --dry-run=server --validate=strict -f "$cronos_tmp/migration.yaml"

"${cronos_kubectl[@]}" create -f "$cronos_tmp/migration.yaml"
if ! "${cronos_kubectl[@]}" wait --for=condition=complete --timeout=330s "job/$cronos_job"; then
  "${cronos_kubectl[@]}" logs "job/$cronos_job" --all-containers=true --tail=100 || true
  "${cronos_kubectl[@]}" get pods --selector "job-name=$cronos_job" -o wide || true
  exit 1
fi

"${cronos_kubectl[@]}" apply --validate=strict -f "$cronos_tmp/apps.yaml"
"${cronos_kubectl[@]}" rollout status deployment/cronos-runtime --timeout=600s
"${cronos_kubectl[@]}" rollout status deployment/cronos-gateway --timeout=600s
"${cronos_kubectl[@]}" get deployment cronos-gateway cronos-runtime -o wide
"${cronos_kubectl[@]}" get pvc cronos-artifacts
