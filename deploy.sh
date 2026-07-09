#!/bin/bash
set -euo pipefail
set -x

# Usage: deploy.sh <environment>
# Example: deploy.sh prod
#          DRY_RUN=true deploy.sh staging
#          APPLY_CANARY=true deploy.sh prod
#
# Run from an admin environment with kubectl, helm, heva, and AWS CLI configured.
# Mirrors the trino-cluster deploy flow: pull S3 infra secrets → heva merge → helm template → helm upgrade.

SERVICE_NAME="fern"
ENVIRONMENT="${1:-}"
NAMESPACE="${4:-}"
APPLY_CANARY="${APPLY_CANARY:-false}"
DELETE_CANARY="${DELETE_CANARY:-false}"
DRY_RUN="${DRY_RUN:-false}"
PROJECT="${PROJECT:-}"
HELM_CHART_FILE="${SERVICE_NAME}-final-${ENVIRONMENT}-values.yaml"

EXPECTED_NAMESPACE="fern"
VALUES_FILENAME="values.yaml"
REPO_ROOT="${FERN_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
HELM_CHART_DIR="${REPO_ROOT}/deploy/helm/fern"

if [[ -z "${ENVIRONMENT}" ]]; then
  echo "Usage: deploy.sh <environment>" >&2
  echo "  environments: prod, staging, qa, dev, hqa, uat" >&2
  exit 1
fi

if [[ -n "${NAMESPACE}" && "${NAMESPACE}" != "${EXPECTED_NAMESPACE}" ]]; then
  echo "WARN: passed NAMESPACE='${NAMESPACE}' does not match expected '${EXPECTED_NAMESPACE}'. Overriding." >&2
fi
NAMESPACE="${EXPECTED_NAMESPACE}"

SECRETS_PATH="/tmp/${EXPECTED_NAMESPACE}-${ENVIRONMENT}"
if [[ -n "${PROJECT}" ]]; then
  SECRETS_PATH="${SECRETS_PATH}-${PROJECT}"
fi

function delete-canary {
  local release_name="${SERVICE_NAME}-canary"
  if [[ -n "${PROJECT}" ]]; then
    release_name="${SERVICE_NAME}-${PROJECT}-canary"
  fi

  if helm ls --namespace "${NAMESPACE}" | grep -q "${release_name}"; then
    echo "Deleting canary release: ${release_name}"
    helm uninstall "${release_name}" --namespace "${NAMESPACE}"
  else
    echo "Canary release ${release_name} not found; nothing to delete."
  fi
}

function pull-secrets {
  local region="${1:-ap-south-1}"
  mkdir -p "${SECRETS_PATH}"

  echo "Pulling ${ENVIRONMENT} infra secrets..."
  if [[ "${ENVIRONMENT}" == "prod" ]]; then
    aws s3 cp "s3://bbana-prod-conf/${ENVIRONMENT}/infra-settings/values-infra-secrets.yaml" \
      "${SECRETS_PATH}/values-infra-secrets.yaml" --region "${region}"
  elif [[ "${ENVIRONMENT}" == "staging" || "${ENVIRONMENT}" == "qa" ]]; then
    aws s3 cp "s3://bbana-nonprod-conf/${ENVIRONMENT}/infra-settings/values-infra-secrets.yaml" \
      "${SECRETS_PATH}/values-infra-secrets.yaml" --region "${region}"
  elif [[ "${ENVIRONMENT}" == "dev" || "${ENVIRONMENT}" == "hqa" || "${ENVIRONMENT}" == "uat" ]]; then
    aws s3 cp "s3://qa-conf/${ENVIRONMENT}/infra-settings/values-infra-secrets-${ENVIRONMENT}.yaml" \
      "${SECRETS_PATH}/values-infra-secrets.yaml" --region "${region}"
  else
    echo "No S3 secrets path configured for environment: ${ENVIRONMENT}" >&2
    exit 1
  fi

  if [[ ! -f "${SECRETS_PATH}/values-infra-secrets.yaml" ]]; then
    echo "ERROR: Failed to download values-infra-secrets.yaml" >&2
    exit 1
  fi
}

function select-registry {
  if [[ "${ENVIRONMENT}" == "prod" || "${ENVIRONMENT}" == "staging" || "${ENVIRONMENT}" == "perf" || "${PROJECT}" == "bb-stable" ]]; then
    registry="274334742953.dkr.ecr.ap-south-1.amazonaws.com/bb-engg"
  elif [[ "${ENVIRONMENT}" == "local" ]]; then
    registry=""
  else
    registry="274334742953.dkr.ecr.us-east-1.amazonaws.com/bb-engg"
  fi
}

function run-heva {
  local is_canary="${1:-false}"
  local secrets_path="${SECRETS_PATH}"
  if [[ "${is_canary}" == "true" ]]; then
    secrets_path="${SECRETS_PATH}-canary"
    mkdir -p "${secrets_path}"
    cp "${SECRETS_PATH}/values-infra-secrets.yaml" "${secrets_path}/values-infra-secrets.yaml"
  fi

  if [[ ! -d "${HELM_CHART_DIR}" ]]; then
    echo "Helm chart not found: ${HELM_CHART_DIR}" >&2
    exit 1
  fi

  local values_file="${ENVIRONMENT}/${VALUES_FILENAME}"
  if [[ ! -f "${HELM_CHART_DIR}/${values_file}" ]]; then
    echo "Values file not found: ${HELM_CHART_DIR}/${values_file}" >&2
    exit 1
  fi

  echo "Running heva merge for ${ENVIRONMENT} (${values_file})..."
  cd "${HELM_CHART_DIR}"
  heva \
    -f "${values_file}" \
    -f "${secrets_path}/values-infra-secrets.yaml" \
    -o "${secrets_path}/${HELM_CHART_FILE}"

  echo "Rendering helm templates..."
  helm template "${SERVICE_NAME}" . \
    --namespace "${NAMESPACE}" \
    --values "${secrets_path}/${HELM_CHART_FILE}" \
    --set "namespace=${NAMESPACE}" \
    --set "registry=${registry}" \
    --set "isCanary=${is_canary}" \
    > /dev/null
}

function deploy {
  local is_canary="${1:-false}"
  local is_dry_run="${2:-false}"

  local release_name="${SERVICE_NAME}"
  if [[ -n "${PROJECT}" ]]; then
    release_name="${SERVICE_NAME}-${PROJECT}"
  fi
  if [[ "${is_canary}" == "true" ]]; then
    release_name="${release_name}-canary"
  fi

  local secrets_path="${SECRETS_PATH}"
  if [[ "${is_canary}" == "true" ]]; then
    secrets_path="${SECRETS_PATH}-canary"
  fi

  local helm_args=(
    upgrade
    --install
    "${release_name}"
    "${HELM_CHART_DIR}"
    --namespace "${NAMESPACE}"
    --create-namespace
    --atomic
    --cleanup-on-fail
    --values "${secrets_path}/${HELM_CHART_FILE}"
    --set "namespace=${NAMESPACE}"
    --set "registry=${registry}"
    --set "isCanary=${is_canary}"
  )

  if [[ "${is_dry_run}" == "true" ]]; then
    helm_args+=(--dry-run --timeout 120s)
  else
    helm_args+=(--timeout 400s)
  fi

  echo "Deploying release ${release_name} to namespace ${NAMESPACE}..."
  helm "${helm_args[@]}"
}

select-registry

if [[ "${APPLY_CANARY}" == "false" && "${DELETE_CANARY}" == "false" ]]; then
  echo "Performing full deployment..."
  pull-secrets
  run-heva false
  delete-canary
  deploy false "${DRY_RUN}"
elif [[ "${APPLY_CANARY}" == "true" && "${DELETE_CANARY}" == "false" ]]; then
  echo "Performing canary deployment..."
  pull-secrets
  run-heva true
  deploy true "${DRY_RUN}"
elif [[ "${APPLY_CANARY}" == "false" && "${DELETE_CANARY}" == "true" ]]; then
  echo "Deleting canary deployment..."
  delete-canary
else
  echo "Invalid canary options. Set only one of APPLY_CANARY or DELETE_CANARY." >&2
  exit 1
fi

echo "Done."
