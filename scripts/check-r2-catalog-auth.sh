#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f .env ]]; then
  echo ".env not found" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
. ./.env
set +a

missing=0
for name in R2_DATA_CATALOG_TOKEN R2_DATA_CATALOG_URI R2_DATA_CATALOG_WAREHOUSE; do
  if [[ -z "${!name:-}" ]]; then
    echo "Missing ${name}" >&2
    missing=1
  fi
done

if [[ "${missing}" -ne 0 ]]; then
  exit 1
fi

tmp_file="$(mktemp)"
trap 'rm -f "${tmp_file}"' EXIT

status="$(
  curl -sS -o "${tmp_file}" -w "%{http_code}" \
    -H "Authorization: Bearer ${R2_DATA_CATALOG_TOKEN}" \
    "${R2_DATA_CATALOG_URI%/}/v1/config?warehouse=${R2_DATA_CATALOG_WAREHOUSE}"
)"

echo "R2 Data Catalog auth check HTTP status: ${status}"

if [[ "${status}" != "200" ]]; then
  echo "Response:"
  head -c 500 "${tmp_file}"
  echo
  exit 1
fi

echo "R2 Data Catalog token can access the configured warehouse."
