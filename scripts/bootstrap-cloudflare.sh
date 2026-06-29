#!/usr/bin/env bash
set -euo pipefail

BUCKET_NAME="${R2_BUCKET_NAME:-seoul-dev}"

if ! command -v npm >/dev/null 2>&1; then
  echo "npm is required because this script runs npx wrangler." >&2
  exit 1
fi

echo "Checking Cloudflare R2 bucket: ${BUCKET_NAME}"

if npx wrangler r2 bucket info "${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "Bucket already exists: ${BUCKET_NAME}"
else
  echo "Creating bucket: ${BUCKET_NAME}"
  npx wrangler r2 bucket create "${BUCKET_NAME}"
fi

echo "Checking R2 Data Catalog for bucket: ${BUCKET_NAME}"

if npx wrangler r2 bucket catalog get "${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "Data Catalog already exists for bucket: ${BUCKET_NAME}"
else
  echo "Enabling Data Catalog for bucket: ${BUCKET_NAME}"
  npx wrangler r2 bucket catalog enable "${BUCKET_NAME}"
fi

echo
echo "Current Data Catalog details:"
npx wrangler r2 bucket catalog get "${BUCKET_NAME}"

cat <<'EOF'

Copy the Catalog URI and Warehouse into .env (use R2_DEV_* for the seoul-dev bucket):

R2_DEV_DATA_CATALOG_URI=<catalog-uri>
R2_DEV_DATA_CATALOG_WAREHOUSE=<warehouse>

Runtime secret values are created in the Cloudflare R2 API Tokens screen:

R2_DEV_DATA_CATALOG_TOKEN=<r2-api-token-value>
R2_ACCESS_KEY_ID=<r2-access-key-id>
R2_SECRET_ACCESS_KEY=<r2-secret-access-key>
EOF
