#!/bin/bash
# Run script for Merchant Onboarding API

# Activate virtual environment
source .venv/bin/activate

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "⚠️  .env file not found. Using system environment variables."
fi

# Set defaults if not already set
export DB_DSN="${DB_DSN:-postgresql://user_dev:iol%40j39Rs1kp%24LY%3F@34.45.186.110:5432/chekoutai}"
export GCS_BUCKET_NAME="${GCS_BUCKET_NAME:-chekout-ai}"
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-shopify-473015}"
export GCP_LOCATION="${GCP_LOCATION:-global}"
export VERTEX_COLLECTION="${VERTEX_COLLECTION:-default_collection}"
export PORT="${PORT:-8080}"
export ALLOWED_ORIGINS="${ALLOWED_ORIGINS:-http://localhost:3000}"

# Set up Google Cloud credentials
# Option 1: Use service account JSON file if GOOGLE_APPLICATION_CREDENTIALS is set
# Option 2: Use credentials from environment variables (handled by GCSHandler)
if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    echo "Using service account file: $GOOGLE_APPLICATION_CREDENTIALS"
elif [ -f .credentials/gcs-service-account.json ]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/.credentials/gcs-service-account.json"
    echo "Using service account file: $GOOGLE_APPLICATION_CREDENTIALS"
else
    echo "⚠️  No service account file found. Using credentials from environment variables."
    echo "   (Make sure GCS_CLIENT_EMAIL, GCS_PRIVATE_KEY are set in .env)"
fi

# Run the application
echo ""
echo "🚀 Starting Merchant Onboarding API on port $PORT..."
echo "   Frontend URL: ${FRONTEND_URL:-http://localhost:3000}"
echo "   GCS Bucket: $GCS_BUCKET_NAME"
echo "   GCP Project: $GCP_PROJECT_ID"
echo ""
uvicorn onboarding_api:app --host 0.0.0.0 --port $PORT --reload

