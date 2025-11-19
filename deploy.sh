#!/bin/bash

# Configuration
PROJECT_ID="shopify-473015"  # GCP project ID
REGION="us-central1"
SERVICE_NAME="merchant-onboarding-api"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "🚀 Deploying ${SERVICE_NAME} to Google Cloud Run..."

# Build and submit to Google Cloud Build
echo "📦 Building and pushing Docker image..."
gcloud builds submit --tag ${IMAGE_NAME}:latest --project ${PROJECT_ID}

# Deploy to Cloud Run (preserves all existing config - env vars, VPC, secrets, etc.)
echo "☁️  Deploying new revision to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME}:latest \
  --platform managed \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --allow-unauthenticated \
  --port 8080 \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 10 \
  --min-instances 0

# Get the service URL
echo "✅ Deployment complete!"
echo "🌐 Service URL:"
gcloud run services describe ${SERVICE_NAME} \
  --platform managed \
  --region ${REGION} \
  --format 'value(status.url)' \
  --project ${PROJECT_ID}

echo ""
echo "📝 Note: Make sure to set environment variables in Cloud Run:"
echo "   - GCS_CLIENT_EMAIL"
echo "   - GCS_PRIVATE_KEY"
echo "   - GCS_BUCKET_NAME"
echo "   - VERTEX_CLIENT_EMAIL (optional)"
echo "   - VERTEX_PRIVATE_KEY (optional)"
echo "   - VERTEX_PROJECT_ID (optional, defaults to shopify-473015)"
echo "   - VERTEX_LOCATION (optional, defaults to global)"
echo "   - DB_DSN (if using database features)"

