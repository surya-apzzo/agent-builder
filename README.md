# Merchant Onboarding API Service

A standalone FastAPI microservice for merchant onboarding in a multi-merchant chatbot system. This service handles file uploads via signed URLs, processes knowledge base documents and product files, and sets up Vertex AI Search datastores.

## Features

- **Signed URL Uploads**: Direct file uploads to Google Cloud Storage (no file size limits)
- **Background Processing**: Asynchronous processing with progress tracking
- **Document Conversion**: Converts PDF, DOCX, TXT, HTML to NDJSON format for Vertex AI Search
- **Product Processing**: Processes CSV/XLSX product files into curated and full schemas
- **Website Crawling**: Uses Vertex AI Search's built-in crawler to automatically crawl merchant websites
- **Vertex AI Search Integration**: Automated datastore creation and document import
- **Status Tracking**: Real-time progress monitoring for onboarding jobs

## Architecture

```
onboarding-service/
├── onboarding_api.py          # Main FastAPI application
├── handlers/
│   ├── gcs_handler.py         # GCS operations + signed URL generation
│   ├── product_processor.py   # Product CSV/XLSX processing
│   ├── document_converter.py  # Document conversion to NDJSON
│   ├── vertex_setup.py        # Vertex AI Search setup (includes website crawling config)
│   └── config_generator.py    # Config JSON generation
├── utils/
│   └── status_tracker.py      # Status tracking
├── requirements.txt
├── Dockerfile
└── README.md
```

## API Endpoints

### File Upload

#### `POST /files/upload-url`
Generate signed URL for direct file upload to GCS.

**Request (Form Data):**
- `filename`: Original filename
- `content_type`: MIME type (e.g., `application/pdf`)
- `folder`: Folder name (`knowledge_base`, `prompt-docs`, `training_files`, `brand-images`)
- `user_id`: User identifier
- `expiration_minutes`: URL expiration time (default: 60)

**Response:**
```json
{
  "upload_url": "https://storage.googleapis.com/...",
  "object_path": "users/{user_id}/knowledge_base/document.pdf",
  "expires_in": 3600,
  "method": "PUT",
  "headers": {
    "Content-Type": "application/pdf"
  }
}
```

#### `POST /files/confirm`
Confirm file upload was successful.

**Request (Form Data):**
- `object_path`: GCS object path

**Response:**
```json
{
  "status": "confirmed",
  "object_path": "users/{user_id}/knowledge_base/document.pdf",
  "size": 12345,
  "content_type": "application/pdf",
  "created": "2024-01-01T00:00:00"
}
```

### Onboarding

#### `POST /onboard`
Start merchant onboarding process.

**Request (JSON):**
```json
{
  "merchant_id": "merchant-slug",
  "user_id": "firebase-uid",
  "shop_name": "Shop Name",
  "shop_url": "https://shop.com",
  "bot_name": "AI Assistant",
  "target_customer": "Tech-savvy millennials",
  "top_questions": "What are your return policies?",
  "top_products": "Product A, Product B",
  "primary_color": "#667eea",
  "secondary_color": "#764ba2",
  "logo_url": "gs://bucket/path/to/logo.png",
  "file_paths": {
    "knowledge": [
      "merchants/{merchant_id}/knowledge_base/doc1.pdf",
      "merchants/{merchant_id}/knowledge_base/doc2.docx"
    ]
  }
}
```

**Note:** 
- Upload `products.csv` and `categories.csv` to `knowledge_base/` folder initially
- The system will **auto-detect** them from `knowledge_base/` - no need to specify in `file_paths`
- You can omit `file_paths` entirely if all files are in `knowledge_base/`

**Response:**
```json
{
  "job_id": "merchant-slug_1234567890",
  "merchant_id": "merchant-slug",
  "status": "started",
  "status_url": "/onboard-status/merchant-slug"
}
```

#### `GET /onboard-status/{merchant_id}`
Get onboarding progress status.

**Response:**
```json
{
  "job_id": "merchant-slug_1234567890",
  "merchant_id": "merchant-slug",
  "user_id": "firebase-uid",
  "status": "in_progress",
  "progress": 50,
  "total_steps": 6,
  "current_step": "convert_documents",
  "steps": {
    "create_folders": {
      "status": "completed",
      "message": "Creating folder structure",
      "started_at": "2024-01-01T00:00:00",
      "completed_at": "2024-01-01T00:00:05"
    },
    "process_products": {
      "status": "completed",
      "message": "Processed 150 products"
    },
    "convert_documents": {
      "status": "in_progress",
      "message": "Converting documents to NDJSON"
    },
    "setup_vertex": {
      "status": "pending",
      "message": "Setting up Vertex AI Search"
    },
    "generate_config": {
      "status": "pending",
      "message": "Generating merchant configuration"
    },
    "finalize": {
      "status": "pending",
      "message": "Finalizing onboarding"
    }
  },
  "created_at": "2024-01-01T00:00:00",
  "updated_at": "2024-01-01T00:01:00"
}
```

### Health Check

#### `GET /health`
Health check endpoint.

#### `GET /`
API information and available endpoints.

## File Upload Workflow

1. **Frontend calls** `POST /files/upload-url` with filename, content_type, folder, and user_id
2. **Backend returns** signed URL for direct GCS upload
3. **Frontend uploads** file directly to GCS using signed URL (PUT request)
4. **Frontend optionally calls** `POST /files/confirm` to verify upload
5. **Frontend calls** `POST /onboard` with merchant info and file paths

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GCS_BUCKET_NAME` | `chekout-ai` | Google Cloud Storage bucket name |
| `GCP_PROJECT_ID` | `shopify-473015` | Google Cloud Project ID |
| `GCP_LOCATION` | `global` | GCP location for Vertex AI Search |
| `VERTEX_COLLECTION` | `default_collection` | Vertex AI Search collection ID |
| `PORT` | `8080` | Server port |
| `ALLOWED_ORIGINS` | `*` | CORS allowed origins (comma-separated) |
| `SIGNED_URL_EXPIRATION` | `3600` | Signed URL expiration in seconds |

## Setup

### Local Development

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Set up Google Cloud credentials:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

3. **Set environment variables:**
```bash
export GCS_BUCKET_NAME="chekout-ai"
export GCP_PROJECT_ID="shopify-473015"
export GCP_LOCATION="global"
```

4. **Run the application:**
```bash
uvicorn onboarding_api:app --host 0.0.0.0 --port 8080 --reload
```

### Docker

1. **Build the image:**
```bash
docker build -t onboarding-service .
```

2. **Run the container:**
```bash
docker run -p 8080:8080 \
  -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json \
  -e GCS_BUCKET_NAME=chekout-ai \
  -e GCP_PROJECT_ID=shopify-473015 \
  -v /path/to/key.json:/path/to/key.json \
  onboarding-service
```

### Cloud Run Deployment

1. **Build and push to Google Container Registry:**
```bash
gcloud builds submit --tag gcr.io/shopify-473015/onboarding-service
```

2. **Deploy to Cloud Run:**
```bash
gcloud run deploy onboarding-service \
  --image gcr.io/shopify-473015/onboarding-service \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GCS_BUCKET_NAME=chekout-ai,GCP_PROJECT_ID=shopify-473015
```

## Processing Steps

The onboarding process includes the following steps:

1. **Create Folders**: Creates GCS folder structure for merchant
2. **Process Products**: Converts product CSV/XLSX to JSON and NDJSON formats
3. **Convert Documents**: Converts knowledge base documents to NDJSON format
4. **Setup Vertex**: Creates Vertex AI Search datastore with website crawling configuration (if shop_url provided) and imports documents (knowledge base, products)
5. **Generate Config**: Creates merchant configuration JSON
6. **Finalize**: Completes onboarding process

## Product Processing

### Curated Products (products.json)
**Location**: `merchants/{merchant_id}/prompt-docs/products.json`

Extracts essential fields for **frontend display** and Langflow:
- `name` - Product name (REQUIRED)
- `image_url` - Product image URL (REQUIRED for frontend display)
- `link` - Product URL/link (REQUIRED for frontend)
- `price` - Price (from variant_price) (REQUIRED)
- `compare_at_price` - Compare at price (optional, only if exists)

**Note**: Description is NOT included in products.json - it can be fetched from Vertex AI Search when needed.

**Usage**: 
- Frontend: Display product cards with images, names, prices, and links
- Langflow: Reference file for product lookups
- Contains ALL products but is NOT passed directly to Gemini prompts

### Full Products (products.ndjson)
**Location**: `merchants/{merchant_id}/training_files/products.ndjson`

Complete product schema for Vertex AI Search ingestion with ALL original fields.

**Usage**: Indexed in Vertex AI Search. When user asks about products, only the top 3-5 most relevant products are retrieved and passed to Gemini (not all 200).

### Important: RAG Architecture

**DO NOT** pass all 200 products to Gemini in every prompt. Instead:

1. **Index all products** in Vertex AI Search (via products.ndjson)
2. **Retrieve only relevant products** (3-5) when user asks a question
3. **Pass only retrieved products** to Gemini in the prompt

This approach:
- ✅ Reduces token usage and costs
- ✅ Provides better context (only relevant products)
- ✅ Faster responses
- ✅ Scales to thousands of products

See `PRODUCT_ARCHITECTURE.md` for detailed implementation guide.

## Document Conversion

Supports conversion from:
- **PDF**: Extracts text from all pages
- **DOCX**: Extracts text from paragraphs
- **TXT**: Direct text extraction
- **HTML**: Extracts text content (removes scripts/styles)

All documents are converted to NDJSON format with chunking for large files.

## Website Crawling

The service uses **Vertex AI Search's built-in website crawler** when a `shop_url` is provided:

- **Native Integration**: Leverages Vertex AI Search's native website crawling capabilities
- **Automatic Configuration**: Datastore is automatically configured for website crawling when `shop_url` is provided
- **No Custom Code**: No need for custom crawler implementation - Vertex AI Search handles everything
- **Efficient**: Vertex AI Search's crawler is optimized for search indexing
- **Managed Service**: Crawling, indexing, and updates are handled by Google's infrastructure

When a merchant provides a `shop_url` during onboarding, the datastore is created with website crawling enabled. Vertex AI Search will automatically crawl the website and index the content. The crawling configuration may need to be completed via the Google Cloud Console or SiteSearchEngine API depending on your setup.

## Configuration Schema

Generated `config.json` structure:
```json
{
  "user_id": "firebase-uid",
  "merchant_id": "merchant-slug",
  "shop_name": "Shop Name",
  "shop_url": "https://shop.com",
  "bot_name": "AI Assistant",
  "products": {
    "bucket_name": "chekout-ai",
    "file_path": "users/{user_id}/prompt-docs/products.json"
  },
  "vertex_search": {
    "project_id": "shopify-473015",
    "location": "global",
    "datastore_id": "{merchant_id}-engine"
  }
}
```

## Error Handling

- All endpoints include comprehensive error handling
- Background tasks track errors at each step
- Status endpoint shows detailed error messages
- Failed steps are marked with error details

## Logging

The service uses Python's logging module with INFO level by default. Logs include:
- Request/response information
- Processing progress
- Error details
- GCS and Vertex AI operations

## License

Proprietary - Internal Use Only

