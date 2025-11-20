"""Main FastAPI application for Merchant Onboarding Service"""

import os
import logging
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, use system environment variables

from fastapi import FastAPI, HTTPException, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from handlers.gcs_handler import GCSHandler
from handlers.product_processor import ProductProcessor
from handlers.document_converter import DocumentConverter
from handlers.vertex_setup import VertexSetup
from handlers.config_generator import ConfigGenerator
from utils.status_tracker import StatusTracker, StepStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize global handlers
gcs_handler = None
product_processor = None
document_converter = None
vertex_setup = None
config_generator = None
status_tracker = StatusTracker()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    global gcs_handler, product_processor, document_converter, vertex_setup, config_generator

    # Startup
    logger.info("Starting Merchant Onboarding Service...")
    try:
        gcs_handler = GCSHandler()
        product_processor = ProductProcessor(gcs_handler)
        document_converter = DocumentConverter(gcs_handler)
        vertex_setup = VertexSetup()
        config_generator = ConfigGenerator(gcs_handler)
        logger.info("All handlers initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize handlers: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down Merchant Onboarding Service...")


# Create FastAPI app
app = FastAPI(
    title="Merchant Onboarding API",
    description="API service for merchant onboarding with file uploads and background processing",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response Models
class OnboardRequest(BaseModel):
    """Merchant onboarding request model"""
    merchant_id: str
    user_id: str
    shop_name: str
    shop_url: str
    bot_name: Optional[str] = "AI Assistant"
    target_customer: Optional[str] = None
    top_questions: Optional[str] = None
    top_products: Optional[str] = None
    primary_color: Optional[str] = "#667eea"
    secondary_color: Optional[str] = "#764ba2"
    logo_url: Optional[str] = None
    platform: Optional[str] = Field(
        None,
        description="E-commerce platform type: 'shopify', 'woocommerce', 'wordpress', or 'custom'. If not provided, will auto-detect from shop_url."
    )
    custom_url_pattern: Optional[str] = Field(
        None,
        description="Custom URL pattern for 'custom' platform. Use {handle} as placeholder. Example: '/item/{handle}' or '/p/{handle}'"
    )
    file_paths: Optional[Dict[str, Any]] = Field(
        None,
        description="Optional: File paths are auto-detected from knowledge_base folder. This field is deprecated - all files should be uploaded to knowledge_base/."
    )


# Background processing function
async def process_onboarding(
    merchant_id: str,
    user_id: str,
    shop_name: str,
    shop_url: str,
    bot_name: str,
    target_customer: Optional[str],
    top_questions: Optional[str],
    top_products: Optional[str],
    primary_color: Optional[str],
    secondary_color: Optional[str],
    logo_url: Optional[str],
    platform: Optional[str],
    custom_url_pattern: Optional[str],
    file_paths: Optional[Dict[str, Any]]
):
    """Background task for processing onboarding"""
    try:
        # Step 1: Create folder structure
        status_tracker.update_step_status(
            merchant_id, "create_folders", StepStatus.IN_PROGRESS
        )
        try:
            gcs_handler.create_folder_structure(merchant_id, user_id)
            status_tracker.update_step_status(
                merchant_id, "create_folders", StepStatus.COMPLETED,
                message="Folder structure created successfully"
            )
        except Exception as e:
            status_tracker.update_step_status(
                merchant_id, "create_folders", StepStatus.FAILED,
                error=str(e)
            )
            raise

        # Step 2: Process products
        # ONLY check knowledge_base folder - no other locations
        products_file_path = None
        
        knowledge_base_prefix = f"merchants/{merchant_id}/knowledge_base/"
        try:
            files_in_kb = gcs_handler.list_files(knowledge_base_prefix)
            # Look for products.json, products.csv, or products.xlsx (in that order of preference)
            for file_path in files_in_kb:
                filename = file_path.split('/')[-1].lower()
                if filename in ['products.json', 'products.csv', 'products.xlsx', 'products.xls']:
                    products_file_path = file_path
                    logger.info(f"Found products file in knowledge_base: {products_file_path}")
                    break
        except Exception as e:
            logger.warning(f"Could not scan knowledge_base for products file: {e}")
        
        if products_file_path:
            status_tracker.update_step_status(
                merchant_id, "process_products", StepStatus.IN_PROGRESS
            )
            try:
                result = product_processor.process_products_file(
                    merchant_id, 
                    products_file_path,
                    shop_url=shop_url,
                    platform=platform,
                    custom_url_pattern=custom_url_pattern
                )
                status_tracker.update_step_status(
                    merchant_id, "process_products", StepStatus.COMPLETED,
                    message=f"Processed {result['product_count']} products from {products_file_path}"
                )
            except Exception as e:
                status_tracker.update_step_status(
                    merchant_id, "process_products", StepStatus.FAILED,
                    error=str(e)
                )
                raise
        else:
            status_tracker.update_step_status(
                merchant_id, "process_products", StepStatus.SKIPPED,
                message="No products file found in knowledge_base"
            )

        # Step 2b: Process categories
        # ONLY check knowledge_base folder - no other locations
        categories_file_path = None
        
        knowledge_base_prefix = f"merchants/{merchant_id}/knowledge_base/"
        try:
            files_in_kb = gcs_handler.list_files(knowledge_base_prefix)
            # Look for categories.csv or categories.xlsx
            for file_path in files_in_kb:
                filename = file_path.split('/')[-1].lower()
                if filename in ['categories.csv', 'categories.xlsx', 'categories.xls']:
                    categories_file_path = file_path
                    logger.info(f"Found categories file in knowledge_base: {categories_file_path}")
                    break
        except Exception as e:
            logger.warning(f"Could not scan knowledge_base for categories file: {e}")
        
        if categories_file_path:
            status_tracker.update_step_status(
                merchant_id, "process_categories", StepStatus.IN_PROGRESS
            )
            try:
                result = product_processor.process_categories_file(merchant_id, categories_file_path)
                status_tracker.update_step_status(
                    merchant_id, "process_categories", StepStatus.COMPLETED,
                    message=f"Processed {result['category_count']} categories from {categories_file_path}"
                )
            except Exception as e:
                status_tracker.update_step_status(
                    merchant_id, "process_categories", StepStatus.FAILED,
                    error=str(e)
                )
                # Don't raise - categories are optional, continue with onboarding
                logger.warning(f"Categories processing failed but continuing: {e}")
        else:
            status_tracker.update_step_status(
                merchant_id, "process_categories", StepStatus.SKIPPED,
                message="No categories file found in knowledge_base"
            )

        # Step 3: Convert documents
        # ONLY check knowledge_base folder - collect all files except products.csv and categories.csv
        document_paths = []
        
        knowledge_base_prefix = f"merchants/{merchant_id}/knowledge_base/"
        try:
            files_in_kb = gcs_handler.list_files(knowledge_base_prefix)
            excluded_files = ['products.json', 'products.csv', 'products.xlsx', 'products.xls', 
                            'categories.csv', 'categories.xlsx', 'categories.xls']
            
            for file_path in files_in_kb:
                filename = file_path.split('/')[-1].lower()
                # Skip product/category files and .keep files
                if filename not in excluded_files and not filename.endswith('.keep'):
                    document_paths.append(file_path)
                    logger.info(f"Found document in knowledge_base: {file_path}")
        except Exception as e:
            logger.warning(f"Could not scan knowledge_base for documents: {e}")
        
        if document_paths:
            status_tracker.update_step_status(
                merchant_id, "convert_documents", StepStatus.IN_PROGRESS
            )
            try:
                result = document_converter.convert_documents(merchant_id, document_paths)
                
                if result['document_count'] > 0:
                    message = f"Converted {result['document_count']} documents"
                    if result.get('skipped_files'):
                        message += f" (skipped {len(result['skipped_files'])} files)"
                    status_tracker.update_step_status(
                        merchant_id, "convert_documents", StepStatus.COMPLETED,
                        message=message
                    )
                else:
                    # No documents were successfully converted
                    message = "No documents were successfully converted"
                    if result.get('skipped_files'):
                        message += f" (all {len(result['skipped_files'])} files were skipped/missing)"
                    status_tracker.update_step_status(
                        merchant_id, "convert_documents", StepStatus.SKIPPED,
                        message=message
                    )
            except Exception as e:
                status_tracker.update_step_status(
                    merchant_id, "convert_documents", StepStatus.FAILED,
                    error=str(e)
                )
                # Don't raise - allow onboarding to continue even if document conversion fails
                logger.error(f"Document conversion failed but continuing onboarding: {e}")
        else:
            status_tracker.update_step_status(
                merchant_id, "convert_documents", StepStatus.SKIPPED,
                message="No documents found in knowledge_base (excluding products.csv and categories.csv)"
            )

        # Step 4: Setup Vertex AI Search (includes website crawling configuration)
        status_tracker.update_step_status(
            merchant_id, "setup_vertex", StepStatus.IN_PROGRESS
        )
        try:
            # Create datastore with website crawling if shop_url provided
            # Vertex AI Search will automatically crawl the website using its built-in crawler
            datastore_result = vertex_setup.create_datastore(
                merchant_id=merchant_id,
                shop_url=shop_url,
                shop_name=shop_name
            )

            # Import documents if available (check if documents.ndjson was created)
            import_errors = []
            import_success = []
            
            documents_ndjson_path = f"merchants/{merchant_id}/training_files/documents.ndjson"
            if gcs_handler.file_exists(documents_ndjson_path):
                try:
                    gcs_uri = f"gs://{gcs_handler.bucket_name}/{documents_ndjson_path}"
                    vertex_setup.import_documents(merchant_id, gcs_uri)
                    import_success.append("documents")
                except Exception as import_error:
                    error_msg = str(import_error)
                    import_errors.append(f"documents: {error_msg}")
                    logger.error(f"Failed to import documents: {error_msg}")

            # Import products if available (check if products.ndjson was created)
            products_ndjson_path = f"merchants/{merchant_id}/training_files/products.ndjson"
            if gcs_handler.file_exists(products_ndjson_path):
                try:
                    gcs_uri = f"gs://{gcs_handler.bucket_name}/{products_ndjson_path}"
                    vertex_setup.import_documents(merchant_id, gcs_uri, import_type="FULL")
                    import_success.append("products")
                except Exception as import_error:
                    error_msg = str(import_error)
                    import_errors.append(f"products: {error_msg}")
                    logger.error(f"Failed to import products: {error_msg}")

            # Import categories if available (check if categories.ndjson was created)
            categories_ndjson_path = f"merchants/{merchant_id}/training_files/categories.ndjson"
            if gcs_handler.file_exists(categories_ndjson_path):
                try:
                    gcs_uri = f"gs://{gcs_handler.bucket_name}/{categories_ndjson_path}"
                    vertex_setup.import_documents(merchant_id, gcs_uri, import_type="FULL")
                    import_success.append("categories")
                except Exception as import_error:
                    error_msg = str(import_error)
                    import_errors.append(f"categories: {error_msg}")
                    logger.error(f"Failed to import categories: {error_msg}")

            # Build status message
            message = "Vertex AI Search datastore configured"
            if shop_url:
                message += f" with website crawling for {shop_url}"
            
            if import_success:
                message += f". Successfully imported: {', '.join(import_success)}"
            
            if import_errors:
                # Check if it's a permission error
                has_permission_error = any("IAM_PERMISSION_DENIED" in err or "Permission" in err for err in import_errors)
                if has_permission_error:
                    message += f". Import failed due to missing permissions. Run ./grant_vertex_permissions.sh to fix."
                    logger.warning(f"Vertex AI import failed due to permissions. Errors: {import_errors}")
                else:
                    message += f". Import errors: {len(import_errors)} file(s) failed"
                    logger.warning(f"Vertex AI import had errors: {import_errors}")
                
                # Don't fail the entire onboarding - mark as completed with warnings
                status_tracker.update_step_status(
                    merchant_id, "setup_vertex", StepStatus.COMPLETED,
                    message=message
                )
            else:
                status_tracker.update_step_status(
                    merchant_id, "setup_vertex", StepStatus.COMPLETED,
                    message=message
                )
        except Exception as e:
            error_msg = str(e)
            # Check if it's a permission error
            if "IAM_PERMISSION_DENIED" in error_msg or "Permission" in error_msg:
                message = f"Vertex AI setup failed: Missing permissions. Run ./grant_vertex_permissions.sh to grant required permissions."
                logger.error(f"Vertex AI setup failed due to permissions: {error_msg}")
                status_tracker.update_step_status(
                    merchant_id, "setup_vertex", StepStatus.FAILED,
                    error=message
                )
                # Don't raise - allow onboarding to continue even if Vertex setup fails
                logger.warning("Continuing onboarding despite Vertex AI setup failure")
            else:
                status_tracker.update_step_status(
                    merchant_id, "setup_vertex", StepStatus.FAILED,
                    error=error_msg
                )
                raise

        # Step 5: Generate config
        status_tracker.update_step_status(
            merchant_id, "generate_config", StepStatus.IN_PROGRESS
        )
        try:
            config_generator.generate_config(
                user_id=user_id,
                merchant_id=merchant_id,
                shop_name=shop_name,
                shop_url=shop_url,
                bot_name=bot_name,
                target_customer=target_customer,
                top_questions=top_questions,
                top_products=top_products,
                primary_color=primary_color,
                secondary_color=secondary_color,
                logo_url=logo_url
            )
            status_tracker.update_step_status(
                merchant_id, "generate_config", StepStatus.COMPLETED,
                message="Configuration generated successfully"
            )
        except Exception as e:
            status_tracker.update_step_status(
                merchant_id, "generate_config", StepStatus.FAILED,
                error=str(e)
            )
            raise

        # Step 6: Finalize
        status_tracker.update_step_status(
            merchant_id, "finalize", StepStatus.COMPLETED,
            message="Onboarding completed successfully"
        )

        logger.info(f"Onboarding completed for merchant: {merchant_id}")

    except Exception as e:
        logger.error(f"Onboarding failed for merchant {merchant_id}: {e}")
        status_tracker.update_step_status(
            merchant_id, "finalize", StepStatus.FAILED,
            error=str(e)
        )


# API Endpoints

@app.get("/")
async def root():
    """API information"""
    return {
        "service": "Merchant Onboarding API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "file_upload": "/files/upload-url",
            "file_upload_bulk": "/files/upload-urls",
            "file_confirm": "/files/confirm",
            "onboard": "/onboard",
            "status": "/onboard-status/{merchant_id}",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check if handlers are initialized
        if not all([gcs_handler, product_processor, document_converter, vertex_setup, config_generator]):
            raise HTTPException(status_code=503, detail="Service not fully initialized")

        # Check GCS connection
        try:
            gcs_handler.bucket.exists()
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"GCS connection failed: {str(e)}")

        # Check which credentials are configured for Vertex AI
        vertex_creds_info = {
            "VERTEX_CREDENTIALS_PATH": os.getenv("VERTEX_CREDENTIALS_PATH"),
            "VERTEX_CLIENT_EMAIL": os.getenv("VERTEX_CLIENT_EMAIL"),
            "VERTEX_PRIVATE_KEY": "***SET***" if os.getenv("VERTEX_PRIVATE_KEY") else None,
            "VERTEX_PROJECT_ID": os.getenv("VERTEX_PROJECT_ID"),
            "VERTEX_LOCATION": os.getenv("VERTEX_LOCATION"),
        }
        
        # Check which service account is actually being used
        actual_vertex_email = None
        if vertex_setup:
            try:
                # Try to get from stored service account email
                if hasattr(vertex_setup, '_service_account_email'):
                    actual_vertex_email = vertex_setup._service_account_email
                # Fallback: try to get from credentials
                elif hasattr(vertex_setup, 'client') and hasattr(vertex_setup.client, '_credentials'):
                    creds = vertex_setup.client._credentials
                    actual_vertex_email = (
                        getattr(creds, 'service_account_email', None) or
                        getattr(creds, '_service_account_email', None) or
                        (creds._key.get('client_email') if hasattr(creds, '_key') and isinstance(creds._key, dict) else None)
                    )
            except Exception as e:
                logger.debug(f"Could not determine service account email: {e}")
        
        return {
            "status": "healthy",
            "service": "Merchant Onboarding API",
            "handlers": {
                "gcs": "initialized",
                "product_processor": "initialized",
                "document_converter": "initialized",
                "vertex_setup": "initialized",
                "config_generator": "initialized"
            },
            "vertex_credentials": {
                "configured": vertex_creds_info,
                "actual_service_account": actual_vertex_email
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")


@app.post("/files/upload-url")
async def get_upload_url(
    filename: str = Form(...),
    content_type: str = Form(...),
    folder: str = Form(...),
    merchant_id: str = Form(...),
    expiration_minutes: int = Form(60)
):
    """
    Generate signed URL for direct file upload to GCS

    Frontend should:
    1. Call this endpoint to get signed URL (with merchant_id)
    2. Upload file directly to GCS using PUT request to signed URL
    3. Optionally call /files/confirm to verify upload

    Note: Frontend should validate user owns merchant_id before calling this endpoint
    """
    try:
        url_info = gcs_handler.generate_upload_url(
            merchant_id=merchant_id,
            folder=folder,
            filename=filename,
            content_type=content_type,
            expiration_minutes=expiration_minutes
        )
        return url_info
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error generating upload URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/files/upload-urls")
async def get_bulk_upload_urls(
    merchant_id: str = Form(...),
    files: str = Form(...)
):
    """
    Generate multiple signed URLs for bulk file uploads
    
    Args:
        merchant_id: Merchant identifier
        files: JSON string with array of file objects:
               [{"folder": "knowledge_base", "filename": "file1.pdf", "content_type": "application/pdf"}, ...]
    
    Returns:
        Array of upload URL objects
    """
    try:
        import json
        files_list = json.loads(files)
        
        if not isinstance(files_list, list):
            raise ValueError("files must be a JSON array")
        
        results = []
        for file_info in files_list:
            try:
                url_info = gcs_handler.generate_upload_url(
                    merchant_id=merchant_id,
                    folder=file_info["folder"],
                    filename=file_info["filename"],
                    content_type=file_info["content_type"],
                    expiration_minutes=file_info.get("expiration_minutes", 60)
                )
                results.append({
                    "filename": file_info["filename"],
                    "folder": file_info["folder"],
                    **url_info
                })
            except Exception as e:
                results.append({
                    "filename": file_info.get("filename", "unknown"),
                    "error": str(e)
                })
        
        return {
            "merchant_id": merchant_id,
            "count": len(results),
            "urls": results
        }
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in files parameter")
    except Exception as e:
        logger.error(f"Error generating bulk upload URLs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/files/confirm")
async def confirm_upload(object_path: str = Form(...)):
    """Confirm file upload was successful"""
    try:
        result = gcs_handler.confirm_upload(object_path)
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        logger.error(f"Error confirming upload: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/onboard")
async def start_onboarding(
    request: OnboardRequest,
    background_tasks: BackgroundTasks
):
    """
    Start merchant onboarding process

    This endpoint accepts merchant information and file paths (not files themselves).
    Files should be uploaded first using the /files/upload-url endpoint.
    """
    try:
        # Create job in status tracker
        job_id = status_tracker.create_job(request.merchant_id, request.user_id)

        # Start background processing
        background_tasks.add_task(
            process_onboarding,
            merchant_id=request.merchant_id,
            user_id=request.user_id,
            shop_name=request.shop_name,
            shop_url=request.shop_url,
            bot_name=request.bot_name,
            target_customer=request.target_customer,
            top_questions=request.top_questions,
            top_products=request.top_products,
            primary_color=request.primary_color,
            secondary_color=request.secondary_color,
            logo_url=request.logo_url,
            platform=request.platform,
            custom_url_pattern=request.custom_url_pattern,
            file_paths=request.file_paths
        )

        logger.info(f"Started onboarding job {job_id} for merchant {request.merchant_id}")

        return {
            "job_id": job_id,
            "merchant_id": request.merchant_id,
            "status": "started",
            "status_url": f"/onboard-status/{request.merchant_id}"
        }

    except Exception as e:
        logger.error(f"Error starting onboarding: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/onboard-status/{merchant_id}")
async def get_onboarding_status(merchant_id: str):
    """Get onboarding progress status"""
    try:
        status = status_tracker.get_status(merchant_id)
        if not status:
            raise HTTPException(status_code=404, detail="Onboarding job not found")

        return status

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)

