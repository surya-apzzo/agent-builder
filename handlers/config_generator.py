"""Configuration generator for merchant setup"""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ConfigGenerator:
    """Generate merchant configuration JSON"""

    def __init__(self, gcs_handler):
        """
        Initialize config generator

        Args:
            gcs_handler: GCSHandler instance
        """
        self.gcs_handler = gcs_handler
        self.project_id = os.getenv("GCP_PROJECT_ID", "shopify-473015")
        self.location = os.getenv("GCP_LOCATION", "global")

    def generate_config(
        self,
        user_id: str,
        merchant_id: str,
        shop_name: str,
        shop_url: str,
        bot_name: Optional[str] = "AI Assistant",
        target_customer: Optional[str] = None,
        customer_persona: Optional[str] = None,
        bot_tone: Optional[str] = None,
        prompt_text: Optional[str] = None,
        top_questions: Optional[str] = None,
        top_products: Optional[str] = None,
        primary_color: Optional[str] = "#667eea",
        secondary_color: Optional[str] = "#764ba2",
        logo_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate merchant configuration JSON

        Args:
            user_id: User identifier
            merchant_id: Merchant identifier
            shop_name: Shop name
            shop_url: Shop URL
            bot_name: Bot name (default: AI Assistant)
            target_customer: Target customer description
            customer_persona: Detailed customer persona description
            bot_tone: Bot tone and personality
            prompt_text: Custom prompt text/guidelines
            top_questions: Top questions
            top_products: Top products
            primary_color: Primary color
            secondary_color: Secondary color
            logo_url: Logo URL

        Returns:
            dict with config path and content
        """
        try:
            # Get current timestamp in ISO format
            now = datetime.now(timezone.utc).isoformat()
            
            # Construct logo URL if provided (convert GCS path to full URL if needed)
            full_logo_url = logo_url
            if logo_url and not logo_url.startswith(('http://', 'https://')):
                # If it's a GCS path, convert to storage URL
                if logo_url.startswith('gs://'):
                    # Extract bucket and path from gs:// URL
                    parts = logo_url.replace('gs://', '').split('/', 1)
                    if len(parts) == 2:
                        bucket, path = parts
                        full_logo_url = f"https://storage.cloud.google.com/{bucket}/{path}"
                else:
                    # Assume it's a GCS path relative to bucket
                    full_logo_url = f"https://storage.cloud.google.com/{self.gcs_handler.bucket_name}/{logo_url}"
            
            # Build the complete config structure
            config = {
                "user_id": user_id,
                "merchant_id": merchant_id,
                "shop_name": shop_name,
                "shop_url": shop_url,
                "bot_name": bot_name,
                "products": {
                    "bucket_name": self.gcs_handler.bucket_name,
                    "file_path": f"merchants/{merchant_id}/prompt-docs/products.json"
                },
                "bigquery": {
                    "project_id": self.project_id,
                    "dataset_id": "chatbot_logs",
                    "table_id": "conversations"
                },
                "vertex_search": {
                    "project_id": self.project_id,
                    "location": self.location,
                    "datastore_id": f"{merchant_id}-engine"
                },
                "branding": {
                    "primary_color": primary_color or "#667eea",
                    "secondary_color": secondary_color or "#764ba2",
                    "logo_url": full_logo_url or ""
                },
                "metadata": {
                    "created_at": now,
                    "updated_at": now,
                    "version": "1.0"
                }
            }

            # Add optional fields (only if provided)
            if target_customer:
                config["target_customer"] = target_customer
            if customer_persona:
                config["customer_persona"] = customer_persona
            if bot_tone:
                config["bot_tone"] = bot_tone
            if prompt_text:
                config["prompt_text"] = prompt_text
            if top_questions:
                config["top_questions"] = top_questions
            if top_products:
                config["top_products"] = top_products

            # Upload config to GCS - Langflow expects merchant_config.json
            config_path = f"merchants/{merchant_id}/merchant_config.json"
            config_content = json.dumps(config, indent=4, ensure_ascii=False)
            self.gcs_handler.upload_file(
                config_path,
                config_content.encode('utf-8'),
                content_type="application/json"
            )

            logger.info(f"Generated and uploaded config: {config_path}")

            return {
                "config_path": config_path,
                "config": config
            }

        except Exception as e:
            logger.error(f"Error generating config: {e}")
            raise

