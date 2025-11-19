"""Configuration generator for merchant setup"""

import os
import json
import logging
from typing import Dict, Any, Optional

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
            top_questions: Top questions
            top_products: Top products
            primary_color: Primary color
            secondary_color: Secondary color
            logo_url: Logo URL

        Returns:
            dict with config path and content
        """
        try:
            config = {
                "merchant_id": merchant_id,
                "user_id": user_id,
                "shop_name": shop_name,
                "shop_url": shop_url,
                "bot_name": bot_name,
                "products": {
                    "bucket_name": self.gcs_handler.bucket_name,
                    "file_path": f"merchants/{merchant_id}/prompt-docs/products.json"
                },
                "vertex_search": {
                    "project_id": self.project_id,
                    "location": self.location,
                    "datastore_id": f"{merchant_id}-engine"
                }
            }

            # Add optional fields
            if target_customer:
                config["target_customer"] = target_customer
            if top_questions:
                config["top_questions"] = top_questions
            if top_products:
                config["top_products"] = top_products
            if primary_color:
                config["primary_color"] = primary_color
            if secondary_color:
                config["secondary_color"] = secondary_color
            if logo_url:
                config["logo_url"] = logo_url

            # Upload config to GCS - Langflow expects merchant_config.json
            config_path = f"merchants/{merchant_id}/merchant_config.json"
            config_content = json.dumps(config, indent=2)
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

