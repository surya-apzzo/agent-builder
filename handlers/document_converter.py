"""Document converter to NDJSON format for Vertex AI Search"""

import os
import json
import re
import base64
import logging
from typing import List, Dict, Any
from io import BytesIO
import PyPDF2
from docx import Document
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class DocumentConverter:
    """Convert documents to NDJSON format for Vertex AI Search"""

    def __init__(self, gcs_handler):
        """
        Initialize document converter

        Args:
            gcs_handler: GCSHandler instance
        """
        self.gcs_handler = gcs_handler

    def convert_documents(
        self,
        merchant_id: str,
        document_paths: List[str]
    ) -> Dict[str, Any]:
        """
        Convert multiple documents to NDJSON format

        Args:
            merchant_id: Merchant identifier
            document_paths: List of GCS paths to documents

        Returns:
            dict with path to generated NDJSON file and document count
        """
        try:
            all_documents = []
            skipped_files = []

            for doc_path in document_paths:
                # Validate file exists before processing
                if not self.gcs_handler.file_exists(doc_path):
                    logger.warning(f"File does not exist, skipping: {doc_path}")
                    skipped_files.append(doc_path)
                    continue
                
                try:
                    logger.info(f"Converting document: {doc_path}")
                    documents = self._convert_single_document(doc_path)
                    all_documents.extend(documents)
                except Exception as e:
                    logger.error(f"Error converting document {doc_path}: {e}")
                    skipped_files.append(doc_path)
                    # Continue with other files instead of failing completely
                    continue

            # Create NDJSON content
            ndjson_content = self._create_ndjson(all_documents)

            # Only upload if we have documents to convert
            if not all_documents:
                logger.warning("No documents were successfully converted")
                return {
                    "ndjson_path": None,
                    "document_count": 0,
                    "skipped_files": skipped_files
                }

            # Upload to GCS
            ndjson_path = f"merchants/{merchant_id}/training_files/documents.ndjson"
            self.gcs_handler.upload_file(
                ndjson_path,
                ndjson_content.encode('utf-8'),
                content_type="application/x-ndjson"
            )

            logger.info(f"Converted {len(all_documents)} documents to NDJSON: {ndjson_path}")
            if skipped_files:
                logger.warning(f"Skipped {len(skipped_files)} files: {skipped_files}")

            return {
                "ndjson_path": ndjson_path,
                "document_count": len(all_documents),
                "skipped_files": skipped_files if skipped_files else None
            }

        except Exception as e:
            logger.error(f"Error converting documents: {e}")
            raise

    def _convert_single_document(self, doc_path: str) -> List[Dict[str, Any]]:
        """
        Convert a single document to Vertex AI Search format

        Args:
            doc_path: GCS path to document

        Returns:
            List of document dictionaries
        """
        # Download document
        file_content = self.gcs_handler.download_file(doc_path)
        filename = os.path.basename(doc_path)

        # Determine file type and extract text
        if doc_path.endswith('.pdf'):
            text_content = self._extract_pdf_text(file_content)
        elif doc_path.endswith('.docx'):
            text_content = self._extract_docx_text(file_content)
        elif doc_path.endswith('.txt'):
            text_content = file_content.decode('utf-8', errors='ignore')
        elif doc_path.endswith('.html') or doc_path.endswith('.htm'):
            text_content = self._extract_html_text(file_content)
        else:
            logger.warning(f"Unsupported file type: {doc_path}, treating as text")
            text_content = file_content.decode('utf-8', errors='ignore')

        # Split into chunks if document is too large
        max_chunk_size = 10000  # characters per chunk
        chunks = self._split_text(text_content, max_chunk_size)

        documents = []
        for i, chunk in enumerate(chunks):
            # Create title
            doc_title = filename if i == 0 else f"{filename} (Part {i + 1})"
            
            # Create document ID - sanitize to match pattern [a-zA-Z0-9-_]*
            # Remove file extension and replace invalid characters with hyphens
            base_name = os.path.splitext(filename)[0]  # Remove extension
            original_id = f"{base_name}_{i}"
            # Sanitize: replace any character not in [a-zA-Z0-9-_] with hyphen
            sanitized_id = re.sub(r'[^a-zA-Z0-9-_]', '-', original_id)
            # Replace multiple consecutive hyphens with single hyphen
            sanitized_id = re.sub(r'-+', '-', sanitized_id)
            # Remove leading/trailing hyphens
            sanitized_id = sanitized_id.strip('-')
            # Ensure ID is not empty
            if not sanitized_id:
                sanitized_id = f"doc-{i}"
            
            # Build struct_data (title should be in struct_data, not at top level)
            struct_data = {
                "title": doc_title,
                "source": doc_path,
                "filename": filename,
                "chunk_index": i,
                "total_chunks": len(chunks)
            }
            
            # Encode content as base64 (matching working script format)
            content_bytes = chunk.encode('utf-8')
            content_base64 = base64.b64encode(content_bytes).decode('utf-8')
            
            # Create Vertex AI Search document format (matching working script)
            doc = {
                "id": sanitized_id,
                "content": {
                    "mime_type": "text/plain",
                    "raw_bytes": content_base64
                },
                "struct_data": struct_data
            }
            documents.append(doc)

        return documents

    def _extract_pdf_text(self, file_content: bytes) -> str:
        """Extract text from PDF"""
        try:
            pdf_file = BytesIO(file_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            text_parts = []

            for page in pdf_reader.pages:
                text_parts.append(page.extract_text())

            return '\n\n'.join(text_parts)
        except Exception as e:
            logger.error(f"Error extracting PDF text: {e}")
            raise

    def _extract_docx_text(self, file_content: bytes) -> str:
        """Extract text from DOCX"""
        try:
            docx_file = BytesIO(file_content)
            doc = Document(docx_file)
            text_parts = []

            for paragraph in doc.paragraphs:
                if paragraph.text.strip():
                    text_parts.append(paragraph.text)

            return '\n\n'.join(text_parts)
        except Exception as e:
            logger.error(f"Error extracting DOCX text: {e}")
            raise

    def _extract_html_text(self, file_content: bytes) -> str:
        """Extract text from HTML"""
        try:
            html_content = file_content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()

            # Get text
            text = soup.get_text()

            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)

            return text
        except Exception as e:
            logger.error(f"Error extracting HTML text: {e}")
            raise

    def _split_text(self, text: str, max_size: int) -> List[str]:
        """
        Split text into chunks

        Args:
            text: Text to split
            max_size: Maximum chunk size

        Returns:
            List of text chunks
        """
        if len(text) <= max_size:
            return [text]

        chunks = []
        current_chunk = ""

        # Try to split on paragraphs first
        paragraphs = text.split('\n\n')

        for paragraph in paragraphs:
            if len(current_chunk) + len(paragraph) + 2 <= max_size:
                current_chunk += paragraph + '\n\n'
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                # If paragraph itself is too large, split by sentences
                if len(paragraph) > max_size:
                    sentences = paragraph.split('. ')
                    for sentence in sentences:
                        if len(current_chunk) + len(sentence) + 2 <= max_size:
                            current_chunk += sentence + '. '
                        else:
                            if current_chunk:
                                chunks.append(current_chunk.strip())
                            current_chunk = sentence + '. '
                else:
                    current_chunk = paragraph + '\n\n'

        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks

    def _create_ndjson(self, documents: List[Dict[str, Any]]) -> str:
        """
        Convert documents list to NDJSON format

        Args:
            documents: List of document dictionaries

        Returns:
            NDJSON string
        """
        lines = []
        for doc in documents:
            lines.append(json.dumps(doc, ensure_ascii=False))
        return '\n'.join(lines)

