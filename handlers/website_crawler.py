"""Website crawler for extracting content from merchant websites"""

import os
import json
import logging
import time
from typing import List, Dict, Any, Set
from urllib.parse import urljoin, urlparse, urlunparse
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class WebsiteCrawler:
    """Crawl website and extract content for Vertex AI Search"""

    def __init__(self, gcs_handler, max_pages: int = 50, max_depth: int = 3):
        """
        Initialize website crawler

        Args:
            gcs_handler: GCSHandler instance
            max_pages: Maximum number of pages to crawl
            max_depth: Maximum crawl depth
        """
        self.gcs_handler = gcs_handler
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def crawl_website(
        self,
        user_id: str,
        base_url: str,
        merchant_id: str
    ) -> Dict[str, Any]:
        """
        Crawl website and create NDJSON file for Vertex AI Search

        Args:
            user_id: User identifier
            base_url: Base URL of the website to crawl
            merchant_id: Merchant identifier

        Returns:
            dict with path to generated NDJSON file and page count
        """
        try:
            logger.info(f"Starting website crawl for: {base_url}")

            # Normalize base URL
            base_url = self._normalize_url(base_url)

            # Crawl pages
            pages = self._crawl_pages(base_url)

            # Convert to NDJSON format
            documents = self._pages_to_documents(pages, base_url, merchant_id)

            # Create NDJSON content
            ndjson_content = self._create_ndjson(documents)

            # Upload to GCS
            ndjson_path = f"users/{user_id}/training_files/website_content.ndjson"
            self.gcs_handler.upload_file(
                ndjson_path,
                ndjson_content.encode('utf-8'),
                content_type="application/x-ndjson"
            )

            logger.info(f"Crawled {len(pages)} pages and uploaded to: {ndjson_path}")

            return {
                "ndjson_path": ndjson_path,
                "page_count": len(pages),
                "base_url": base_url
            }

        except Exception as e:
            logger.error(f"Error crawling website: {e}")
            raise

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to ensure it has a scheme"""
        parsed = urlparse(url)
        if not parsed.scheme:
            url = 'https://' + url
        parsed = urlparse(url)
        # Remove trailing slash
        if parsed.path == '/':
            url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        return url.rstrip('/')

    def _crawl_pages(self, base_url: str) -> List[Dict[str, Any]]:
        """
        Crawl website pages

        Args:
            base_url: Base URL to start crawling from

        Returns:
            List of page dictionaries with url, title, content
        """
        visited: Set[str] = set()
        to_visit: List[tuple] = [(base_url, 0)]  # (url, depth)
        pages: List[Dict[str, Any]] = []

        base_domain = urlparse(base_url).netloc

        while to_visit and len(pages) < self.max_pages:
            current_url, depth = to_visit.pop(0)

            # Skip if already visited or too deep
            if current_url in visited or depth > self.max_depth:
                continue

            # Skip if not same domain
            if urlparse(current_url).netloc != base_domain:
                continue

            try:
                logger.info(f"Crawling: {current_url} (depth: {depth})")

                # Fetch page
                response = self.session.get(current_url, timeout=10, allow_redirects=True)
                response.raise_for_status()

                # Parse content
                soup = BeautifulSoup(response.content, 'html.parser')

                # Extract title
                title = soup.find('title')
                title_text = title.get_text().strip() if title else current_url

                # Remove script and style elements
                for script in soup(["script", "style", "nav", "footer", "header"]):
                    script.decompose()

                # Extract main content
                # Try to find main content area
                main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
                if main_content:
                    content = main_content.get_text(separator='\n', strip=True)
                else:
                    # Fallback to body
                    body = soup.find('body')
                    content = body.get_text(separator='\n', strip=True) if body else soup.get_text(separator='\n', strip=True)

                # Clean up whitespace
                lines = [line.strip() for line in content.splitlines() if line.strip()]
                content = '\n'.join(lines)

                # Only add if content is meaningful (more than 100 chars)
                if len(content) > 100:
                    pages.append({
                        "url": current_url,
                        "title": title_text,
                        "content": content,
                        "depth": depth
                    })
                    visited.add(current_url)

                    # Find links for next level
                    if depth < self.max_depth:
                        links = soup.find_all('a', href=True)
                        for link in links:
                            href = link['href']
                            absolute_url = urljoin(current_url, href)

                            # Remove fragments and query params for deduplication
                            parsed = urlparse(absolute_url)
                            clean_url = urlunparse((
                                parsed.scheme,
                                parsed.netloc,
                                parsed.path,
                                '', '', ''
                            )).rstrip('/')

                            # Add to visit list if not visited and same domain
                            if (clean_url not in visited and
                                urlparse(clean_url).netloc == base_domain and
                                clean_url not in [url for url, _ in to_visit]):
                                to_visit.append((clean_url, depth + 1))

                # Be polite - add delay between requests
                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                logger.warning(f"Failed to fetch {current_url}: {e}")
                continue
            except Exception as e:
                logger.warning(f"Error processing {current_url}: {e}")
                continue

        logger.info(f"Crawled {len(pages)} pages from {base_url}")
        return pages

    def _pages_to_documents(
        self,
        pages: List[Dict[str, Any]],
        base_url: str,
        merchant_id: str
    ) -> List[Dict[str, Any]]:
        """
        Convert crawled pages to Vertex AI Search document format

        Args:
            pages: List of page dictionaries
            base_url: Base URL
            merchant_id: Merchant identifier

        Returns:
            List of document dictionaries
        """
        documents = []

        for i, page in enumerate(pages):
            # Split large pages into chunks
            content = page['content']
            max_chunk_size = 10000  # characters per chunk

            if len(content) <= max_chunk_size:
                chunks = [content]
            else:
                chunks = self._split_text(content, max_chunk_size)

            for j, chunk in enumerate(chunks):
                doc = {
                    "id": f"website_{merchant_id}_{i}_{j}",
                    "title": page['title'] if j == 0 else f"{page['title']} (Part {j + 1})",
                    "content": chunk,
                    "structData": {
                        "source": "website_crawl",
                        "url": page['url'],
                        "base_url": base_url,
                        "chunk_index": j,
                        "total_chunks": len(chunks),
                        "depth": page['depth']
                    }
                }
                documents.append(doc)

        return documents

    def _split_text(self, text: str, max_size: int) -> List[str]:
        """Split text into chunks"""
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
        """Convert documents list to NDJSON format"""
        lines = []
        for doc in documents:
            lines.append(json.dumps(doc, ensure_ascii=False))
        return '\n'.join(lines)

