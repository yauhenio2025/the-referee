"""
Google Scholar Search Service using Oxylabs

Ported from the original gs-harvester JavaScript implementation.
Handles:
- Searching Google Scholar via Oxylabs proxy
- Parsing Scholar HTML pages
- Extracting paper metadata (title, authors, year, citations, Scholar ID)
- LLM-verified paper matching
"""
import httpx
import base64
import asyncio
import re
import logging
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote_plus

from ..config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class ScholarSearchService:
    """Service for searching Google Scholar via Oxylabs"""

    # Class-level cache for queries
    _query_cache: Dict[str, Dict] = {}

    def __init__(self):
        self.oxylabs_endpoint = "https://realtime.oxylabs.io/v1/queries"
        self.username = settings.oxylabs_username
        self.password = settings.oxylabs_password
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=60.0)
        return self._client

    async def close(self):
        """Close HTTP client"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    def _get_cache_key(self, query: str, language: str = "en") -> str:
        """Generate cache key for query"""
        return f"{language}:{query.lower().strip()}"

    async def search(
        self,
        query: str,
        language: str = "en",
        max_results: int = 20,
        year_low: Optional[int] = None,
        year_high: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Execute a search query on Google Scholar

        Args:
            query: Search query string
            language: Language code (default: en)
            max_results: Maximum results to fetch
            year_low: Filter results from this year
            year_high: Filter results to this year

        Returns:
            Dict with 'papers' list and 'totalResults' count
        """
        logger.info(f"[SCHOLAR SEARCH] Query: \"{query[:60]}...\" lang={language}")

        # Check cache
        cache_key = self._get_cache_key(query, language)
        if cache_key in self._query_cache:
            cached = self._query_cache[cache_key]
            if cached.get("papers"):
                logger.info(f"[CACHE HIT] {len(cached['papers'])} papers from cache")
                return {
                    "papers": cached["papers"][:max_results],
                    "totalResults": cached.get("totalResults", len(cached["papers"]))
                }

        # Build URL
        params = {
            "q": query,
            "hl": language,
            "as_sdt": "0,5",  # Search articles (not patents/legal)
        }
        if year_low:
            params["as_ylo"] = year_low
        if year_high:
            params["as_yhi"] = year_high

        base_url = f"https://scholar.google.com/scholar?{urlencode(params)}"

        papers = []
        total_results = None
        current_page = 0
        max_pages = (max_results + 9) // 10  # 10 results per page

        while len(papers) < max_results and current_page < max_pages:
            page_url = base_url if current_page == 0 else f"{base_url}&start={current_page * 10}"

            logger.info(f"Fetching page {current_page + 1}/{max_pages}...")
            html = await self._fetch_with_retry(page_url)

            if current_page == 0:
                total_results = self._extract_result_count(html)

            extracted = self._parse_scholar_page(html)

            if not extracted:
                logger.info(f"No results on page {current_page + 1}, stopping")
                break

            logger.info(f"✓ Extracted {len(extracted)} papers from page {current_page + 1}")
            papers.extend(extracted)
            current_page += 1

            # Rate limit between pages
            if current_page < max_pages and len(papers) < max_results:
                await asyncio.sleep(2)

        # Cache results
        if papers:
            self._query_cache[cache_key] = {
                "papers": papers,
                "totalResults": total_results or len(papers),
            }

        logger.info(f"Search complete: {len(papers)} papers found")

        return {
            "papers": papers[:max_results],
            "totalResults": total_results or len(papers)
        }

    async def get_cited_by(
        self,
        scholar_id: str,
        max_results: int = 200,
        year_low: Optional[int] = None,
        year_high: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get papers that cite a given paper

        Args:
            scholar_id: Google Scholar cluster ID
            max_results: Maximum results to fetch
            year_low/high: Year filters

        Returns:
            Dict with 'papers' list and 'totalResults' count
        """
        logger.info(f"[CITED BY] Scholar ID: {scholar_id}")

        params = {
            "hl": "en",
            "cites": scholar_id,
            "scipsc": "1",  # Search within citations
        }
        if year_low:
            params["as_ylo"] = year_low
        if year_high:
            params["as_yhi"] = year_high

        base_url = f"https://scholar.google.com/scholar?{urlencode(params)}"

        papers = []
        total_results = None
        current_page = 0
        max_pages = (max_results + 9) // 10

        while len(papers) < max_results and current_page < max_pages:
            page_url = base_url if current_page == 0 else f"{base_url}&start={current_page * 10}"

            logger.info(f"Fetching cited-by page {current_page + 1}/{max_pages}...")
            html = await self._fetch_with_retry(page_url)

            if current_page == 0:
                total_results = self._extract_result_count(html)

            extracted = self._parse_scholar_page(html)

            if not extracted:
                logger.info("No more citations, stopping")
                break

            logger.info(f"✓ Extracted {len(extracted)} citing papers from page {current_page + 1}")
            papers.extend(extracted)
            current_page += 1

            if current_page < max_pages and len(papers) < max_results:
                await asyncio.sleep(2)

        logger.info(f"Cited-by search complete: {len(papers)} citing papers")

        return {
            "papers": papers[:max_results],
            "totalResults": total_results or len(papers),
            "pages_fetched": current_page,
        }

    async def _fetch_with_retry(self, url: str, max_retries: int = 5) -> str:
        """Fetch URL via Oxylabs with retry logic"""
        last_error = None

        for attempt in range(max_retries):
            try:
                html = await self._fetch_via_oxylabs(url)
                return html
            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")

                if attempt < max_retries - 1:
                    backoff = 1000 * (2 ** attempt) / 1000  # Exponential backoff in seconds
                    await asyncio.sleep(backoff)

        raise last_error or Exception("All retry attempts failed")

    async def _fetch_via_oxylabs(self, url: str) -> str:
        """Fetch URL via Oxylabs SERP Scraper API"""
        if not self.username or not self.password:
            raise ValueError("Oxylabs credentials not configured")

        payload = {
            "source": "google",
            "url": url,
            "geo_location": "United States",
            "user_agent_type": "desktop",
        }

        auth_string = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()

        client = await self._get_client()

        response = await client.post(
            self.oxylabs_endpoint,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Basic {auth_string}",
            },
            json=payload,
        )

        if response.status_code != 200:
            raise Exception(f"Oxylabs API HTTP {response.status_code}")

        data = response.json()

        if data.get("error"):
            raise Exception(f"Oxylabs API error: {data['error']}")

        if data.get("results") and data["results"][0]:
            result = data["results"][0]
            content = result.get("content") or result.get("html") or result.get("body")
            if content:
                return content

        # Handle async job
        if data.get("job") and data["job"].get("id"):
            job_status = data["job"].get("status")
            if job_status == "faulted":
                raise Exception("Oxylabs job faulted")

            logger.info(f"[OXYLABS] Job {data['job']['id']} status: {job_status}, polling...")
            return await self._poll_oxylabs_job(data["job"]["id"])

        raise Exception("Invalid Oxylabs response format")

    async def _poll_oxylabs_job(self, job_id: str, max_attempts: int = 30) -> str:
        """Poll Oxylabs async job until completion"""
        auth_string = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        client = await self._get_client()

        for attempt in range(max_attempts):
            if attempt > 0:
                await asyncio.sleep(2)

            response = await client.get(
                f"https://data.oxylabs.io/v1/queries/{job_id}",
                headers={"Authorization": f"Basic {auth_string}"},
            )

            if response.status_code != 200:
                raise Exception(f"Job status check failed: HTTP {response.status_code}")

            data = response.json()
            status = data.get("status")

            logger.info(f"[OXYLABS POLL] Attempt {attempt + 1}: status={status}")

            if status == "done":
                # Fetch results
                results_response = await client.get(
                    f"https://data.oxylabs.io/v1/queries/{job_id}/results",
                    headers={"Authorization": f"Basic {auth_string}"},
                )

                if results_response.status_code != 200:
                    raise Exception(f"Results fetch failed: HTTP {results_response.status_code}")

                results_data = results_response.json()
                if results_data.get("results") and results_data["results"][0]:
                    result = results_data["results"][0]
                    content = result.get("content") or result.get("html") or result.get("body")
                    if content:
                        return content

                raise Exception("Job completed but no content in results")

            if status == "faulted":
                raise Exception("Job faulted during processing")

        raise Exception(f"Job polling timeout after {max_attempts} attempts")

    def _parse_scholar_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse Google Scholar HTML page and extract paper metadata"""
        soup = BeautifulSoup(html, "html.parser")
        papers = []

        # Try multiple selectors for paper containers
        selectors = [".gs_ri", ".gs_r.gs_scl", "div[data-cid]", ".gs_or"]
        elements = []

        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                logger.debug(f"Found {len(elements)} papers using selector: {selector}")
                break

        if not elements:
            logger.warning("No papers found with any selector")
            return papers

        for el in elements:
            try:
                # Get cluster ID (data-cid attribute)
                cluster_id = el.get("data-cid")
                if not cluster_id:
                    parent = el.find_parent(attrs={"data-cid": True})
                    if parent:
                        cluster_id = parent.get("data-cid")

                # Title and link
                title_el = el.select_one(".gs_rt a") or el.select_one(".gs_rt span[id]") or el.select_one("h3 a")
                if not title_el:
                    continue

                title = title_el.get_text(strip=True)
                link = title_el.get("href")

                if not title:
                    continue

                # Authors and publication info
                authors_el = el.select_one(".gs_a")
                authors_raw = authors_el.get_text(strip=True) if authors_el else ""

                # Parse authors: "Author1, Author2 - Publication, Year - Publisher"
                parts = authors_raw.split(" - ")
                authors_part = parts[0] if parts else ""
                publication_part = parts[1] if len(parts) > 1 else ""

                authors = [a.strip() for a in authors_part.split(",") if a.strip() and not re.match(r"^\d{4}$", a.strip())]

                year_match = re.search(r"\b(19|20)\d{2}\b", authors_raw)
                year = int(year_match.group(0)) if year_match else None

                # Venue
                venue = None
                if publication_part:
                    venue = re.sub(r"\b(19|20)\d{2}\b", "", publication_part).strip().rstrip(",")

                # Abstract
                abstract_el = el.select_one(".gs_rs")
                abstract = abstract_el.get_text(strip=True) if abstract_el else None

                # Citation count and Scholar ID
                citation_count = 0
                scholar_id = None

                cited_by_link = el.select_one("a[href*='cites=']")
                if cited_by_link:
                    cited_text = cited_by_link.get_text(strip=True)
                    count_match = re.search(r"Cited by (\d+)", cited_text)
                    if count_match:
                        citation_count = int(count_match.group(1))

                    href = cited_by_link.get("href", "")
                    id_match = re.search(r"cites=(\d+)", href)
                    if id_match:
                        scholar_id = id_match.group(1)

                # If no scholar_id from cited-by, use cluster_id
                if not scholar_id and cluster_id:
                    scholar_id = cluster_id

                papers.append({
                    "id": scholar_id or f"{title[:50].replace(' ', '-')}-{year}",
                    "scholarId": scholar_id,
                    "clusterId": cluster_id,
                    "title": title,
                    "authors": authors,
                    "authorsRaw": authors_raw,
                    "year": year,
                    "abstract": abstract,
                    "citationCount": citation_count,
                    "link": link,
                    "venue": venue,
                    "source": "google_scholar",
                })

            except Exception as e:
                logger.error(f"Error parsing paper element: {e}")
                continue

        return papers

    def _extract_result_count(self, html: str) -> Optional[int]:
        """Extract total result count from Scholar HTML"""
        # Patterns for different languages
        patterns = [
            r"About\s+([\d,\.]+)\s+results?",
            r"([\d,\.]+)\s+results?\s*\(",
            r"Environ\s+([\d\s]+)\s+résultats?",  # French
            r"Aproximadamente\s+([\d,\.]+)\s+resultados?",  # Spanish
            r"Ungefähr\s+([\d,\.]+)\s+Ergebnisse?",  # German
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                # Clean number - remove commas, dots, spaces
                clean_num = re.sub(r"[,\.\s]", "", match.group(1))
                try:
                    count = int(clean_num)
                    if count > 0:
                        return count
                except ValueError:
                    continue

        return None

    async def search_and_verify_match(
        self,
        title: str,
        author: Optional[str] = None,
        year: Optional[int] = None,
        publisher: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search for a paper by title/metadata and verify match using LLM

        Args:
            title: Paper title
            author: Optional author name
            year: Optional year
            publisher: Optional publisher/venue

        Returns:
            Dict with 'paper', 'verification', 'allResults'
        """
        # Build advanced query
        query_parts = []

        if author or year or publisher:
            query_parts.append(f'allintitle:"{title}"')
        else:
            query_parts.append(f'"{title}"')

        if author:
            # Use wildcard for author name matching
            clean_author = author.strip().lower()
            query_parts.append(f'author:"*{clean_author}*"')

        if publisher:
            query_parts.append(f'source:"{publisher}"')

        query = " ".join(query_parts)
        logger.info(f"[SEARCH+VERIFY] Query: {query}")

        results = await self.search(query, max_results=10)

        if not results.get("papers"):
            # Fallback to simple title search
            logger.info("[SEARCH+VERIFY] No results with metadata, trying title only...")
            results = await self.search(f'"{title}"', max_results=10)

        if not results.get("papers"):
            return {
                "paper": None,
                "verification": None,
                "allResults": [],
                "error": "No results found"
            }

        all_results = results["papers"]

        # Use LLM to verify the best match
        from .paper_verification import verify_scholar_match

        primary = all_results[0]
        alternatives = all_results[1:4]

        verification = await verify_scholar_match(title, author, year, primary, alternatives)

        best_match = primary
        if verification.get("betterMatch"):
            best_match = verification["betterMatch"]
            logger.info(f"[SEARCH+VERIFY] LLM found better match: {verification['betterMatch']['title'][:50]}...")

        return {
            "paper": best_match,
            "verification": {
                "verified": verification.get("verified", True),
                "confidence": verification.get("confidence", 0.8),
                "reason": verification.get("reason", ""),
            },
            "allResults": all_results,
        }


# Singleton instance
_scholar_service: Optional[ScholarSearchService] = None


def get_scholar_service() -> ScholarSearchService:
    """Get or create Scholar search service singleton"""
    global _scholar_service
    if _scholar_service is None:
        _scholar_service = ScholarSearchService()
    return _scholar_service
