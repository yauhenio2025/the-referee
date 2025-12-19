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
import traceback
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote_plus

from ..config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Timeout constants
HTTP_TIMEOUT = 30.0  # 30s per HTTP request (reduced from 60)
SEARCH_TOTAL_TIMEOUT = 120.0  # 2 minutes max per search query
FETCH_RETRY_TIMEOUT = 90.0  # 90s max for all retries combined


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
            # Explicit timeout config - connect fast, allow longer reads
            timeout = httpx.Timeout(
                connect=10.0,  # 10s to establish connection
                read=HTTP_TIMEOUT,  # 30s to read response
                write=10.0,  # 10s to send request
                pool=10.0,  # 10s to get connection from pool
            )
            self._client = httpx.AsyncClient(timeout=timeout)
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

        try:
            return await asyncio.wait_for(
                self._search_impl(query, language, max_results, year_low, year_high),
                timeout=SEARCH_TOTAL_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error(f"[SCHOLAR SEARCH] Total timeout ({SEARCH_TOTAL_TIMEOUT}s) exceeded for query")
            return {"papers": [], "totalResults": 0, "error": "Search timeout"}

    async def _search_impl(
        self,
        query: str,
        language: str,
        max_results: int,
        year_low: Optional[int],
        year_high: Optional[int],
    ) -> Dict[str, Any]:
        """Internal search implementation"""

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
            "hl": language,  # UI language
            "lr": f"lang_{language}",  # FILTER results by language (e.g., lang_it, lang_es)
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
        on_page_complete: Optional[callable] = None,
        start_page: int = 0,
    ) -> Dict[str, Any]:
        """
        Get papers that cite a given paper - WITH PAGE-BY-PAGE CALLBACK

        Args:
            scholar_id: Google Scholar cluster ID
            max_results: Maximum results to fetch
            year_low/high: Year filters
            on_page_complete: Callback(page_num, papers) called after each page - SAVE TO DB HERE
            start_page: Resume from this page (0-indexed)

        Returns:
            Dict with 'papers' list, 'totalResults' count, 'last_page' for resume
        """
        logger.info(f"╔{'═'*60}╗")
        logger.info(f"║  GET_CITED_BY ENTRY POINT")
        logger.info(f"╠{'═'*60}╣")
        logger.info(f"║  scholar_id: {scholar_id}")
        logger.info(f"║  max_results: {max_results}")
        logger.info(f"║  year_low: {year_low}, year_high: {year_high}")
        logger.info(f"║  start_page: {start_page}")
        logger.info(f"║  on_page_complete callback: {'SET' if on_page_complete else 'NOT SET'}")
        logger.info(f"╚{'═'*60}╝")

        # No timeout wrapper - let it run, save pages as we go
        return await self._get_cited_by_impl(
            scholar_id, max_results, year_low, year_high, on_page_complete, start_page
        )

    async def _get_cited_by_impl(
        self,
        scholar_id: str,
        max_results: int,
        year_low: Optional[int],
        year_high: Optional[int],
        on_page_complete: Optional[callable] = None,
        start_page: int = 0,
    ) -> Dict[str, Any]:
        """Internal cited-by implementation with page-by-page callback for immediate DB saves"""
        # Build URL exactly like gs-harvester JS version
        # CRITICAL: scipsc=1 tells Scholar to search WITHIN citations, not just the paper
        base_url = f"https://scholar.google.com/scholar?hl=en&cites={scholar_id}&scipsc=1"

        logger.info(f"[CITED_BY_IMPL] ═══════════════════════════════════════════════")
        logger.info(f"[CITED_BY_IMPL] BASE URL (with scipsc=1): {base_url}")

        if year_low:
            base_url += f"&as_ylo={year_low}"
        if year_high:
            base_url += f"&as_yhi={year_high}"

        logger.info(f"[CITED_BY_IMPL] FINAL BASE URL: {base_url}")

        all_papers = []
        total_results = None
        current_page = start_page
        max_pages = (max_results + 9) // 10
        consecutive_failures = 0
        max_consecutive_failures = 3

        logger.info(f"[CITED_BY_IMPL] max_pages calculated: {max_pages}")
        logger.info(f"[CITED_BY_IMPL] Starting page loop...")

        while len(all_papers) < max_results and current_page < max_pages:
            page_url = base_url if current_page == 0 else f"{base_url}&start={current_page * 10}"

            logger.info(f"[PAGE {current_page + 1}/{max_pages}] ───────────────────────────────")
            logger.info(f"[PAGE {current_page + 1}] URL: {page_url}")

            try:
                logger.info(f"[PAGE {current_page + 1}] Calling _fetch_with_retry...")
                html = await self._fetch_with_retry(page_url)
                logger.info(f"[PAGE {current_page + 1}] HTML received, length: {len(html)} bytes")
                logger.info(f"[PAGE {current_page + 1}] HTML preview: {html[:500]}...")

                if current_page == 0 or total_results is None:
                    total_results = self._extract_result_count(html)
                    logger.info(f"[PAGE {current_page + 1}] Extracted total_results: {total_results}")

                logger.info(f"[PAGE {current_page + 1}] Calling _parse_scholar_page...")
                extracted = self._parse_scholar_page(html)
                logger.info(f"[PAGE {current_page + 1}] Parse returned {len(extracted)} papers")

                if not extracted:
                    logger.info(f"[PAGE {current_page + 1}] *** NO PAPERS EXTRACTED - stopping loop ***")
                    logger.info(f"[PAGE {current_page + 1}] HTML snippet for debugging: {html[500:2000]}...")
                    break

                logger.info(f"[PAGE {current_page + 1}] ✓ Extracted {len(extracted)} citing papers")
                for idx, paper in enumerate(extracted[:3]):
                    logger.info(f"[PAGE {current_page + 1}]   [{idx}] {paper.get('title', 'NO TITLE')[:60]}...")

                # IMMEDIATE CALLBACK - save to DB NOW before anything can fail
                if on_page_complete:
                    logger.info(f"[PAGE {current_page + 1}] Calling on_page_complete callback...")
                    logger.info(f"[PAGE {current_page + 1}] Callback type: {type(on_page_complete)}")
                    logger.info(f"[PAGE {current_page + 1}] Papers to save: {len(extracted)}")
                    try:
                        await on_page_complete(current_page, extracted)
                        logger.info(f"[PAGE {current_page + 1}] ✓ Callback completed successfully")
                    except Exception as save_error:
                        logger.error(f"[PAGE {current_page + 1}] ✗✗✗ CALLBACK FAILED ✗✗✗")
                        logger.error(f"[PAGE {current_page + 1}] Error type: {type(save_error).__name__}")
                        logger.error(f"[PAGE {current_page + 1}] Error message: {save_error}")
                        logger.error(f"[PAGE {current_page + 1}] Traceback: {traceback.format_exc()}")
                        # Continue anyway - at least we tried
                else:
                    logger.info(f"[PAGE {current_page + 1}] No callback set - papers not saved to DB")

                all_papers.extend(extracted)
                current_page += 1
                consecutive_failures = 0
                logger.info(f"[PROGRESS] Total papers so far: {len(all_papers)}")

                if current_page < max_pages and len(all_papers) < max_results:
                    logger.info(f"[RATE LIMIT] Sleeping 2 seconds before next page...")
                    await asyncio.sleep(2)

            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"[PAGE {current_page + 1}] ✗ FETCH FAILED ({consecutive_failures}/{max_consecutive_failures})")
                logger.warning(f"[PAGE {current_page + 1}] Error type: {type(e).__name__}")
                logger.warning(f"[PAGE {current_page + 1}] Error: {e}")
                logger.warning(f"[PAGE {current_page + 1}] Traceback: {traceback.format_exc()}")

                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"[CITED_BY_IMPL] ✗✗✗ TOO MANY FAILURES - STOPPING ✗✗✗")
                    logger.error(f"[CITED_BY_IMPL] Stopped at page {current_page}. Saved {len(all_papers)} papers.")
                    break

                current_page += 1
                await asyncio.sleep(5)

        logger.info(f"╔{'═'*60}╗")
        logger.info(f"║  CITED_BY_IMPL COMPLETE")
        logger.info(f"╠{'═'*60}╣")
        logger.info(f"║  Total papers: {len(all_papers)}")
        logger.info(f"║  Pages fetched: {current_page}")
        logger.info(f"║  Total results (Scholar count): {total_results}")
        logger.info(f"╚{'═'*60}╝")

        return {
            "papers": all_papers[:max_results],
            "totalResults": total_results or len(all_papers),
            "pages_fetched": current_page,
            "last_page": current_page,  # For resume
        }

    async def _fetch_with_retry(self, url: str, max_retries: int = 5) -> str:
        """
        Fetch URL via Oxylabs with retry logic - matches gs-harvester JS exactly

        JS version uses: maxRetries = 5, then falls back to direct scraping
        """
        last_error = None

        try:
            # Wrap all retries in a total timeout
            async with asyncio.timeout(FETCH_RETRY_TIMEOUT):
                for attempt in range(max_retries):
                    try:
                        html = await self._fetch_via_oxylabs(url)
                        if attempt > 0:
                            logger.info(f"✓ Oxylabs succeeded on attempt {attempt + 1}")
                        return html
                    except asyncio.TimeoutError:
                        last_error = TimeoutError(f"HTTP request timed out on attempt {attempt + 1}")
                        logger.warning(f"Attempt {attempt + 1}/{max_retries} timed out")
                    except Exception as e:
                        last_error = e
                        logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")

                    if attempt < max_retries - 1:
                        # Exponential backoff: 1s, 2s, 4s, 8s (matches JS)
                        backoff = min(2 ** attempt, 8)
                        logger.info(f"  Retrying in {backoff}s...")
                        await asyncio.sleep(backoff)

        except asyncio.TimeoutError:
            logger.error(f"Oxylabs exhausted ({max_retries} attempts). Trying direct scraping fallback...")
            # FALLBACK: Try direct scraping like JS does
            try:
                return await self._fetch_direct(url, max_retries=2)
            except Exception as direct_error:
                logger.error(f"Direct scraping also failed: {direct_error}")
                raise last_error or TimeoutError(f"All retries exhausted after {FETCH_RETRY_TIMEOUT}s total timeout")

        # If we get here, all Oxylabs attempts failed - try direct scraping
        logger.warning(f"Oxylabs exhausted ({max_retries} attempts). Falling back to direct scraping...")
        try:
            return await self._fetch_direct(url, max_retries=2)
        except Exception as direct_error:
            logger.error(f"Direct scraping also failed: {direct_error}")
            raise last_error or Exception("All retry attempts failed")

    async def _fetch_via_oxylabs(self, url: str) -> str:
        """Fetch URL via Oxylabs SERP Scraper API - matches gs-harvester JS exactly"""
        if not self.username or not self.password:
            raise ValueError("Oxylabs credentials not configured")

        # Match JS exactly: const payload = { source: 'google', url: url };
        # DO NOT add extra parameters like geo_location, user_agent_type
        payload = {
            "source": "google",
            "url": url,
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

    async def _poll_oxylabs_job(self, job_id: str, max_attempts: int = 15) -> str:
        """Poll Oxylabs async job until completion (max 30 seconds)"""
        auth_string = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        client = await self._get_client()

        for attempt in range(max_attempts):
            if attempt > 0:
                await asyncio.sleep(2)  # 2s between polls = 30s max total

            try:
                response = await client.get(
                    f"https://data.oxylabs.io/v1/queries/{job_id}",
                    headers={"Authorization": f"Basic {auth_string}"},
                )

                if response.status_code != 200:
                    raise Exception(f"Job status check failed: HTTP {response.status_code}")

                data = response.json()
                status = data.get("status")

                logger.info(f"[OXYLABS POLL] Attempt {attempt + 1}/{max_attempts}: status={status}")

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

            except asyncio.TimeoutError:
                logger.warning(f"[OXYLABS POLL] Attempt {attempt + 1} timed out")
                continue

        raise TimeoutError(f"Oxylabs job polling timeout after {max_attempts} attempts (~{max_attempts * 2}s)")

    async def _fetch_direct(self, url: str, max_retries: int = 2) -> str:
        """
        Direct fetch fallback when Oxylabs fails - matches gs-harvester JS exactly

        JS version: fetchDirect(url, maxRetries = 2)
        More aggressive than Oxylabs since we use it as last resort
        """
        client = await self._get_client()

        for attempt in range(max_retries):
            try:
                # Timeout: 15s for first attempt, 30s for retry (matches JS)
                timeout = 15.0 + (attempt * 15.0)

                response = await client.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate",
                        "Connection": "keep-alive",
                    },
                    timeout=timeout,
                    follow_redirects=True,
                )

                if response.status_code != 200:
                    raise Exception(f"HTTP {response.status_code}")

                html = response.text

                # Check for CAPTCHA or blocking (matches JS)
                if "unusual traffic" in html.lower() or "captcha" in html.lower() or "recaptcha" in html.lower():
                    raise Exception("Google Scholar CAPTCHA detected")

                if len(html) < 500:
                    raise Exception("Response too short - likely blocked")

                if attempt > 0:
                    logger.info(f"✓ Direct scraping succeeded on attempt {attempt + 1}")
                return html

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Direct fetch attempt {attempt + 1}/{max_retries} failed: {e} - all methods exhausted")
                    raise
                else:
                    logger.warning(f"Direct fetch attempt {attempt + 1}/{max_retries} failed: {e}")
                    # Longer backoff for direct scraping: 5s, 10s (matches JS)
                    backoff = 5.0 * (attempt + 1)
                    logger.info(f"  Retrying in {backoff}s...")
                    await asyncio.sleep(backoff)

        raise Exception("Direct scraping failed after all attempts")

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

                # Use separator=' ' to preserve spaces between nested elements
                # Then normalize multiple spaces to single space
                title = title_el.get_text(separator=' ', strip=True)
                title = re.sub(r'\s+', ' ', title).strip()
                link = title_el.get("href")

                if not title:
                    continue

                # Authors and publication info
                authors_el = el.select_one(".gs_a")
                authors_raw = authors_el.get_text(separator=' ', strip=True) if authors_el else ""
                authors_raw = re.sub(r'\s+', ' ', authors_raw).strip()

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
                abstract = None
                if abstract_el:
                    abstract = abstract_el.get_text(separator=' ', strip=True)
                    abstract = re.sub(r'\s+', ' ', abstract).strip()

                # Citation count and Scholar ID
                citation_count = 0
                scholar_id = None

                cited_by_link = el.select_one("a[href*='cites=']")
                if cited_by_link:
                    cited_text = cited_by_link.get_text(strip=True)
                    # Match citation count in multiple languages:
                    # English: "Cited by 123", Spanish: "Citado por 123",
                    # French: "Cité 123 fois", German: "Zitiert von: 123", etc.
                    count_match = re.search(r"(\d+)", cited_text)
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
