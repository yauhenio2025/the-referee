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
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote_plus

from ..config import get_settings
from .api_logger import log_api_call

logger = logging.getLogger(__name__)
settings = get_settings()

# Force immediate log output
def log_now(msg: str, level: str = "info"):
    """Log message and immediately flush to stdout"""
    timestamp = datetime.utcnow().strftime("%H:%M:%S")
    print(f"{timestamp} | scholar | {level.upper()} | {msg}", flush=True)
    sys.stdout.flush()

# Timeout constants
HTTP_TIMEOUT = 45.0  # 45s per HTTP request (increased for complex queries)
SEARCH_TOTAL_TIMEOUT = 180.0  # 3 minutes max per search query
FETCH_RETRY_TIMEOUT = 150.0  # 150s max for all retries combined


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
        log_now(f"[SCHOLAR SEARCH] Query: \"{query[:60]}...\" lang={language}")

        try:
            return await asyncio.wait_for(
                self._search_impl(query, language, max_results, year_low, year_high),
                timeout=SEARCH_TOTAL_TIMEOUT
            )
        except asyncio.TimeoutError:
            log_now(f"[SCHOLAR SEARCH] Total timeout ({SEARCH_TOTAL_TIMEOUT}s) exceeded for query")
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
                log_now(f"[CACHE HIT] {len(cached['papers'])} papers from cache")
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

            log_now(f"Fetching page {current_page + 1}/{max_pages}...")
            html = await self._fetch_with_retry(page_url)

            if current_page == 0:
                total_results = self._extract_result_count(html)

            extracted = self._parse_scholar_page(html)

            if not extracted:
                log_now(f"No results on page {current_page + 1}, stopping")
                break

            log_now(f"✓ Extracted {len(extracted)} papers from page {current_page + 1}")
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

        log_now(f"Search complete: {len(papers)} papers found")

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
        additional_query: Optional[str] = None,
        on_page_failed: Optional[callable] = None,
        language_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get papers that cite a given paper - WITH PAGE-BY-PAGE CALLBACK

        Args:
            scholar_id: Google Scholar cluster ID
            max_results: Maximum results to fetch
            year_low/high: Year filters
            on_page_complete: Callback(page_num, papers) called after each page - SAVE TO DB HERE
            start_page: Resume from this page (0-indexed)
            additional_query: Additional query terms to append (e.g., exclusions like -author:"Smith")
            on_page_failed: Callback(page_num, url, error) called when page fails - STORE FOR RETRY
            language_filter: Language restriction (e.g., "lang_en" for English only,
                           "lang_zh-CN|lang_zh-TW|lang_fr|..." for multiple non-English)

        Returns:
            Dict with 'papers' list, 'totalResults' count, 'last_page' for resume,
            plus 'failed_pages' list with details of pages that failed all retries
        """
        log_now(f"╔{'═'*60}╗")
        log_now(f"║  GET_CITED_BY ENTRY POINT")
        log_now(f"╠{'═'*60}╣")
        log_now(f"║  scholar_id: {scholar_id}")
        log_now(f"║  max_results: {max_results}")
        log_now(f"║  year_low: {year_low}, year_high: {year_high}")
        log_now(f"║  start_page: {start_page}")
        log_now(f"║  additional_query: {additional_query}")
        log_now(f"║  language_filter: {language_filter}")
        log_now(f"║  on_page_complete callback: {'SET' if on_page_complete else 'NOT SET'}")
        log_now(f"║  on_page_failed callback: {'SET' if on_page_failed else 'NOT SET'}")
        log_now(f"╚{'═'*60}╝")

        # No timeout wrapper - let it run, save pages as we go
        return await self._get_cited_by_impl(
            scholar_id, max_results, year_low, year_high, on_page_complete, start_page, additional_query, on_page_failed, language_filter
        )

    async def _get_cited_by_impl(
        self,
        scholar_id: str,
        max_results: int,
        year_low: Optional[int],
        year_high: Optional[int],
        on_page_complete: Optional[callable] = None,
        start_page: int = 0,
        additional_query: Optional[str] = None,
        on_page_failed: Optional[callable] = None,  # NEW: callback for failed pages
        language_filter: Optional[str] = None,  # Language restriction (e.g., "lang_en" or "lang_zh-CN|lang_fr|...")
    ) -> Dict[str, Any]:
        """Internal cited-by implementation with page-by-page callback for immediate DB saves

        Args:
            on_page_failed: async callback(page_num, url, error_msg) called when a page fails all retries.
                           Use this to store failed pages for later retry.
            language_filter: Language restriction parameter (lr=). Examples:
                           "lang_en" for English only
                           "lang_zh-CN|lang_zh-TW|lang_fr|lang_de|..." for multiple languages
        """
        # Build URL exactly like gs-harvester JS version
        # CRITICAL: scipsc=1 tells Scholar to search WITHIN citations, not just the paper
        base_url = f"https://scholar.google.com/scholar?hl=en"

        log_now(f"[CITED_BY_IMPL] ═══════════════════════════════════════════════")

        # Add language filter EARLY if specified (Google Scholar seems to need it before cites param)
        # Format: lr=lang_en or lr=lang_zh-CN%7Clang_zh-TW%7Clang_fr%7C...
        # URL-encode pipes as %7C for Oxylabs compatibility
        if language_filter:
            encoded_filter = language_filter.replace("|", "%7C")
            base_url += f"&lr={encoded_filter}"
            log_now(f"[CITED_BY_IMPL] Language filter: {language_filter} -> {encoded_filter}")

        base_url += f"&cites={scholar_id}&scipsc=1"
        log_now(f"[CITED_BY_IMPL] BASE URL (with scipsc=1): {base_url}")

        if year_low:
            base_url += f"&as_ylo={year_low}"
        if year_high:
            base_url += f"&as_yhi={year_high}"

        # Add exclusion/additional query terms (for overflow harvesting)
        if additional_query:
            # URL encode the additional query and append with &q=
            encoded_query = quote_plus(additional_query)
            base_url += f"&q={encoded_query}"
            log_now(f"[CITED_BY_IMPL] Additional query: {additional_query}")

        log_now(f"[CITED_BY_IMPL] FINAL BASE URL: {base_url}")

        all_papers = []
        failed_pages = []  # Track failed pages for retry
        total_results = None
        first_gs_count = None  # GS count from page 0 (for gap tracking)
        last_gs_count = None   # GS count from most recent page (may differ)
        current_page = start_page
        max_pages = (max_results + 9) // 10
        consecutive_failures = 0
        max_consecutive_failures = 3
        pages_succeeded = 0

        log_now(f"[CITED_BY_IMPL] max_pages calculated: {max_pages}")
        log_now(f"[CITED_BY_IMPL] Starting page loop...")

        while len(all_papers) < max_results and current_page < max_pages:
            page_url = base_url if current_page == 0 else f"{base_url}&start={current_page * 10}"

            log_now(f"[PAGE {current_page + 1}/{max_pages}] ───────────────────────────────")
            log_now(f"[PAGE {current_page + 1}] URL: {page_url}")

            try:
                log_now(f"[PAGE {current_page + 1}] Calling _fetch_with_retry...")
                html = await self._fetch_with_retry(page_url)
                log_now(f"[PAGE {current_page + 1}] HTML received, length: {len(html)} bytes")
                log_now(f"[PAGE {current_page + 1}] HTML preview: {html[:500]}...")

                # Extract GS count from EVERY page to detect estimate changes
                page_gs_count = self._extract_result_count(html)
                log_now(f"[PAGE {current_page + 1}] GS reports: {page_gs_count} results")

                if current_page == 0 or total_results is None:
                    total_results = page_gs_count
                    first_gs_count = page_gs_count
                    log_now(f"[PAGE {current_page + 1}] First GS count: {first_gs_count}")

                # Always track the most recent GS count (may differ from first)
                if page_gs_count is not None:
                    if last_gs_count is not None and page_gs_count != last_gs_count:
                        log_now(f"[PAGE {current_page + 1}] ⚠️ GS COUNT CHANGED: {last_gs_count} → {page_gs_count}")
                    last_gs_count = page_gs_count

                log_now(f"[PAGE {current_page + 1}] Calling _parse_scholar_page...")
                extracted = self._parse_scholar_page(html)
                log_now(f"[PAGE {current_page + 1}] Parse returned {len(extracted)} papers")

                if not extracted:
                    log_now(f"[PAGE {current_page + 1}] *** NO PAPERS EXTRACTED - stopping loop ***")
                    log_now(f"[PAGE {current_page + 1}] HTML snippet for debugging: {html[500:2000]}...")
                    break

                log_now(f"[PAGE {current_page + 1}] ✓ Extracted {len(extracted)} citing papers")
                for idx, paper in enumerate(extracted[:3]):
                    log_now(f"[PAGE {current_page + 1}]   [{idx}] {paper.get('title', 'NO TITLE')[:60]}...")

                # IMMEDIATE CALLBACK - save to DB NOW before anything can fail
                if on_page_complete:
                    log_now(f"[PAGE {current_page + 1}] Calling on_page_complete callback...")
                    log_now(f"[PAGE {current_page + 1}] Callback type: {type(on_page_complete)}")
                    log_now(f"[PAGE {current_page + 1}] Papers to save: {len(extracted)}")
                    try:
                        await on_page_complete(current_page, extracted)
                        log_now(f"[PAGE {current_page + 1}] ✓ Callback completed successfully")
                        # Log successful page fetch for activity stats
                        asyncio.create_task(log_api_call(
                            call_type='page_fetch',
                            count=1,
                            success=True,
                            extra_info=f"papers={len(extracted)}"
                        ))
                    except Exception as save_error:
                        log_now(f"[PAGE {current_page + 1}] ✗✗✗ CALLBACK FAILED ✗✗✗")
                        log_now(f"[PAGE {current_page + 1}] Error type: {type(save_error).__name__}")
                        log_now(f"[PAGE {current_page + 1}] Error message: {save_error}")
                        log_now(f"[PAGE {current_page + 1}] Traceback: {traceback.format_exc()}")
                        # Continue anyway - at least we tried
                else:
                    log_now(f"[PAGE {current_page + 1}] No callback set - papers not saved to DB")

                all_papers.extend(extracted)
                current_page += 1
                consecutive_failures = 0
                pages_succeeded += 1
                log_now(f"[PROGRESS] Total papers so far: {len(all_papers)}")

                if current_page < max_pages and len(all_papers) < max_results:
                    log_now(f"[RATE LIMIT] Sleeping 4 seconds before next page...")
                    await asyncio.sleep(4)

            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                consecutive_failures += 1
                log_now(f"[PAGE {current_page + 1}] ✗ FETCH FAILED ({consecutive_failures}/{max_consecutive_failures})")
                log_now(f"[PAGE {current_page + 1}] Error type: {type(e).__name__}")
                log_now(f"[PAGE {current_page + 1}] Error: {e}")
                log_now(f"[PAGE {current_page + 1}] Traceback: {traceback.format_exc()}")

                # RECORD THE FAILED PAGE for later retry
                failed_page_info = {
                    "page_number": current_page,
                    "url": page_url,
                    "error": error_msg,
                    "year_low": year_low,
                    "year_high": year_high,
                }
                failed_pages.append(failed_page_info)
                log_now(f"[PAGE {current_page + 1}] 📝 Recorded failed page for retry: page {current_page}")

                # Call the failure callback if provided (to store in DB immediately)
                if on_page_failed:
                    try:
                        await on_page_failed(current_page, page_url, error_msg)
                        log_now(f"[PAGE {current_page + 1}] ✓ Failure recorded via callback")
                    except Exception as cb_err:
                        log_now(f"[PAGE {current_page + 1}] ⚠️ Failed to record failure: {cb_err}")

                if consecutive_failures >= max_consecutive_failures:
                    log_now(f"[CITED_BY_IMPL] ✗✗✗ TOO MANY CONSECUTIVE FAILURES - STOPPING ✗✗✗")
                    log_now(f"[CITED_BY_IMPL] Stopped at page {current_page}. Saved {len(all_papers)} papers.")
                    log_now(f"[CITED_BY_IMPL] Failed pages recorded: {len(failed_pages)} - will retry later")
                    break

                current_page += 1
                await asyncio.sleep(5)

        # Determine if GS count changed during pagination (explains gaps)
        gs_count_changed = (first_gs_count is not None and last_gs_count is not None
                          and first_gs_count != last_gs_count)

        log_now(f"╔{'═'*60}╗")
        log_now(f"║  CITED_BY_IMPL COMPLETE")
        log_now(f"╠{'═'*60}╣")
        log_now(f"║  Total papers: {len(all_papers)}")
        log_now(f"║  Pages succeeded: {pages_succeeded}")
        log_now(f"║  Pages failed: {len(failed_pages)}")
        log_now(f"║  Last page: {current_page}")
        log_now(f"║  First GS count (page 0): {first_gs_count}")
        log_now(f"║  Last GS count (final page): {last_gs_count}")
        if gs_count_changed:
            log_now(f"║  ⚠️ GS COUNT CHANGED: {first_gs_count} → {last_gs_count}")
        log_now(f"╚{'═'*60}╝")

        return {
            "papers": all_papers[:max_results],
            "totalResults": total_results or len(all_papers),
            "pages_fetched": current_page,
            "pages_succeeded": pages_succeeded,
            "pages_failed": len(failed_pages),
            "failed_pages": failed_pages,  # Include failed page details
            "last_page": current_page,  # For resume
            # Gap tracking fields
            "first_gs_count": first_gs_count,  # GS count from page 0
            "last_gs_count": last_gs_count,    # GS count from final page (may differ)
            "gs_count_changed": gs_count_changed,  # True if estimate changed during scraping
        }

    async def verify_last_page(
        self,
        scholar_id: str,
        expected_count: int,
        year_low: Optional[int] = None,
        year_high: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Verify that the last page of results exists and confirm actual count.

        This fetches the calculated last page based on expected_count to:
        1. Confirm results actually exist at that offset
        2. Get the actual result count from Scholar's page header
        3. Help identify if we're missing any pages

        Args:
            scholar_id: Google Scholar cluster ID
            expected_count: Expected number of results (from HarvestTarget or initial fetch)
            year_low/year_high: Year filters

        Returns:
            Dict with:
            - verified_count: Actual count from last page header
            - last_page_exists: Whether the last page returned results
            - last_page_papers: Number of papers on the last page
            - calculated_last_start: The start offset we calculated
            - actual_results_at_offset: Papers found at that offset
        """
        log_now(f"[VERIFY_LAST_PAGE] Verifying for scholar_id={scholar_id}, expected={expected_count}, year={year_low}-{year_high}")

        # Calculate the last page offset
        # If expected_count=648, last page starts at 640 (results 641-650) but more precisely:
        # Page 64 (0-indexed 63) would start at 630 and show results 631-640
        # Page 65 (0-indexed 64) would start at 640 and show results 641-650, but 648 means only 8 results
        # Actually Scholar starts at 0, so:
        # - 648 results means positions 1-648
        # - Last page start = ((648-1) // 10) * 10 = 640
        # But looking at the screenshot, start=638 shows "Page 64 of 648"
        # So Scholar's logic is: start = (total - 10) rounded to nearest 10?
        # Let's calculate: if 648 results, last full page ends at 640, so start=638 shows 639-648
        # Actually just do: last_start = max(0, expected_count - 10)
        # Hmm, let me think more carefully:
        # - start=0 shows results 1-10
        # - start=10 shows results 11-20
        # - start=630 shows results 631-640
        # - start=638 shows "Page 64 of 648" - results 639-648 (10 results)
        # Wait, that's not right. If start=638, that's result 639 onwards.
        # 648 - 638 = 10, so it shows results 639-648 (all 10)
        # So the calculation is: last_start = (expected_count - 1) // 10 * 10
        # For 648: (647) // 10 * 10 = 640, but screenshot shows 638...
        # Hmm, let me look again at the URL: start=638
        # 648 total, start=638 means showing result 639 onwards
        # Page 64 = start 630 would show 631-640, that's only 10 results
        # Wait, the screenshot says "Page 64 of 648 results"
        # If start=638, and page=64, then pages are 10 results each
        # 64 * 10 = 640, but start=638? That's confusing.
        # Let me just calculate: last_start = max(0, ((expected_count - 1) // 10) * 10)
        # For 648: ((647) // 10) * 10 = 640
        # But we need to be a bit more conservative to make sure we're on a page that exists
        # Let's use: last_start = max(0, expected_count - 10) if expected_count > 10 else 0
        # For 648: 648 - 10 = 638. That matches the screenshot!

        if expected_count <= 0:
            return {
                "verified_count": 0,
                "last_page_exists": False,
                "last_page_papers": 0,
                "calculated_last_start": 0,
                "actual_results_at_offset": 0,
                "error": "Expected count is zero or negative"
            }

        # Calculate last page start offset
        # Scholar shows 10 results per page, start=0 is first page
        # If we have 648 results, start=638 shows the last 10 (or fewer)
        last_start = max(0, expected_count - 10)

        # Build URL
        base_url = f"https://scholar.google.com/scholar?hl=en&cites={scholar_id}&scipsc=1"
        if year_low:
            base_url += f"&as_ylo={year_low}"
        if year_high:
            base_url += f"&as_yhi={year_high}"

        page_url = f"{base_url}&start={last_start}"
        log_now(f"[VERIFY_LAST_PAGE] Fetching last page: {page_url}")

        try:
            html = await self._fetch_with_retry(page_url)
            verified_count = self._extract_result_count(html)
            papers = self._parse_scholar_page(html)

            result = {
                "verified_count": verified_count,
                "last_page_exists": len(papers) > 0,
                "last_page_papers": len(papers),
                "calculated_last_start": last_start,
                "actual_results_at_offset": len(papers),
                "expected_count": expected_count,
                "discrepancy": abs(verified_count - expected_count) if verified_count else None,
            }

            log_now(f"[VERIFY_LAST_PAGE] Result: verified={verified_count}, papers_on_page={len(papers)}, expected={expected_count}")

            if verified_count and verified_count != expected_count:
                log_now(f"[VERIFY_LAST_PAGE] ⚠️ DISCREPANCY: Scholar says {verified_count}, we expected {expected_count}")

            return result

        except Exception as e:
            log_now(f"[VERIFY_LAST_PAGE] ✗ Failed to fetch last page: {e}")
            return {
                "verified_count": None,
                "last_page_exists": False,
                "last_page_papers": 0,
                "calculated_last_start": last_start,
                "actual_results_at_offset": 0,
                "error": str(e)
            }

    async def fetch_specific_page(
        self,
        scholar_id: str,
        page_start: int,
        year_low: Optional[int] = None,
        year_high: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Fetch a specific page of citation results.

        Used for gap-filling when specific pages were missed.

        Args:
            scholar_id: Google Scholar cluster ID
            page_start: The start offset (0, 10, 20, etc.)
            year_low/year_high: Year filters

        Returns:
            Dict with papers list and metadata
        """
        log_now(f"[FETCH_SPECIFIC_PAGE] scholar_id={scholar_id}, start={page_start}, year={year_low}-{year_high}")

        # Build URL
        base_url = f"https://scholar.google.com/scholar?hl=en&cites={scholar_id}&scipsc=1"
        if year_low:
            base_url += f"&as_ylo={year_low}"
        if year_high:
            base_url += f"&as_yhi={year_high}"

        page_url = f"{base_url}&start={page_start}" if page_start > 0 else base_url

        try:
            html = await self._fetch_with_retry(page_url)
            total_results = self._extract_result_count(html)
            papers = self._parse_scholar_page(html)

            log_now(f"[FETCH_SPECIFIC_PAGE] Got {len(papers)} papers from start={page_start}")

            return {
                "papers": papers,
                "total_results": total_results,
                "page_start": page_start,
                "success": True,
            }

        except Exception as e:
            log_now(f"[FETCH_SPECIFIC_PAGE] ✗ Failed: {e}")
            return {
                "papers": [],
                "total_results": None,
                "page_start": page_start,
                "success": False,
                "error": str(e),
            }

    async def _fetch_with_retry(self, url: str, max_retries: int = 50) -> str:
        """
        Fetch URL via Oxylabs with persistent retry logic.

        Keep retrying Oxylabs until FETCH_RETRY_TIMEOUT (150s) is exhausted.
        Oxylabs is reliable - transient faults recover quickly with retries.
        Direct scraping fallback is unreliable (Google blocks it), so we
        prioritize persistent Oxylabs retries.
        """
        last_error = None
        attempt = 0

        try:
            # Wrap all retries in a total timeout - keep trying until exhausted
            async with asyncio.timeout(FETCH_RETRY_TIMEOUT):
                while attempt < max_retries:
                    try:
                        html = await self._fetch_via_oxylabs(url)
                        if attempt > 0:
                            log_now(f"✓ Oxylabs succeeded on attempt {attempt + 1}")
                        return html
                    except asyncio.TimeoutError:
                        last_error = TimeoutError(f"HTTP request timed out on attempt {attempt + 1}")
                        log_now(f"Attempt {attempt + 1} timed out")
                    except Exception as e:
                        last_error = e
                        log_now(f"Attempt {attempt + 1} failed: {e}")

                    attempt += 1
                    # Exponential backoff capped at 8s: 1s, 2s, 4s, 8s, 8s, 8s...
                    backoff = min(2 ** min(attempt - 1, 3), 8)
                    log_now(f"  Retrying in {backoff}s...")
                    await asyncio.sleep(backoff)

        except asyncio.TimeoutError:
            log_now(f"Oxylabs exhausted after {attempt} attempts and {FETCH_RETRY_TIMEOUT}s timeout")
            # Only try direct as last resort
            try:
                return await self._fetch_direct(url, max_retries=2)
            except Exception as direct_error:
                log_now(f"Direct scraping also failed: {direct_error}")
                raise last_error or TimeoutError(f"All retries exhausted after {FETCH_RETRY_TIMEOUT}s total timeout")

        # If we somehow exit the loop (shouldn't happen with high max_retries)
        log_now(f"Oxylabs exhausted after {attempt} attempts")
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

        # Log the Oxylabs API call
        success = response.status_code == 200
        asyncio.create_task(log_api_call(
            call_type='oxylabs',
            success=success,
            extra_info=f"status={response.status_code}"
        ))

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

            log_now(f"[OXYLABS] Job {data['job']['id']} status: {job_status}, polling...")
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

                log_now(f"[OXYLABS POLL] Attempt {attempt + 1}/{max_attempts}: status={status}")

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
                log_now(f"[OXYLABS POLL] Attempt {attempt + 1} timed out")
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
                    log_now(f"✓ Direct scraping succeeded on attempt {attempt + 1}")
                return html

            except Exception as e:
                if attempt == max_retries - 1:
                    log_now(f"Direct fetch attempt {attempt + 1}/{max_retries} failed: {e} - all methods exhausted")
                    raise
                else:
                    log_now(f"Direct fetch attempt {attempt + 1}/{max_retries} failed: {e}")
                    # Longer backoff for direct scraping: 5s, 10s (matches JS)
                    backoff = 5.0 * (attempt + 1)
                    log_now(f"  Retrying in {backoff}s...")
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
                log_now(f"Found {len(elements)} papers using selector: {selector}")
                break

        if not elements:
            log_now("No papers found with any selector")
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

                # Extract author profile URLs from .gs_a links
                # Links to Scholar profiles have href like "/citations?user=ABC123&..."
                author_profiles = []
                if authors_el:
                    for link in authors_el.find_all("a"):
                        href = link.get("href", "")
                        name = link.get_text(strip=True)
                        if name and "citations?user=" in href:
                            # Convert relative URL to absolute
                            if href.startswith("/"):
                                href = f"https://scholar.google.com{href}"
                            author_profiles.append({"name": name, "profile_url": href})
                        elif name and not href.startswith("http"):
                            # Author without Scholar profile
                            author_profiles.append({"name": name, "profile_url": None})

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
                    "authorProfiles": author_profiles,  # [{name, profile_url}, ...]
                    "year": year,
                    "abstract": abstract,
                    "citationCount": citation_count,
                    "link": link,
                    "venue": venue,
                    "source": "google_scholar",
                })

            except Exception as e:
                log_now(f"Error parsing paper element: {e}")
                continue

        return papers

    def _extract_result_count(self, html: str) -> Optional[int]:
        """Extract total result count from Scholar HTML"""
        # Patterns for different languages
        patterns = [
            r"About\s+([\d,\.\s]+)\s+results?",  # Added \s to capture space-separated thousands
            r"([\d,\.\s]+)\s+results?\s*\(",
            r"Environ\s+([\d\s]+)\s+résultats?",  # French
            r"Aproximadamente\s+([\d,\.]+)\s+resultados?",  # Spanish
            r"Ungefähr\s+([\d,\.]+)\s+Ergebnisse?",  # German
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                raw_match = match.group(1)
                # Clean number - remove commas, dots, spaces
                clean_num = re.sub(r"[,\.\s]", "", raw_match)
                try:
                    count = int(clean_num)
                    if count > 0:
                        # Log for debugging parsing issues (catches truncation bugs)
                        log_now(f"[COUNT PARSE] raw='{raw_match}' -> clean='{clean_num}' -> {count}")
                        return count
                except ValueError:
                    log_now(f"[COUNT PARSE ERROR] raw='{raw_match}' -> clean='{clean_num}' FAILED")
                    continue

        # Log when no count found - helps debug HTML format changes
        # Look for any "results" text to see what format Scholar is using
        results_context = re.search(r'.{0,30}results.{0,30}', html, re.IGNORECASE)
        if results_context:
            log_now(f"[COUNT PARSE] No match found. Context: '{results_context.group(0)}'")

        return None

    async def get_year_citation_count(self, scholar_id: str, year: int) -> Optional[int]:
        """
        Query Scholar to get the citation count for a specific year.

        Useful for missing year gaps where we don't know how many citations exist.
        Makes a lightweight query to Scholar and parses "About X results" count.

        Args:
            scholar_id: The cluster ID to get citations for
            year: The specific year to check

        Returns:
            The citation count for that year, or None if query fails
        """
        url = f"https://scholar.google.com/scholar?hl=en&cites={scholar_id}&scipsc=1&as_ylo={year}&as_yhi={year}"

        try:
            html = await self._fetch_with_retry(url)
            if not html:
                return None

            count = self._extract_result_count(html)
            return count
        except Exception as e:
            log_now(f"Failed to get year count for {scholar_id}/{year}: {e}")
            return None

    async def get_paper_by_scholar_id(self, scholar_id: str) -> Optional[Dict[str, Any]]:
        """
        Look up a paper's metadata by its Google Scholar cluster/cites ID.

        Uses the cluster endpoint to fetch the work and parses the first result.
        This is useful for "quick add" where user provides just a Scholar ID.

        Args:
            scholar_id: The cluster/cites ID from Google Scholar

        Returns:
            Dict with paper metadata (title, authors, year, citationCount, etc.)
            or None if not found
        """
        url = f"https://scholar.google.com/scholar?cluster={scholar_id}&hl=en"
        log_now(f"[LOOKUP] Fetching paper by scholar_id: {scholar_id}")

        try:
            html = await self._fetch_with_retry(url)
            if not html:
                log_now(f"[LOOKUP] No HTML returned for {scholar_id}")
                return None

            papers = self._parse_scholar_page(html)
            if not papers:
                log_now(f"[LOOKUP] No papers found for cluster {scholar_id}")
                return None

            # Return the first (primary) result
            paper = papers[0]
            # Ensure the scholar_id is set correctly
            paper["scholarId"] = scholar_id
            log_now(f"[LOOKUP] Found: {paper.get('title', 'Unknown')[:60]}... ({paper.get('citationCount', 0)} citations)")
            return paper

        except Exception as e:
            log_now(f"[LOOKUP] Error looking up {scholar_id}: {e}")
            return None

    async def scrape_abstract_via_allintitle(
        self,
        title: str,
    ) -> Dict[str, Any]:
        """
        Scrape full abstract from Google Scholar using allintitle search.

        This uses the allintitle:"paper title" query which shows expanded abstracts
        for papers from major publishers (Taylor & Francis, Elsevier, etc.).

        The abstract is in the .gs_fma_abs selector when the search returns
        exactly one result and the publisher provides abstract data.

        Args:
            title: The exact paper title to search for

        Returns:
            Dict with:
                - abstract: The full abstract text (or None if not found)
                - success: Boolean indicating if abstract was found
                - source: 'allintitle_scrape' if successful
        """
        log_now(f"[ALLINTITLE ABSTRACT] Searching for: \"{title[:60]}...\"")

        try:
            # Build the allintitle query with quoted title
            query = f'allintitle:"{title}"'

            # Build URL - don't filter by language for abstract scraping
            params = {
                "q": query,
                "hl": "en",
                "as_sdt": "0,5",
            }
            url = f"https://scholar.google.com/scholar?{urlencode(params)}"

            log_now(f"[ALLINTITLE ABSTRACT] URL: {url}")

            # Fetch the page
            html = await self._fetch_with_retry(url)

            # Parse the HTML
            soup = BeautifulSoup(html, "html.parser")

            # Check result count - this works best with single results
            result_count_el = soup.select_one("#gs_ab_md")
            result_text = result_count_el.get_text() if result_count_el else ""

            # Look for the expanded abstract in .gs_fma_abs
            # This is shown when Scholar has full abstract data from publishers
            abstract_el = soup.select_one(".gs_fma_abs")

            if abstract_el:
                # Get all text from the abstract div, including nested elements
                abstract = abstract_el.get_text(separator=' ', strip=True)
                # Clean up whitespace
                abstract = re.sub(r'\s+', ' ', abstract).strip()

                if abstract and len(abstract) > 50:  # Reasonable abstract length
                    log_now(f"[ALLINTITLE ABSTRACT] ✓ Found abstract ({len(abstract)} chars): {abstract[:100]}...")
                    return {
                        "abstract": abstract,
                        "success": True,
                        "source": "allintitle_scrape",
                    }

            # Fallback: try .gs_rs (standard abstract snippet) - less complete but sometimes available
            snippet_el = soup.select_one(".gs_rs.gs_fma_s")
            if snippet_el:
                snippet = snippet_el.get_text(separator=' ', strip=True)
                snippet = re.sub(r'\s+', ' ', snippet).strip()
                # Remove trailing "..." and common truncation markers
                snippet = re.sub(r'\s*…\s*$', '', snippet)
                snippet = re.sub(r'\s*\.\.\.\s*$', '', snippet)

                if snippet and len(snippet) > 50:
                    log_now(f"[ALLINTITLE ABSTRACT] ✓ Found snippet ({len(snippet)} chars): {snippet[:100]}...")
                    return {
                        "abstract": snippet,
                        "success": True,
                        "source": "allintitle_scrape",
                        "is_snippet": True,  # Indicate it might be truncated
                    }

            # Also try the standard .gs_rs selector
            standard_snippet = soup.select_one(".gs_rs")
            if standard_snippet:
                snippet = standard_snippet.get_text(separator=' ', strip=True)
                snippet = re.sub(r'\s+', ' ', snippet).strip()

                if snippet and len(snippet) > 50:
                    log_now(f"[ALLINTITLE ABSTRACT] ✓ Found standard snippet ({len(snippet)} chars)")
                    return {
                        "abstract": snippet,
                        "success": True,
                        "source": "allintitle_scrape",
                        "is_snippet": True,
                    }

            log_now("[ALLINTITLE ABSTRACT] ✗ No abstract found on page")
            return {
                "abstract": None,
                "success": False,
                "source": None,
            }

        except Exception as e:
            log_now(f"[ALLINTITLE ABSTRACT] Error: {e}")
            return {
                "abstract": None,
                "success": False,
                "error": str(e),
            }

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
        log_now(f"[SEARCH+VERIFY] Query: {query}")

        results = await self.search(query, max_results=10)

        if not results.get("papers"):
            # Fallback to simple title search
            log_now("[SEARCH+VERIFY] No results with metadata, trying title only...")
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
            log_now(f"[SEARCH+VERIFY] LLM found better match: {verification['betterMatch']['title'][:50]}...")

        return {
            "paper": best_match,
            "verification": {
                "verified": verification.get("verified", True),
                "confidence": verification.get("confidence", 0.8),
                "reason": verification.get("reason", ""),
            },
            "allResults": all_results,
        }


    async def search_by_author(
        self,
        author_query: str,
        max_results: int = 100,
        start_page: int = 0,
        on_page_complete: Optional[callable] = None,
    ) -> Dict[str, Any]:
        """
        Execute an author search query on Google Scholar.

        Used for Thinker Bibliographies to find all works by a specific author.
        Returns ALL results by paginating through the full result set.

        Args:
            author_query: Author search query (e.g., 'author:"H Marcuse"' or 'author:"马尔库塞"')
            max_results: Maximum results to fetch (default 100 for safety)
            start_page: Page to start from (0-indexed, for resume)
            on_page_complete: Callback(page_num, papers) called after each page

        Returns:
            Dict with 'papers' list, 'totalResults' count, and pagination info
        """
        log_now(f"[AUTHOR SEARCH] Query: \"{author_query[:60]}...\" max={max_results}")

        try:
            return await asyncio.wait_for(
                self._search_by_author_impl(author_query, max_results, start_page, on_page_complete),
                timeout=SEARCH_TOTAL_TIMEOUT * 3  # Allow more time for large author searches
            )
        except asyncio.TimeoutError:
            log_now(f"[AUTHOR SEARCH] Total timeout exceeded for query")
            return {"papers": [], "totalResults": 0, "error": "Search timeout"}

    async def _search_by_author_impl(
        self,
        author_query: str,
        max_results: int,
        start_page: int,
        on_page_complete: Optional[callable],
    ) -> Dict[str, Any]:
        """Internal author search implementation with pagination"""

        # Build URL - author searches use regular search with author: operator
        # Don't restrict language for author searches - we want ALL works
        params = {
            "q": author_query,
            "hl": "en",
            "as_sdt": "0,5",  # Search articles (not patents/legal)
        }

        base_url = f"https://scholar.google.com/scholar?{urlencode(params)}"

        all_papers = []
        total_results = None
        current_page = start_page
        max_pages = (max_results + 9) // 10
        consecutive_failures = 0
        max_consecutive_failures = 3
        pages_succeeded = 0

        log_now(f"[AUTHOR SEARCH] Starting from page {start_page}, max_pages={max_pages}")

        while len(all_papers) < max_results and current_page < max_pages:
            page_url = base_url if current_page == 0 else f"{base_url}&start={current_page * 10}"

            log_now(f"[AUTHOR PAGE {current_page + 1}/{max_pages}] Fetching...")

            try:
                html = await self._fetch_with_retry(page_url)

                if current_page == start_page:
                    total_results = self._extract_result_count(html)
                    log_now(f"[AUTHOR SEARCH] Total results reported by GS: {total_results}")

                    # Adjust max_pages if we now know the actual count
                    if total_results:
                        actual_max_pages = min((total_results + 9) // 10, max_pages)
                        if actual_max_pages < max_pages:
                            max_pages = actual_max_pages
                            log_now(f"[AUTHOR SEARCH] Adjusted max_pages to {max_pages}")

                extracted = self._parse_scholar_page(html)

                if not extracted:
                    log_now(f"[AUTHOR PAGE {current_page + 1}] No results, stopping")
                    break

                log_now(f"[AUTHOR PAGE {current_page + 1}] ✓ Extracted {len(extracted)} papers")

                # Call page callback if provided
                if on_page_complete:
                    try:
                        await on_page_complete(current_page, extracted)
                        log_now(f"[AUTHOR PAGE {current_page + 1}] ✓ Callback completed")
                    except Exception as save_error:
                        log_now(f"[AUTHOR PAGE {current_page + 1}] ✗ Callback failed: {save_error}")

                all_papers.extend(extracted)
                current_page += 1
                consecutive_failures = 0
                pages_succeeded += 1

                # Rate limiting between pages
                if current_page < max_pages and len(all_papers) < max_results:
                    await asyncio.sleep(4)

            except Exception as e:
                consecutive_failures += 1
                log_now(f"[AUTHOR PAGE {current_page + 1}] ✗ Failed ({consecutive_failures}/{max_consecutive_failures}): {e}")

                if consecutive_failures >= max_consecutive_failures:
                    log_now(f"[AUTHOR SEARCH] Too many failures, stopping")
                    break

                current_page += 1
                await asyncio.sleep(5)

        log_now(f"[AUTHOR SEARCH] Complete: {len(all_papers)} papers from {pages_succeeded} pages")

        return {
            "papers": all_papers[:max_results],
            "totalResults": total_results or len(all_papers),
            "pages_fetched": current_page,
            "pages_succeeded": pages_succeeded,
            "last_page": current_page,
        }

    async def fetch_author_profile(self, profile_url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch and parse a Google Scholar author profile page.

        Extracts: full name, affiliation, homepage URL, and research topics.

        Args:
            profile_url: Full URL like https://scholar.google.com/citations?user=1X4qGg4AAAAJ

        Returns:
            Dict with profile data:
            {
                "scholar_user_id": "1X4qGg4AAAAJ",
                "full_name": "Gregory Jackson",
                "affiliation": "King's College London",
                "homepage_url": "https://example.com",
                "topics": ["Corporate governance", "Corporate social responsibility", ...]
            }
            or None if fetch fails
        """
        log_now(f"[AUTHOR PROFILE] Fetching: {profile_url[:60]}...")

        # Extract user ID from URL
        user_id_match = re.search(r"user=([A-Za-z0-9_-]+)", profile_url)
        if not user_id_match:
            log_now(f"[AUTHOR PROFILE] Could not extract user ID from: {profile_url}")
            return None

        scholar_user_id = user_id_match.group(1)

        try:
            html = await self._fetch_with_retry(profile_url)
            if not html:
                log_now(f"[AUTHOR PROFILE] No HTML returned for {profile_url}")
                return None

            return self._parse_author_profile(html, scholar_user_id, profile_url)

        except Exception as e:
            log_now(f"[AUTHOR PROFILE] Error fetching {profile_url}: {e}")
            return None

    def _parse_author_profile(self, html: str, scholar_user_id: str, profile_url: str) -> Dict[str, Any]:
        """Parse Google Scholar author profile HTML"""
        soup = BeautifulSoup(html, "html.parser")

        result = {
            "scholar_user_id": scholar_user_id,
            "profile_url": profile_url,
            "full_name": None,
            "affiliation": None,
            "homepage_url": None,
            "topics": [],
        }

        # Full name - in div#gsc_prf_in
        name_el = soup.select_one("#gsc_prf_in")
        if name_el:
            result["full_name"] = name_el.get_text(strip=True)
            log_now(f"[AUTHOR PROFILE] Name: {result['full_name']}")

        # Affiliation - first link with class gsc_prf_ila (institution link)
        affiliation_el = soup.select_one("a.gsc_prf_ila")
        if affiliation_el:
            result["affiliation"] = affiliation_el.get_text(strip=True)
            log_now(f"[AUTHOR PROFILE] Affiliation: {result['affiliation']}")

        # Homepage URL - look for "Homepage" link in the profile info section
        # It's typically in the div.gsc_prf_il containing "Verified email at..."
        # The homepage link says "Homepage" and links externally
        for link in soup.select("a"):
            text = link.get_text(strip=True)
            href = link.get("href", "")
            if text.lower() == "homepage" and href.startswith("http"):
                result["homepage_url"] = href
                log_now(f"[AUTHOR PROFILE] Homepage: {href}")
                break

        # Topics/Research interests - links with class gsc_prf_inta (interest tag)
        # These are the blue tags like "Corporate governance", "Industrial relations"
        topic_els = soup.select("a.gsc_prf_inta")
        for topic_el in topic_els:
            topic = topic_el.get_text(strip=True)
            if topic:
                result["topics"].append(topic)

        if result["topics"]:
            log_now(f"[AUTHOR PROFILE] Topics: {', '.join(result['topics'][:3])}...")

        return result


# Singleton instance
_scholar_service: Optional[ScholarSearchService] = None


def get_scholar_service() -> ScholarSearchService:
    """Get or create Scholar search service singleton"""
    global _scholar_service
    if _scholar_service is None:
        _scholar_service = ScholarSearchService()
    return _scholar_service
