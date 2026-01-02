#!/usr/bin/env python3
"""
Reharvest Jameson 2013 using AUTHOR INITIAL SPLITTING strategy.

Instead of LLM-based content exclusion terms, we partition results by author initials:
1. First harvest all non-English results (by language)
2. For English results > 1000:
   - Exclude common author initials until count < 950
   - Harvest exclusion set
   - Then harvest each excluded initial as inclusion sets
3. Compare results with the standard approach

Usage:
    python scripts/reharvest_author_split.py --dry-run
    python scripts/reharvest_author_split.py --run
"""

import asyncio
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Set, Dict, Any, List

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load env BEFORE any other imports
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Configuration
PAPER_ID = 71
EDITION_ID = 1073
SCHOLAR_ID = "15603705792201309427"
TARGET_YEAR = 2025
MAX_RESULTS = 950  # Stay under 1000

# Author initials ordered by approximate frequency in academic publishing
AUTHOR_INITIALS = [
    "m", "s", "j", "d", "r", "c", "a", "p", "t", "k",
    "b", "l", "n", "h", "g", "w", "e", "f", "i", "v",
    "o", "u", "y", "z", "q", "x"
]

# Non-English language codes (from Google Scholar settings - NO Russian exists!)
# Valid: zh-CN, zh-TW, nl, fr, de, it, ja, ko, pl, pt, es, tr
NON_ENGLISH_LANGS = [
    "lang_zh-CN", "lang_zh-TW", "lang_ja", "lang_ko", "lang_de", "lang_fr",
    "lang_es", "lang_pt", "lang_it", "lang_nl", "lang_pl", "lang_tr"
]

# Global stats
STATS = {
    "oxylabs_requests": 0,
    "oxylabs_successes": 0,
    "oxylabs_failures": 0,
    "non_english_papers": 0,
    "english_papers": 0,
    "total_papers": 0,
    "new_papers": 0,
    "duplicates": 0,
}

# Logging
LOG_FILE = None
LOG_FILE_PATH = None


def log(msg: str, level: str = "INFO"):
    """Log to both console and file"""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{timestamp}] [{level:<5}] {msg}"
    print(line, flush=True)
    if LOG_FILE:
        LOG_FILE.write(line + "\n")
        LOG_FILE.flush()


class LoggingScholarService:
    """Wrapper around ScholarSearchService with detailed logging"""

    def __init__(self, real_service):
        self.service = real_service

    async def get_cited_by(self, **kwargs):
        """Wrap get_cited_by with logging"""
        global STATS

        year = kwargs.get('year_low', kwargs.get('year_high', '?'))
        lang = kwargs.get('language_filter', 'all')
        query = kwargs.get('additional_query', '')[:50] if kwargs.get('additional_query') else ''

        log(f"[OXYLABS] Requesting citations: year={year}, lang={lang}, query={query}...")
        STATS["oxylabs_requests"] += 1

        start_time = datetime.now()
        try:
            result = await self.service.get_cited_by(**kwargs)
            elapsed = (datetime.now() - start_time).total_seconds()

            total = result.get('totalResults', 0)
            papers = len(result.get('papers', []))
            pages = result.get('pages_fetched', 0)
            failed = result.get('pages_failed', 0)

            STATS["oxylabs_successes"] += 1
            log(f"[OXYLABS] SUCCESS in {elapsed:.1f}s: total={total}, papers={papers}, pages={pages}, failed={failed}")

            return result
        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            STATS["oxylabs_failures"] += 1
            log(f"[OXYLABS] FAILED in {elapsed:.1f}s: {e}", "ERROR")
            raise

    async def close(self):
        await self.service.close()


async def get_existing_citation_ids(year: int) -> Set[str]:
    """Get scholar IDs of citations we already have for this paper/year"""
    from app.database import engine

    async with engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT scholar_id FROM citations
            WHERE edition_id = :edition_id AND year = :year AND scholar_id IS NOT NULL
        """), {"edition_id": EDITION_ID, "year": year})
        ids = {row[0] for row in result.fetchall()}
    log(f"[DB] Found {len(ids)} existing citations for year {year}")
    return ids


async def get_count(scholar, query_suffix: str = "", lang: str = "lang_en") -> int:
    """Get result count for a query."""
    result = await scholar.get_cited_by(
        scholar_id=SCHOLAR_ID,
        max_results=10,
        year_low=TARGET_YEAR,
        year_high=TARGET_YEAR,
        language_filter=lang,
        additional_query=query_suffix if query_suffix else None,
    )
    return result.get('totalResults', 0)


async def harvest_query(scholar, existing_ids: Set[str], query_suffix: str = "",
                       lang: str = "lang_en", label: str = "") -> List[Dict]:
    """Harvest all papers for a given query."""
    global STATS

    papers = []
    new_count = 0

    result = await scholar.get_cited_by(
        scholar_id=SCHOLAR_ID,
        max_results=1000,  # Get as many as possible
        year_low=TARGET_YEAR,
        year_high=TARGET_YEAR,
        language_filter=lang,
        additional_query=query_suffix if query_suffix else None,
    )

    page_papers = result.get('papers', [])
    total = result.get('totalResults', 0)

    for paper in page_papers:
        scholar_id = paper.get('scholarId')  # API uses camelCase
        if scholar_id and scholar_id not in existing_ids:
            papers.append(paper)
            existing_ids.add(scholar_id)
            new_count += 1
        else:
            STATS["duplicates"] += 1

    log(f"  [{label}] Total={total}, Retrieved={len(page_papers)}, NEW={new_count}")

    return papers


async def find_exclusion_set(scholar) -> tuple:
    """Find which initials to exclude to get under MAX_RESULTS."""
    log("[STRATEGY] Finding optimal author initial exclusion set...")

    # Start with no exclusions
    base_count = await get_count(scholar, "", "lang_en")
    log(f"[STRATEGY] Base English count: {base_count}")

    if base_count <= MAX_RESULTS:
        log("[STRATEGY] Already under limit, no exclusion needed")
        return [], base_count

    # Progressively add exclusions
    excluded = []
    for initial in AUTHOR_INITIALS:
        excluded.append(initial)
        exclusion_query = " ".join([f'-author:"{i} "' for i in excluded])

        count = await get_count(scholar, exclusion_query, "lang_en")
        log(f"[STRATEGY] Excluding {excluded}: {count} results")

        if count <= MAX_RESULTS:
            return excluded, count

        # Rate limit
        await asyncio.sleep(2)

    # If we excluded all and still over, return what we have
    log("[STRATEGY] WARNING: Could not get under limit even with all exclusions!")
    return excluded, count


async def save_papers_to_db(papers: List[Dict]) -> int:
    """Save harvested papers to database."""
    from app.database import engine

    saved = 0
    now = datetime.now()
    async with engine.begin() as conn:
        for paper in papers:
            try:
                await conn.execute(text("""
                    INSERT INTO citations (
                        paper_id, edition_id, scholar_id, title, authors,
                        venue, year, link, abstract, citation_count,
                        intersection_count, created_at
                    ) VALUES (
                        :paper_id, :edition_id, :scholar_id, :title, :authors,
                        :venue, :year, :link, :abstract, :citation_count,
                        :intersection_count, :created_at
                    )
                    ON CONFLICT (paper_id, scholar_id) DO NOTHING
                """), {
                    "paper_id": PAPER_ID,
                    "edition_id": EDITION_ID,
                    "scholar_id": paper.get("scholarId"),
                    "title": paper.get("title"),
                    "authors": ", ".join(paper.get("authors", [])) if isinstance(paper.get("authors"), list) else paper.get("authors"),
                    "venue": paper.get("venue"),
                    "year": paper.get("year"),
                    "link": paper.get("link"),
                    "abstract": paper.get("abstract") or "",
                    "citation_count": paper.get("citationCount", 0),
                    "intersection_count": 0,
                    "created_at": now,
                })
                saved += 1
            except Exception as e:
                log(f"[DB] Failed to save paper: {e}", "ERROR")
    return saved


async def run(dry_run: bool = True):
    """Main harvest logic."""
    global LOG_FILE, LOG_FILE_PATH, STATS

    # Setup logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode_str = "dryrun" if dry_run else "run"
    LOG_FILE_PATH = f"author_split_{TARGET_YEAR}_{timestamp}_{mode_str}.log"
    json_filename = f"author_split_{TARGET_YEAR}_{timestamp}.json"
    LOG_FILE = open(LOG_FILE_PATH, "w")

    log("=" * 80)
    log("+=" + "=" * 76 + "+")
    log("| AUTHOR INITIAL SPLIT HARVESTER" + " " * 45 + "|")
    log(f"| Paper: Jameson - Postmodernism (ID {PAPER_ID})" + " " * 35 + "|")
    log(f"| Year: {TARGET_YEAR}" + " " * 63 + "|")
    log(f"| Mode: {'DRY RUN' if dry_run else 'LIVE RUN'}" + " " * (61 if dry_run else 60) + "|")
    log("+=" + "=" * 76 + "+")
    log("=" * 80)

    # Initialize
    from app.services.scholar_search import ScholarSearchService
    real_scholar = ScholarSearchService()
    scholar = LoggingScholarService(real_scholar)

    existing_ids = await get_existing_citation_ids(TARGET_YEAR)
    all_papers = []

    # ========== PHASE 1: Non-English Languages ==========
    log("")
    log("=" * 60)
    log("PHASE 1: Non-English Languages")
    log("=" * 60)

    for lang in NON_ENGLISH_LANGS:
        lang_name = lang.replace("lang_", "")
        count = await get_count(scholar, "", lang)

        if count > 0:
            log(f"[{lang_name}] {count} papers - harvesting...")
            papers = await harvest_query(scholar, existing_ids, "", lang, lang_name)
            all_papers.extend(papers)
            STATS["non_english_papers"] += len(papers)
        else:
            log(f"[{lang_name}] 0 papers - skipping")

        await asyncio.sleep(2)

    log(f"[PHASE 1 COMPLETE] {STATS['non_english_papers']} non-English papers")

    # ========== PHASE 2: English with Author Split ==========
    log("")
    log("=" * 60)
    log("PHASE 2: English (Author Initial Split Strategy)")
    log("=" * 60)

    # Find exclusion set
    excluded_initials, exclusion_count = await find_exclusion_set(scholar)

    if not excluded_initials:
        # No split needed - harvest directly
        log("[ENGLISH] Harvesting all English papers directly...")
        papers = await harvest_query(scholar, existing_ids, "", "lang_en", "EN-ALL")
        all_papers.extend(papers)
        STATS["english_papers"] += len(papers)
    else:
        # Step 2a: Harvest exclusion set (papers NOT by excluded initials)
        exclusion_query = " ".join([f'-author:"{i} "' for i in excluded_initials])
        log(f"[ENGLISH] Step 2a: Harvesting exclusion set ({exclusion_count} papers)")
        log(f"[ENGLISH] Query: {exclusion_query[:80]}...")

        papers = await harvest_query(scholar, existing_ids, exclusion_query, "lang_en", "EN-EXCL")
        all_papers.extend(papers)
        STATS["english_papers"] += len(papers)

        await asyncio.sleep(5)

        # Step 2b: Harvest each excluded initial as inclusion
        log(f"[ENGLISH] Step 2b: Harvesting {len(excluded_initials)} inclusion sets...")

        for initial in excluded_initials:
            inclusion_query = f'author:"{initial} "'
            count = await get_count(scholar, inclusion_query, "lang_en")

            if count > 0:
                log(f"[EN-{initial.upper()}] {count} papers - harvesting...")

                if count > 1000:
                    log(f"[EN-{initial.upper()}] WARNING: Over 1000! Will only get first 1000")

                papers = await harvest_query(scholar, existing_ids, inclusion_query, "lang_en", f"EN-{initial.upper()}")
                all_papers.extend(papers)
                STATS["english_papers"] += len(papers)
            else:
                log(f"[EN-{initial.upper()}] 0 papers - skipping")

            await asyncio.sleep(3)

    # ========== SUMMARY ==========
    log("")
    log("=" * 60)
    log("FINAL SUMMARY")
    log("=" * 60)

    STATS["total_papers"] = len(all_papers)
    STATS["new_papers"] = STATS["total_papers"]

    log(f"Oxylabs requests: {STATS['oxylabs_requests']}")
    log(f"Oxylabs successes: {STATS['oxylabs_successes']}")
    log(f"Oxylabs failures: {STATS['oxylabs_failures']}")
    log(f"Non-English papers: {STATS['non_english_papers']}")
    log(f"English papers: {STATS['english_papers']}")
    log(f"Total NEW papers: {STATS['total_papers']}")
    log(f"Duplicates skipped: {STATS['duplicates']}")

    # Save JSON backup
    log("")
    log(f"[BACKUP] Saving to {json_filename}...")
    with open(json_filename, "w") as f:
        json.dump({
            "metadata": {
                "paper_id": PAPER_ID,
                "edition_id": EDITION_ID,
                "year": TARGET_YEAR,
                "strategy": "author_initial_split",
                "excluded_initials": excluded_initials,
                "timestamp": datetime.now().isoformat(),
                "dry_run": dry_run,
            },
            "stats": STATS,
            "papers": all_papers
        }, f, indent=2, default=str)
    log(f"[BACKUP] Saved {len(all_papers)} papers")

    # Save to DB if not dry run
    if not dry_run and all_papers:
        log("")
        log("[DB] Saving papers to database...")
        saved = await save_papers_to_db(all_papers)
        log(f"[DB] Saved {saved} papers")

    log("")
    log(f"Log file: {LOG_FILE_PATH}")
    log(f"JSON file: {json_filename}")

    await scholar.close()
    LOG_FILE.close()

    return STATS


def main():
    parser = argparse.ArgumentParser(description="Author Initial Split Harvester")
    parser.add_argument("--dry-run", action="store_true", help="Count only, don't save")
    parser.add_argument("--run", action="store_true", help="Actually harvest and save")
    args = parser.parse_args()

    if not args.dry_run and not args.run:
        print("Please specify --dry-run or --run")
        sys.exit(1)

    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
