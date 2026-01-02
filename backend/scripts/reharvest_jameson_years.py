#!/usr/bin/env python3
"""
RE-HARVEST SPECIFIC YEARS for Jameson's Postmodernism book.

This script re-harvests citations for specific years using the NEW METHODOLOGY:
1. First harvest NON-ENGLISH papers (language stratification)
2. Then handle ENGLISH papers with exclusion/inclusion strategy if needed

PAPER: Postmodernism, or the cultural logic of late capitalism (F Jameson)
PAPER_ID: 71
EDITION_ID: 1073

TARGET YEARS: 2015, 2016, 2017, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026

Features:
- DETAILED logging to console AND log file
- Oxylabs request/response logging
- DB save logging
- JSON backup of all harvested papers
- Progress tracking

Usage:
    python scripts/reharvest_jameson_years.py --dry-run        # Check counts only
    python scripts/reharvest_jameson_years.py --run            # Actually harvest
    python scripts/reharvest_jameson_years.py --run --year 2021  # Single year
"""

import argparse
import asyncio
import json
import os
import sys
import logging
import traceback
from datetime import datetime
from typing import Set, Dict, Any, List, Optional

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load env BEFORE any other imports
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Constants
PAPER_ID = 71
EDITION_ID = 1073
SCHOLAR_ID = "15603705792201309427"  # Jameson's Postmodernism cluster ID
EDITION_TITLE = "Postmodernism, or the cultural logic of late capitalism"

TARGET_YEARS = [2015, 2016, 2017, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]

# Global log file handle
LOG_FILE = None
LOG_FILE_PATH = None

# Stats tracking
STATS = {
    "oxylabs_requests": 0,
    "oxylabs_successes": 0,
    "oxylabs_failures": 0,
    "db_saves": 0,
    "db_duplicates": 0,
    "papers_found": 0,
    "papers_new": 0,
}


def log(msg: str, level: str = "INFO"):
    """Log with timestamp - writes to stdout AND log file"""
    global LOG_FILE
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] [{level:5}] {msg}"
    print(line, flush=True)
    if LOG_FILE:
        LOG_FILE.write(line + "\n")
        LOG_FILE.flush()


def log_separator(char: str = "=", width: int = 80):
    """Log a separator line"""
    log(char * width)


def log_box(lines: List[str], char: str = "="):
    """Log a box with lines"""
    width = max(len(line) for line in lines) + 4
    log(f"+{char * (width - 2)}+")
    for line in lines:
        log(f"| {line.ljust(width - 4)} |")
    log(f"+{char * (width - 2)}+")


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


async def get_existing_citation_ids(db: AsyncSession, paper_id: int, year: int) -> Set[str]:
    """Get scholar IDs of citations we already have for this paper/year"""
    result = await db.execute(text("""
        SELECT scholar_id FROM citations
        WHERE paper_id = :paper_id AND year = :year AND scholar_id IS NOT NULL
    """), {"paper_id": paper_id, "year": year})
    ids = {row[0] for row in result.fetchall()}
    log(f"[DB] Found {len(ids)} existing citations for year {year}")
    return ids


async def get_year_count(scholar, scholar_id: str, year: int) -> int:
    """Get citation count for a specific year from Google Scholar"""
    result = await scholar.get_cited_by(
        scholar_id=scholar_id,
        max_results=10,
        year_low=year,
        year_high=year,
    )
    return result.get('totalResults', 0)


async def save_citation_to_db(
    db: AsyncSession,
    paper: Dict[str, Any],
    paper_id: int,
    edition_id: int,
    year: int,
    existing_ids: Set[str]
) -> bool:
    """Save a single citation to database. Returns True if new, False if duplicate."""
    global STATS

    from app.models import Citation

    pid = paper.get("scholarId") or paper.get("id")
    if not pid:
        log(f"    [SKIP] No scholar_id: {paper.get('title', '???')[:40]}", "WARN")
        return False

    if pid in existing_ids:
        return False  # Already have it

    # New paper - save it
    existing_ids.add(pid)

    authors_raw = paper.get("authors")
    if isinstance(authors_raw, list):
        authors_str = ", ".join(authors_raw)
    else:
        authors_str = authors_raw

    citation = Citation(
        paper_id=paper_id,
        edition_id=edition_id,
        scholar_id=pid,
        title=paper.get("title"),
        authors=authors_str,
        year=paper.get("year") or year,
        venue=paper.get("venue"),
        citation_count=paper.get("citationCount") or 0,
        link=paper.get("link"),
        intersection_count=0,
    )

    try:
        db.add(citation)
        await db.commit()
        STATS["db_saves"] += 1
        return True
    except IntegrityError:
        await db.rollback()
        STATS["db_duplicates"] += 1
        log(f"    [DUP] {pid[:20]}... already in DB", "WARN")
        return False
    except Exception as e:
        await db.rollback()
        log(f"    [ERROR] Failed to save: {e}", "ERROR")
        return False


async def harvest_year_stratified(
    db: AsyncSession,
    scholar,
    year: int,
    existing_ids: Set[str],
    json_backup: List[Dict],
    save_json_func
) -> Dict[str, Any]:
    """
    Harvest a single year using stratified language approach.

    Strategy:
    1. First harvest non-English papers (each language separately)
    2. Then check English-only count
    3. If English < 1000: harvest directly
    4. If English >= 1000: use exclusion term strategy
    """
    from app.services.overflow_harvester import (
        harvest_with_language_stratification,
        NON_ENGLISH_LANGUAGE_LIST,
        ENGLISH_ONLY,
        GOOGLE_SCHOLAR_LIMIT,
    )

    log_box([
        f"HARVESTING YEAR {year}",
        f"Strategy: Language Stratification",
    ])

    stats = {
        "year": year,
        "non_english": 0,
        "english": 0,
        "total_new": 0,
        "total_found": 0,
    }

    # Get current count
    total_count = await get_year_count(scholar.service, SCHOLAR_ID, year)
    log(f"[YEAR {year}] Google Scholar reports: {total_count} total citations")
    stats["google_total"] = total_count

    # Callback to save papers
    async def on_page_complete(page_num: int, papers: List[Dict]):
        global STATS
        new_count = 0

        for p in papers:
            json_backup.append({**p, "year": year})
            STATS["papers_found"] += 1
            stats["total_found"] += 1

            if await save_citation_to_db(db, p, PAPER_ID, EDITION_ID, year, existing_ids):
                new_count += 1
                STATS["papers_new"] += 1
                stats["total_new"] += 1

        # Save JSON backup after each page
        save_json_func()

        log(f"  [PAGE {page_num + 1}] {len(papers)} papers | {new_count} NEW | Total new: {stats['total_new']}")

    # ========== STEP 1: Non-English Languages ==========
    log(f"[YEAR {year}] Step 1: Harvesting non-English papers ({len(NON_ENGLISH_LANGUAGE_LIST)} languages)...")

    for lang_code in NON_ENGLISH_LANGUAGE_LIST:
        try:
            # First check count
            lang_result = await scholar.get_cited_by(
                scholar_id=SCHOLAR_ID,
                max_results=10,
                year_low=year,
                year_high=year,
                language_filter=lang_code,
            )
            lang_count = lang_result.get('totalResults', 0)

            if lang_count == 0:
                log(f"  [{lang_code}] 0 papers (skipping)")
                continue

            log(f"  [{lang_code}] {lang_count} papers - harvesting...")

            # Harvest
            harvest_result = await scholar.get_cited_by(
                scholar_id=SCHOLAR_ID,
                max_results=min(lang_count, GOOGLE_SCHOLAR_LIMIT),
                year_low=year,
                year_high=year,
                language_filter=lang_code,
                on_page_complete=on_page_complete,
            )

            stats["non_english"] += harvest_result.get('pages_succeeded', 0) * 10

            # Rate limit
            await asyncio.sleep(3)

        except Exception as e:
            log(f"  [{lang_code}] ERROR: {e}", "ERROR")
            await asyncio.sleep(5)

    log(f"[YEAR {year}] Non-English phase complete: {stats['total_new']} new papers so far")

    # ========== STEP 2: English Papers ==========
    log(f"[YEAR {year}] Step 2: Checking English-only papers...")

    try:
        english_result = await scholar.get_cited_by(
            scholar_id=SCHOLAR_ID,
            max_results=10,
            year_low=year,
            year_high=year,
            language_filter=ENGLISH_ONLY,
        )
        english_count = english_result.get('totalResults', 0)
        log(f"[YEAR {year}] English-only papers: {english_count}")

        if english_count == 0:
            log(f"[YEAR {year}] No English papers - done!")

        elif english_count < GOOGLE_SCHOLAR_LIMIT:
            log(f"[YEAR {year}] English count ({english_count}) < 1000 - harvesting directly!")

            harvest_result = await scholar.get_cited_by(
                scholar_id=SCHOLAR_ID,
                max_results=english_count,
                year_low=year,
                year_high=year,
                language_filter=ENGLISH_ONLY,
                on_page_complete=on_page_complete,
            )

            stats["english"] = harvest_result.get('pages_succeeded', 0) * 10

        else:
            log(f"[YEAR {year}] English count ({english_count}) >= 1000 - using overflow harvester...")

            # Use the full stratified harvester for overflow case
            overflow_result = await harvest_with_language_stratification(
                db=db,
                scholar_service=scholar.service,
                edition_id=EDITION_ID,
                scholar_id=SCHOLAR_ID,
                year=year,
                edition_title=EDITION_TITLE,
                paper_id=PAPER_ID,
                existing_scholar_ids=existing_ids,
                on_page_complete=on_page_complete,
                total_for_year=total_count,
                job_id=None,
            )

            stats["english"] = overflow_result.get("english_harvested", 0)

    except Exception as e:
        log(f"[YEAR {year}] English harvest error: {e}", "ERROR")
        log(traceback.format_exc(), "ERROR")

    log_box([
        f"YEAR {year} COMPLETE",
        f"Google Scholar total: {stats.get('google_total', '?')}",
        f"Non-English harvested: {stats['non_english']}",
        f"English harvested: {stats['english']}",
        f"Total new papers: {stats['total_new']}",
    ], char="-")

    return stats


async def run_reharvest(years: List[int], dry_run: bool = True):
    """Main entry point for re-harvesting"""
    global LOG_FILE, LOG_FILE_PATH, STATS

    # Setup logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_FILE_PATH = f"reharvest_jameson_{timestamp}.log"
    LOG_FILE = open(LOG_FILE_PATH, 'w')

    log_separator()
    log_box([
        "JAMESON RE-HARVEST SCRIPT",
        f"Paper: {EDITION_TITLE[:50]}...",
        f"Paper ID: {PAPER_ID}, Edition ID: {EDITION_ID}",
        f"Target years: {years}",
        f"Mode: {'DRY RUN' if dry_run else 'LIVE HARVEST'}",
        f"Log file: {LOG_FILE_PATH}",
    ])
    log_separator()

    # Also redirect Python logging to our log file
    class LogFileHandler(logging.Handler):
        def emit(self, record):
            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            line = f"[{ts}] [{record.levelname:5}] [{record.name}] {record.getMessage()}"
            if LOG_FILE:
                LOG_FILE.write(line + "\n")
                LOG_FILE.flush()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(LogFileHandler())

    # Database connection
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log("ERROR: DATABASE_URL not set", "ERROR")
        return

    log(f"[DB] Connecting to: {db_url[:50]}...")

    engine = create_async_engine(
        db_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        connect_args={
            "server_settings": {
                "tcp_keepalives_idle": "60",
                "tcp_keepalives_interval": "10",
                "tcp_keepalives_count": "5",
            },
            "command_timeout": 300,
        }
    )
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Initialize scholar service
    from app.services.scholar_search import ScholarSearchService
    real_scholar = ScholarSearchService()
    scholar = LoggingScholarService(real_scholar)

    # JSON backup
    json_backup_path = f"reharvest_jameson_{timestamp}.json"
    all_papers = []

    def save_json_backup():
        with open(json_backup_path, 'w') as f:
            json.dump({
                "paper_id": PAPER_ID,
                "edition_id": EDITION_ID,
                "timestamp": timestamp,
                "years": years,
                "total_papers": len(all_papers),
                "papers": all_papers,
            }, f, indent=2)

    log(f"[BACKUP] JSON backup will be saved to: {json_backup_path}")

    try:
        async with async_session() as db:
            # DRY RUN: Just check counts
            if dry_run:
                log_separator()
                log("DRY RUN - Checking counts only")
                log_separator()

                for year in years:
                    existing_ids = await get_existing_citation_ids(db, PAPER_ID, year)
                    gs_count = await get_year_count(scholar.service, SCHOLAR_ID, year)
                    gap = gs_count - len(existing_ids)
                    coverage = (len(existing_ids) / gs_count * 100) if gs_count > 0 else 0

                    log(f"Year {year}: Scholar={gs_count}, DB={len(existing_ids)}, Gap={gap}, Coverage={coverage:.1f}%")
                    await asyncio.sleep(2)

                log_separator()
                log("To actually harvest, run with --run flag")
                return

            # LIVE HARVEST
            log_separator()
            log("LIVE HARVEST - Starting...")
            log_separator()

            all_stats = []

            for year in years:
                log_separator()

                # Get existing citations
                existing_ids = await get_existing_citation_ids(db, PAPER_ID, year)

                # Harvest this year
                year_stats = await harvest_year_stratified(
                    db=db,
                    scholar=scholar,
                    year=year,
                    existing_ids=existing_ids,
                    json_backup=all_papers,
                    save_json_func=save_json_backup,
                )
                all_stats.append(year_stats)

                # Rate limit between years
                log(f"[RATE LIMIT] Sleeping 10 seconds before next year...")
                await asyncio.sleep(10)

            # Final summary
            log_separator()
            log_box([
                "FINAL SUMMARY",
                f"Years processed: {len(years)}",
                f"Total Oxylabs requests: {STATS['oxylabs_requests']}",
                f"Oxylabs successes: {STATS['oxylabs_successes']}",
                f"Oxylabs failures: {STATS['oxylabs_failures']}",
                f"Total papers found: {STATS['papers_found']}",
                f"New papers saved: {STATS['papers_new']}",
                f"Duplicates skipped: {STATS['db_duplicates']}",
            ])

            log("\nPer-year breakdown:")
            for s in all_stats:
                log(f"  {s['year']}: Google={s.get('google_total', '?')}, New={s['total_new']}")

            # Save final JSON
            save_json_backup()
            log(f"\n[BACKUP] Final JSON saved: {json_backup_path}")
            log(f"[BACKUP] Total papers in backup: {len(all_papers)}")

    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        log(traceback.format_exc(), "ERROR")
    finally:
        await scholar.close()
        await engine.dispose()

        if LOG_FILE:
            log_separator()
            log("LOG FILE COMPLETE")
            log(f"Saved to: {LOG_FILE_PATH}")
            LOG_FILE.close()


def main():
    parser = argparse.ArgumentParser(
        description="Re-harvest specific years for Jameson's Postmodernism book",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Target years: {TARGET_YEARS}

Examples:
  # Dry run - check counts for all target years
  python scripts/reharvest_jameson_years.py --dry-run

  # Actually harvest all target years
  python scripts/reharvest_jameson_years.py --run

  # Harvest a single year
  python scripts/reharvest_jameson_years.py --run --year 2021

  # Harvest multiple specific years
  python scripts/reharvest_jameson_years.py --run --years 2021 2022 2023
"""
    )

    parser.add_argument("--dry-run", action="store_true",
                        help="Only check counts, don't harvest (default)")
    parser.add_argument("--run", action="store_true",
                        help="Actually run the harvest")
    parser.add_argument("--year", type=int,
                        help="Harvest only this single year")
    parser.add_argument("--years", type=int, nargs="+",
                        help="Harvest these specific years")

    args = parser.parse_args()

    # Determine which years to process
    if args.year:
        years = [args.year]
    elif args.years:
        years = args.years
    else:
        years = TARGET_YEARS

    # Validate years
    for y in years:
        if y < 1984 or y > 2030:
            print(f"ERROR: Invalid year {y}")
            sys.exit(1)

    dry_run = not args.run

    if not dry_run:
        print("=" * 60)
        print("WARNING: LIVE HARVEST MODE")
        print(f"Will harvest years: {years}")
        print("This will make Oxylabs requests and save to database.")
        print("=" * 60)
        response = input("Continue? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            sys.exit(0)

    asyncio.run(run_reharvest(years, dry_run=dry_run))


if __name__ == "__main__":
    main()
