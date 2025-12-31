#!/usr/bin/env python3
"""
Test split inclusion approach - two OR queries instead of one big one.

Query 1 (9 terms): intitle:"cultural" OR intitle:"contemporary" OR intitle:"theory" OR intitle:"social" OR intitle:"critical" OR intitle:"modern" OR intitle:"political" OR intitle:"culture" OR intitle:"postmodern"

Query 2 (19 terms): intitle:"analysis" OR intitle:"identity" OR intitle:"media" OR intitle:"global" OR intitle:"art" OR intitle:"discourse" OR intitle:"space" OR intitle:"literature" OR intitle:"history" OR intitle:"power" OR intitle:"urban" OR intitle:"aesthetic" OR intitle:"capitalism" OR intitle:"film" OR intitle:"study" OR intitle:"approach" OR intitle:"architecture" OR intitle:"narrative" OR intitle:"new"
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from app.services.scholar_search import ScholarSearchService


SCHOLAR_ID = "15603705792201309427"
YEAR = 2014

# Split the terms into two groups
TERMS_GROUP_1 = ["cultural", "contemporary", "theory", "social", "critical", "modern", "political", "culture", "postmodern"]
TERMS_GROUP_2 = ["analysis", "identity", "media", "global", "art", "discourse", "space", "literature", "history", "power", "urban", "aesthetic", "capitalism", "film", "study", "approach", "architecture", "narrative", "new"]


def build_or_query(terms: List[str]) -> str:
    """Build an OR query from terms."""
    return " OR ".join([f'intitle:"{t}"' for t in terms])


async def harvest_query(scholar: ScholarSearchService, query: str, query_name: str) -> List[Dict]:
    """Harvest all papers for a query (up to 1000)."""
    print(f"\nHarvesting {query_name}...")
    print(f"Query: {query[:80]}...")

    all_papers = []

    def on_page(page_num: int, papers: List[Dict]):
        all_papers.extend(papers)
        print(f"  Page {page_num + 1}: {len(papers)} papers (total so far: {len(all_papers)})")

    result = await scholar.get_cited_by(
        scholar_id=SCHOLAR_ID,
        max_results=1000,  # Get all
        year_low=YEAR,
        year_high=YEAR,
        additional_query=query,
        on_page_complete=on_page,
    )

    total_reported = result.get('totalResults', 0)
    print(f"  Google Scholar reported: {total_reported}")
    print(f"  Actually harvested: {len(all_papers)}")

    return all_papers


async def run_test():
    """Run the split inclusion harvest test."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 80)
    print("SPLIT INCLUSION HARVEST TEST")
    print(f"Timestamp: {timestamp}")
    print(f"Paper: Jameson's Postmodernism (Scholar ID: {SCHOLAR_ID})")
    print(f"Year: {YEAR}")
    print("=" * 80)

    print(f"\nGroup 1 terms ({len(TERMS_GROUP_1)}): {', '.join(TERMS_GROUP_1)}")
    print(f"Group 2 terms ({len(TERMS_GROUP_2)}): {', '.join(TERMS_GROUP_2)}")

    scholar = ScholarSearchService()

    try:
        # Harvest group 1
        query1 = build_or_query(TERMS_GROUP_1)
        papers1 = await harvest_query(scholar, query1, "Group 1")

        await asyncio.sleep(5)  # Rate limiting

        # Harvest group 2
        query2 = build_or_query(TERMS_GROUP_2)
        papers2 = await harvest_query(scholar, query2, "Group 2")

        # Combine and deduplicate
        print("\n" + "=" * 80)
        print("DEDUPLICATION:")
        print("=" * 80)

        all_papers = papers1 + papers2
        print(f"Total papers before dedup: {len(all_papers)}")

        # Build set of unique scholar IDs
        seen_ids: Set[str] = set()
        unique_papers: List[Dict] = []
        duplicates = 0
        no_id = 0

        for p in all_papers:
            pid = p.get("scholarId") or p.get("id")
            if not pid:
                no_id += 1
                # Still keep papers without IDs
                unique_papers.append(p)
                continue
            if pid in seen_ids:
                duplicates += 1
                continue
            seen_ids.add(pid)
            unique_papers.append(p)

        print(f"Duplicates found: {duplicates}")
        print(f"Papers without ID: {no_id}")
        print(f"Unique papers: {len(unique_papers)}")

        # Save to JSON
        json_file = f"split_inclusion_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump({
                "timestamp": timestamp,
                "scholar_id": SCHOLAR_ID,
                "year": YEAR,
                "group1_count": len(papers1),
                "group2_count": len(papers2),
                "total_before_dedup": len(all_papers),
                "duplicates": duplicates,
                "no_id": no_id,
                "unique_count": len(unique_papers),
                "papers": unique_papers,
            }, f, indent=2)
        print(f"\nSaved to: {json_file}")

        # Summary
        print("\n" + "=" * 80)
        print("SUMMARY:")
        print("=" * 80)
        print(f"  Group 1 harvested:        {len(papers1)}")
        print(f"  Group 2 harvested:        {len(papers2)}")
        print(f"  Total before dedup:       {len(all_papers)}")
        print(f"  Duplicates removed:       {duplicates}")
        print(f"  UNIQUE PAPERS:            {len(unique_papers)}")
        print()
        print(f"  Expected inclusion:       ~840-850")
        print(f"  Big OR query returned:    732")
        print(f"  Our split approach:       {len(unique_papers)}")
        print()
        if len(unique_papers) > 732:
            print(f"  ✓ We recovered {len(unique_papers) - 732} papers that the big OR missed!")

    finally:
        await scholar.close()


if __name__ == "__main__":
    asyncio.run(run_test())
