#!/usr/bin/env python3
"""
Test the mini-set inclusion approach for overflow harvesting.

Instead of one big OR query, we partition the inclusion set into mini-sets
where 3 terms at a time are positive (OR'd together) and the rest remain negative.

This helps diagnose where papers are being lost.
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from app.services.scholar_search import ScholarSearchService


# The 28 terms from partition run #21
EXCLUSION_TERMS = [
    "cultural", "contemporary", "theory", "social", "critical", "modern",
    "political", "culture", "postmodern", "analysis", "identity", "media",
    "global", "art", "discourse", "space", "literature", "history",
    "power", "urban", "aesthetic", "capitalism", "film", "study",
    "approach", "architecture", "narrative", "new"
]

SCHOLAR_ID = "15603705792201309427"
YEAR = 2014
BATCH_SIZE = 3


def build_miniset_query(terms: List[str], positive_indices: List[int]) -> str:
    """
    Build a query where terms at positive_indices are OR'd together,
    and all other terms are negated.

    Example: positive_indices=[0,1,2] for terms cultural, contemporary, theory
    Result: intitle:"cultural" OR intitle:"contemporary" OR intitle:"theory" -intitle:"social" ...
    """
    parts = []

    # First add the positive terms (OR'd together)
    positive_parts = []
    for i in positive_indices:
        positive_parts.append(f'intitle:"{terms[i]}"')
    parts.append(" OR ".join(positive_parts))

    # Then add all other terms as negatives
    for i, term in enumerate(terms):
        if i not in positive_indices:
            parts.append(f'-intitle:"{term}"')

    return " ".join(parts)


async def get_count(scholar: ScholarSearchService, query: str) -> int:
    """Get count for a query from Google Scholar."""
    result = await scholar.get_cited_by(
        scholar_id=SCHOLAR_ID,
        max_results=10,
        year_low=YEAR,
        year_high=YEAR,
        additional_query=query,
    )
    return result.get('totalResults', 0)


async def run_test():
    """Run the mini-set inclusion test."""
    print("=" * 80)
    print("MINI-SET INCLUSION TEST")
    print(f"Paper: Jameson's Postmodernism (Scholar ID: {SCHOLAR_ID})")
    print(f"Year: {YEAR}")
    print(f"Total terms: {len(EXCLUSION_TERMS)}")
    print(f"Batch size: {BATCH_SIZE}")
    print("=" * 80)
    print()

    scholar = ScholarSearchService()

    try:
        # First get the baseline counts
        print("BASELINE COUNTS:")
        print("-" * 40)

        # Total for year
        total_result = await scholar.get_cited_by(
            scholar_id=SCHOLAR_ID,
            max_results=10,
            year_low=YEAR,
            year_high=YEAR,
        )
        total_count = total_result.get('totalResults', 0)
        print(f"Total for {YEAR}: {total_count}")

        await asyncio.sleep(2)  # Rate limiting

        # Exclusion set count
        exclusion_query = " ".join([f'-intitle:"{t}"' for t in EXCLUSION_TERMS])
        exclusion_count = await get_count(scholar, exclusion_query)
        print(f"Exclusion set (without ANY term): {exclusion_count}")

        expected_inclusion = total_count - exclusion_count
        print(f"Expected inclusion (total - exclusion): {expected_inclusion}")

        await asyncio.sleep(2)

        # Big OR query (for comparison)
        big_or_query = " OR ".join([f'intitle:"{t}"' for t in EXCLUSION_TERMS])
        big_or_count = await get_count(scholar, big_or_query)
        print(f"Big OR query result: {big_or_count}")
        print(f"Gap (expected - big OR): {expected_inclusion - big_or_count}")

        print()
        print("=" * 80)
        print("MINI-SET RESULTS:")
        print("=" * 80)

        results: List[Tuple[List[str], int]] = []
        total_miniset = 0

        # Generate batches
        num_batches = (len(EXCLUSION_TERMS) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx in range(num_batches):
            start_idx = batch_idx * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, len(EXCLUSION_TERMS))
            positive_indices = list(range(start_idx, end_idx))

            batch_terms = [EXCLUSION_TERMS[i] for i in positive_indices]
            query = build_miniset_query(EXCLUSION_TERMS, positive_indices)

            await asyncio.sleep(3)  # Rate limiting between queries

            count = await get_count(scholar, query)
            results.append((batch_terms, count))
            total_miniset += count

            print(f"Batch {batch_idx + 1}/{num_batches}: {batch_terms}")
            print(f"  Query: {query[:80]}...")
            print(f"  Count: {count}")
            print()

        print("=" * 80)
        print("SUMMARY:")
        print("=" * 80)
        print()
        print(f"{'Batch':<10} {'Terms':<50} {'Count':>10}")
        print("-" * 70)

        for i, (terms, count) in enumerate(results):
            terms_str = ", ".join(terms)
            print(f"{i+1:<10} {terms_str:<50} {count:>10}")

        print("-" * 70)
        print(f"{'TOTAL':<10} {'':<50} {total_miniset:>10}")
        print()

        print("ANALYSIS:")
        print(f"  Total for year:           {total_count}")
        print(f"  Exclusion set:            {exclusion_count}")
        print(f"  Expected inclusion:       {expected_inclusion}")
        print(f"  Big OR query:             {big_or_count}")
        print(f"  Sum of mini-sets:         {total_miniset}")
        print()
        print(f"  Gap (expected - big OR):  {expected_inclusion - big_or_count}")
        print(f"  Gap (expected - miniset): {expected_inclusion - total_miniset}")
        print()

        # What's missing?
        missing_from_miniset = expected_inclusion - total_miniset
        if missing_from_miniset > 0:
            print(f"  ⚠️  {missing_from_miniset} papers have terms from MULTIPLE batches")
            print(f"      (they are excluded by each mini-set query)")

        # Save results to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"miniset_test_{timestamp}.txt"
        with open(output_file, 'w') as f:
            f.write(f"Mini-Set Inclusion Test Results\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Scholar ID: {SCHOLAR_ID}\n")
            f.write(f"Year: {YEAR}\n")
            f.write(f"\n")
            f.write(f"Total for year: {total_count}\n")
            f.write(f"Exclusion set: {exclusion_count}\n")
            f.write(f"Expected inclusion: {expected_inclusion}\n")
            f.write(f"Big OR query: {big_or_count}\n")
            f.write(f"Sum of mini-sets: {total_miniset}\n")
            f.write(f"\n")
            for i, (terms, count) in enumerate(results):
                f.write(f"Batch {i+1}: {terms} = {count}\n")

        print(f"Results saved to: {output_file}")

    finally:
        await scholar.close()


if __name__ == "__main__":
    asyncio.run(run_test())
