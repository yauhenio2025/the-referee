#!/usr/bin/env python3
"""
Comprehensive orphaned citation recovery.
Handles cases where editions exist on deleted intermediate papers.
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL environment variable must be set")
    sys.exit(1)

sync_url = DATABASE_URL.replace("+asyncpg", "")

# Direct mapping: deleted paper -> (active target paper, edition on active paper)
DIRECT_MAPPING = {
    462: (460, 1884),  # Homo juridicus (Portuguese)
    618: (605, 1879),  # Eros and civilization
    614: (601, 1880),  # The end of utopia
    464: (460, 1882),  # Homo juridicus (English)
    463: (460, 1883),  # Homo Juridicus (Spanish)
}

# Chained mapping: deleted paper -> edition on another deleted paper -> active target
# These need edition transfer first
CHAINED_CASES = [
    # (source_deleted_paper, edition_on_deleted_paper, active_target_paper)
    (634, 1878, 605),  # Eros y civilización: edition 1878 is on deleted #618, should go to #605
    (644, 1875, 617),  # Den endimensionella manniskan: edition 1875 is on deleted #650, should go to #617
]

def recover():
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        total_recovered = 0
        total_duplicates = 0

        print("=" * 70)
        print("PHASE 1: Move editions from deleted papers to active papers")
        print("=" * 70)

        for source_paper_id, edition_id, target_paper_id in CHAINED_CASES:
            print(f"\nMoving edition #{edition_id} to paper #{target_paper_id}...")
            conn.execute(text("""
                UPDATE editions SET paper_id = :target_paper_id WHERE id = :edition_id
            """), {"target_paper_id": target_paper_id, "edition_id": edition_id})
            print(f"  ✓ Edition #{edition_id} now belongs to paper #{target_paper_id}")

        conn.commit()

        print("\n" + "=" * 70)
        print("PHASE 2: Recover citations for direct mapping cases")
        print("=" * 70)

        for source_paper_id, (target_paper_id, target_edition_id) in DIRECT_MAPPING.items():
            result = conn.execute(text("""
                SELECT COUNT(*) FROM citations WHERE paper_id = :source_id
            """), {"source_id": source_paper_id})
            count = result.scalar()

            if count == 0:
                continue

            print(f"\nPaper #{source_paper_id}: {count} orphaned citations")

            # Find duplicates
            result = conn.execute(text("""
                SELECT c_orphan.id
                FROM citations c_orphan
                WHERE c_orphan.paper_id = :source_paper_id
                AND c_orphan.scholar_id IN (
                    SELECT scholar_id FROM citations WHERE paper_id = :target_paper_id
                )
            """), {"source_paper_id": source_paper_id, "target_paper_id": target_paper_id})
            duplicate_ids = [row[0] for row in result.fetchall()]

            if duplicate_ids:
                conn.execute(text("DELETE FROM citations WHERE id = ANY(:ids)"), {"ids": duplicate_ids})
                total_duplicates += len(duplicate_ids)

            # Move remaining
            result = conn.execute(text("""
                UPDATE citations
                SET paper_id = :target_paper_id, edition_id = :target_edition_id
                WHERE paper_id = :source_paper_id
            """), {
                "target_paper_id": target_paper_id,
                "target_edition_id": target_edition_id,
                "source_paper_id": source_paper_id
            })

            moved = result.rowcount
            total_recovered += moved
            print(f"  -> Paper #{target_paper_id}, edition #{target_edition_id}: moved {moved}, deleted {len(duplicate_ids)} dups")

        conn.commit()

        print("\n" + "=" * 70)
        print("PHASE 3: Recover citations for chained cases")
        print("=" * 70)

        for source_paper_id, edition_id, target_paper_id in CHAINED_CASES:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM citations WHERE paper_id = :source_id
            """), {"source_id": source_paper_id})
            count = result.scalar()

            if count == 0:
                continue

            print(f"\nPaper #{source_paper_id}: {count} orphaned citations")

            # Find duplicates
            result = conn.execute(text("""
                SELECT c_orphan.id
                FROM citations c_orphan
                WHERE c_orphan.paper_id = :source_paper_id
                AND c_orphan.scholar_id IN (
                    SELECT scholar_id FROM citations WHERE paper_id = :target_paper_id
                )
            """), {"source_paper_id": source_paper_id, "target_paper_id": target_paper_id})
            duplicate_ids = [row[0] for row in result.fetchall()]

            if duplicate_ids:
                conn.execute(text("DELETE FROM citations WHERE id = ANY(:ids)"), {"ids": duplicate_ids})
                total_duplicates += len(duplicate_ids)

            # Move remaining
            result = conn.execute(text("""
                UPDATE citations
                SET paper_id = :target_paper_id, edition_id = :edition_id
                WHERE paper_id = :source_paper_id
            """), {
                "target_paper_id": target_paper_id,
                "edition_id": edition_id,
                "source_paper_id": source_paper_id
            })

            moved = result.rowcount
            total_recovered += moved
            print(f"  -> Paper #{target_paper_id}, edition #{edition_id}: moved {moved}, deleted {len(duplicate_ids)} dups")

        conn.commit()

        print("\n" + "=" * 70)
        print("PHASE 4: Update harvested_citation_count for all affected editions")
        print("=" * 70)

        all_edition_ids = [eid for _, eid in DIRECT_MAPPING.values()] + [eid for _, eid, _ in CHAINED_CASES]
        for eid in all_edition_ids:
            conn.execute(text("""
                UPDATE editions
                SET harvested_citation_count = (SELECT COUNT(*) FROM citations WHERE edition_id = :eid)
                WHERE id = :eid
            """), {"eid": eid})

        conn.commit()

        print(f"\n{'='*70}")
        print(f"TOTAL RECOVERED: {total_recovered} citations")
        print(f"TOTAL DUPLICATES DELETED: {total_duplicates}")
        print(f"{'='*70}")

        # Verify no more orphans
        print("\nVERIFICATION - Remaining orphaned citations:")
        result = conn.execute(text("""
            SELECT COUNT(*) FROM citations c
            JOIN papers p ON c.paper_id = p.id
            WHERE p.deleted_at IS NOT NULL
        """))
        remaining = result.scalar()
        print(f"  {remaining} orphaned citations remaining")

if __name__ == "__main__":
    recover()
