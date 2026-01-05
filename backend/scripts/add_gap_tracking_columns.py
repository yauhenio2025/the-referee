#!/usr/bin/env python3
"""
Add comprehensive gap tracking columns to harvest_targets table.
Tracks why gaps occur between expected and actual counts.
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

# New columns for harvest_targets
HARVEST_TARGET_COLUMNS = [
    # Original count GS showed on page 1
    ("original_expected", "INTEGER"),
    # Last count GS showed (may differ from original as we paginate)
    ("final_gs_count", "INTEGER"),
    # Reason for gap between expected and actual
    # Values: gs_estimate_changed, rate_limit, parse_error, max_pages_reached,
    #         blocked, captcha, empty_page, pagination_ended, unknown
    ("gap_reason", "VARCHAR(50)"),
    # Additional context as JSON (e.g., error messages, page where issue occurred)
    ("gap_details", "JSONB"),
    # The page number where scraping stopped (useful for debugging)
    ("last_scraped_page", "INTEGER"),
    # Whether gap has been manually reviewed
    ("gap_reviewed", "BOOLEAN DEFAULT FALSE"),
    # Notes from manual review
    ("gap_review_notes", "TEXT"),
]

def run_migration():
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        print("Adding gap tracking columns to harvest_targets table...")

        for col_name, col_type in HARVEST_TARGET_COLUMNS:
            # Check if column already exists
            result = conn.execute(text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'harvest_targets'
                AND column_name = '{col_name}'
            """))

            if result.fetchone():
                print(f"  Column '{col_name}' already exists. Skipping.")
                continue

            # Add the column
            print(f"  Adding '{col_name}' ({col_type})...")
            conn.execute(text(f"""
                ALTER TABLE harvest_targets
                ADD COLUMN {col_name} {col_type}
            """))
            print(f"    Added {col_name}")

        conn.commit()
        print("\nMigration complete!")

        # Verify columns
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'harvest_targets'
            AND column_name IN ('original_expected', 'final_gs_count', 'gap_reason',
                               'gap_details', 'last_scraped_page', 'gap_reviewed', 'gap_review_notes')
            ORDER BY column_name
        """))
        print("\nGap tracking columns in harvest_targets:")
        for row in result.fetchall():
            print(f"   {row[0]}: {row[1]}, nullable={row[2]}, default={row[3]}")

if __name__ == "__main__":
    run_migration()
