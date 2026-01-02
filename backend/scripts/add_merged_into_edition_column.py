#!/usr/bin/env python3
"""
Add merged_into_edition_id column to editions table.
This allows merging duplicate editions (same work, different URLs/scholar_ids)
into a canonical edition while preserving both scholar_ids for harvesting.
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL environment variable must be set")
    sys.exit(1)

# Convert asyncpg to psycopg2 for synchronous migration
sync_url = DATABASE_URL.replace("+asyncpg", "")

def run_migration():
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        # Check if column already exists
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'editions'
            AND column_name = 'merged_into_edition_id'
        """))

        if result.fetchone():
            print("Column 'merged_into_edition_id' already exists. Nothing to do.")
            return

        # Add the column
        print("Adding 'merged_into_edition_id' column to editions table...")
        conn.execute(text("""
            ALTER TABLE editions
            ADD COLUMN merged_into_edition_id INTEGER REFERENCES editions(id) ON DELETE SET NULL
        """))

        # Add index for faster lookups
        print("Adding index on merged_into_edition_id...")
        conn.execute(text("""
            CREATE INDEX ix_editions_merged_into ON editions(merged_into_edition_id)
            WHERE merged_into_edition_id IS NOT NULL
        """))

        conn.commit()
        print("✅ Column added successfully!")

        # Verify
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'editions'
            AND column_name = 'merged_into_edition_id'
        """))
        row = result.fetchone()
        if row:
            print(f"   Column: {row[0]}, Type: {row[1]}, Nullable: {row[2]}")

if __name__ == "__main__":
    run_migration()
