#!/usr/bin/env python3
"""
Add redirected_harvest_count column to editions table.
Tracks how many citations were harvested from merged editions' scholar_ids
(these citations go to the canonical edition, but we track the contribution).
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
            AND column_name = 'redirected_harvest_count'
        """))

        if result.fetchone():
            print("Column 'redirected_harvest_count' already exists. Nothing to do.")
            return

        # Add the column
        print("Adding 'redirected_harvest_count' column to editions table...")
        conn.execute(text("""
            ALTER TABLE editions
            ADD COLUMN redirected_harvest_count INTEGER DEFAULT 0 NOT NULL
        """))

        conn.commit()
        print("Column added successfully!")

        # Verify
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'editions'
            AND column_name = 'redirected_harvest_count'
        """))
        row = result.fetchone()
        if row:
            print(f"   Column: {row[0]}, Type: {row[1]}, Nullable: {row[2]}, Default: {row[3]}")

if __name__ == "__main__":
    run_migration()
