#!/usr/bin/env python3
"""
Add stall tracking columns to editions table for diagnostics.
Tracks reset count, last stall point (year/offset), reason, and timestamp.
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

COLUMNS = [
    ("harvest_reset_count", "INTEGER DEFAULT 0 NOT NULL"),
    ("last_stall_year", "INTEGER"),
    ("last_stall_offset", "INTEGER"),
    ("last_stall_reason", "VARCHAR(100)"),
    ("last_stall_at", "TIMESTAMP"),
]

def run_migration():
    engine = create_engine(sync_url)

    with engine.connect() as conn:
        for col_name, col_type in COLUMNS:
            # Check if column already exists
            result = conn.execute(text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'editions'
                AND column_name = '{col_name}'
            """))

            if result.fetchone():
                print(f"Column '{col_name}' already exists. Skipping.")
                continue

            # Add the column
            print(f"Adding '{col_name}' column to editions table...")
            conn.execute(text(f"""
                ALTER TABLE editions
                ADD COLUMN {col_name} {col_type}
            """))
            print(f"   Added {col_name} ({col_type})")

        conn.commit()
        print("\nMigration complete!")

        # Verify all columns
        result = conn.execute(text("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = 'editions'
            AND column_name LIKE '%stall%'
            ORDER BY column_name
        """))
        print("\nStall-related columns in editions table:")
        for row in result.fetchall():
            print(f"   {row[0]}: {row[1]}, nullable={row[2]}, default={row[3]}")

if __name__ == "__main__":
    run_migration()
