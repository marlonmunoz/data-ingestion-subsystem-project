#!/usr/bin/env python3
"""
Database Cleanup Script
Drops all tables and sequences from the real_estate_db to provide a clean slate.
Use this when restarting the project from scratch.
"""

import psycopg2
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def cleanup_database(db_url: str):
    """
    Drop all tables and sequences from the database.
    
    Args:
        db_url: PostgreSQL connection string
    """
    print("🧹 Starting database cleanup...")
    
    try:
        # Connect to the database
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Get all tables in public schema
        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """)
        tables = cursor.fetchall()
        
        if not tables:
            print("✅ No tables found. Database is already clean.")
            return
        
        print(f"\n📋 Found {len(tables)} table(s) to drop:")
        for table in tables:
            print(f"   - {table[0]}")
        
        # Drop each table with CASCADE
        for table in tables:
            table_name = table[0]
            print(f"\n🗑️  Dropping table: {table_name}")
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
            print(f"   ✅ Dropped {table_name}")
        
        # Get all sequences in public schema
        cursor.execute("""
            SELECT sequencename 
            FROM pg_sequences 
            WHERE schemaname = 'public'
            ORDER BY sequencename;
        """)
        sequences = cursor.fetchall()
        
        if sequences:
            print(f"\n📋 Found {len(sequences)} sequence(s) to drop:")
            for seq in sequences:
                print(f"   - {seq[0]}")
            
            # Drop each sequence
            for seq in sequences:
                seq_name = seq[0]
                print(f"\n🗑️  Dropping sequence: {seq_name}")
                cursor.execute(f'DROP SEQUENCE IF EXISTS "{seq_name}" CASCADE;')
                print(f"   ✅ Dropped {seq_name}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*50)
        print("✅ Database cleanup completed successfully!")
        print("="*50)
        
    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Database connection string from config
    DB_URL = "postgresql://postgres:password@localhost:5432/real_estate_db"
    
    print("="*50)
    print("🧹 DATABASE CLEANUP SCRIPT")
    print("="*50)
    print(f"\nTarget Database: real_estate_db")
    print("This will DROP ALL TABLES and SEQUENCES!")
    
    # Ask for confirmation
    response = input("\n⚠️  Are you sure you want to proceed? (yes/no): ").strip().lower()
    
    if response == "yes":
        cleanup_database(DB_URL)
    else:
        print("\n❌ Cleanup cancelled. No changes made.")
        sys.exit(0)
        
'''
Run this cleanup script if you need to restart again in the future by running:
python scripts/cleanup_database.py
'''
