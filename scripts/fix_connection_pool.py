#!/usr/bin/env python3
"""
Fix Connection Pool Script

Use this script when you see the error:
"Timeout getting connection from pool. pool size: 10, max: 10, active tx: 0"

This terminates idle connections to free up the connection pool.
"""

import psycopg2
import sys

def fix_connection_pool(db_name='real_estate_db'):
    try:
        # Connect to postgres database
        conn = psycopg2.connect('postgresql://postgres:password@localhost:5432/postgres')
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Get active connections before terminating
        cursor.execute("""
            SELECT pid, usename, application_name, state
            FROM pg_stat_activity
            WHERE datname = %s
              AND pid <> pg_backend_pid()
        """, (db_name,))
        
        connections = cursor.fetchall()
        
        print("="*70)
        print(f"CONNECTION POOL FIX FOR: {db_name}")
        print("="*70)
        print(f"\nFound {len(connections)} active connection(s):\n")
        
        for pid, user, app, state in connections:
            print(f"  PID {pid}: {user} | {app or 'Unknown'} | {state}")
        
        # Terminate idle connections
        cursor.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s
              AND pid <> pg_backend_pid()
              AND state = 'idle'
        """, (db_name,))
        
        terminated = cursor.fetchall()
        
        print(f"\n✅ Terminated {len(terminated)} idle connection(s)")
        
        # Check remaining connections
        cursor.execute("""
            SELECT COUNT(*)
            FROM pg_stat_activity 
            WHERE datname = %s
        """, (db_name,))
        
        remaining = cursor.fetchone()[0]
        print(f"✅ Remaining active connections: {remaining}")
        
        print("\n" + "="*70)
        print("CONNECTION POOL FIXED!")
        print("="*70)
        
        cursor.close()
        conn.close()
        
        return 0
        
    except psycopg2.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
        print("\nMake sure PostgreSQL is running and credentials are correct.")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    db_name = sys.argv[1] if len(sys.argv) > 1 else 'real_estate_db'
    exit_code = fix_connection_pool(db_name)
    sys.exit(exit_code)
