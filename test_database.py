import duckdb
import os

def test_database():
    """Test database connectivity and structure"""
    db_path = "voter_data.duckdb"
    
    print(f"Checking database: {db_path}")
    print(f"File exists: {os.path.exists(db_path)}")
    
    if not os.path.exists(db_path):
        print("❌ Database file not found!")
        return
    
    try:
        # Connect to database
        conn = duckdb.connect(db_path)
        print("✅ Successfully connected to database")
        
        # List all tables
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchdf()
        print(f"📋 Available tables: {list(tables['name'])}")
        
        # Check for Delhi_Voter table
        if 'Delhi_Voter' in tables['name'].values:
            print("✅ Delhi_Voter table found")
            
            # Get table structure
            columns = conn.execute("PRAGMA table_info(Delhi_Voter)").fetchdf()
            print(f"📊 Table columns: {list(columns['name'])}")
            
            # Get row count
            count = conn.execute("SELECT COUNT(*) FROM Delhi_Voter").fetchone()[0]
            print(f"📈 Total rows: {count}")
            
            # Show sample data
            sample = conn.execute("SELECT * FROM Delhi_Voter LIMIT 3").fetchdf()
            print("📋 Sample data:")
            print(sample)
            
        else:
            print("❌ Delhi_Voter table not found")
            print("Available tables:", list(tables['name']))
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_database() 