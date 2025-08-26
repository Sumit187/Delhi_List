import duckdb
import pandas as pd
from pathlib import Path

def csv_to_duckdb_simple(csv_folder_path, csv_filename, table_name='my_data'):
    """
    Simple function to read a CSV file from a folder into DuckDB
    
    Parameters:
    - csv_folder_path: Path to the folder containing the CSV file
    - csv_filename: Name of the CSV file
    - table_name: Name for the table in DuckDB
    """
    
    # Construct full path to CSV file
    csv_path = Path(csv_folder_path) / csv_filename
    
    # Check if file exists
    if not csv_path.exists():
        print(f"Error: CSV file not found at {csv_path}")
        return None
    
    try:
        # Connect to DuckDB (in-memory database)
        con = duckdb.connect(':memory:')
        
        # Read CSV and create table
        con.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}')
        """)
        
        # Get row count
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Successfully loaded {count} rows from {csv_filename} into table '{table_name}'")
        
        # Show first few rows
        print("\nFirst 3 rows:")
        result = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
        print(result)
        
        return con
        
    except Exception as e:
        print(f"Error: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Create a sample CSV file in a folder
    sample_folder = Path("data")
    sample_folder.mkdir(exist_ok=True)
    
    # Create sample data
    sample_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    }
    
    df = pd.DataFrame(sample_data)
    csv_file = sample_folder / "employees.csv"
    df.to_csv(csv_file, index=False)
    print(f"Created sample CSV file: {csv_file}")
    
    # Read the CSV into DuckDB
    con = csv_to_duckdb_simple("data", "employees.csv", "employees")
    
    if con:
        # Example query
        print("\nQuerying data:")
        result = con.execute("SELECT name, age FROM employees WHERE age > 30").fetchdf()
        print(result)
        
        con.close() 