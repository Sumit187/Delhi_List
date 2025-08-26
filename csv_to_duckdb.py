import duckdb
import pandas as pd
import os
from pathlib import Path

def create_sample_csv():
    """Create a sample CSV file for demonstration"""
    sample_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
        'salary': [50000, 60000, 70000, 55000, 65000]
    }
    
    df = pd.DataFrame(sample_data)
    csv_path = Path('sample_data.csv')
    df.to_csv(csv_path, index=False)
    print(f"Created sample CSV file: {csv_path}")
    return csv_path

def read_csv_to_duckdb(csv_file_path, table_name='my_table', database_path=':memory:'):
    """
    Read a CSV file and load it into DuckDB database
    
    Parameters:
    - csv_file_path: Path to the CSV file
    - table_name: Name for the table in DuckDB
    - database_path: Path to DuckDB database file (use ':memory:' for in-memory)
    """
    
    try:
        # Connect to DuckDB (in-memory database)
        con = duckdb.connect(database_path)
        
        # Method 1: Using DuckDB's built-in CSV reader
        print(f"Reading CSV file: {csv_file_path}")
        print(f"Loading into table: {table_name}")
        
        # Create table from CSV
        con.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_file_path}')
        """)
        
        # Verify the data was loaded
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        print(f"Successfully loaded {result[0]} rows into table '{table_name}'")
        
        # Show the first few rows
        print("\nFirst 5 rows of the data:")
        result = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        print(result)
        
        # Show table schema
        print(f"\nTable schema for '{table_name}':")
        result = con.execute(f"DESCRIBE {table_name}").fetchdf()
        print(result)
        
        return con
        
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def read_csv_with_options(csv_file_path, table_name='my_table_with_options'):
    """
    Read CSV with specific options for handling different formats
    """
    try:
        con = duckdb.connect(':memory:')
        
        # Create table with specific CSV options
        con.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto(
                '{csv_file_path}',
                header=true,
                delimiter=',',
                auto_detect=true,
                sample_size=1000
            )
        """)
        
        print(f"Loaded data with options into table '{table_name}'")
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        print(f"Total rows: {result[0]}")
        
        return con
        
    except Exception as e:
        print(f"Error reading CSV with options: {e}")
        return None

def read_multiple_csv_files(folder_path, table_prefix='data'):
    """
    Read multiple CSV files from a folder into separate tables
    """
    try:
        con = duckdb.connect(':memory:')
        csv_files = list(Path(folder_path).glob('*.csv'))
        
        if not csv_files:
            print(f"No CSV files found in {folder_path}")
            return con
        
        for i, csv_file in enumerate(csv_files):
            table_name = f"{table_prefix}_{i+1}"
            print(f"Loading {csv_file.name} into table {table_name}")
            
            con.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_csv_auto('{csv_file}')
            """)
            
            result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            print(f"  - Loaded {result[0]} rows")
        
        return con
        
    except Exception as e:
        print(f"Error reading multiple CSV files: {e}")
        return None

def query_examples(con, table_name='my_table'):
    """
    Demonstrate various queries on the loaded data
    """
    print("\n" + "="*50)
    print("QUERY EXAMPLES")
    print("="*50)
    
    # Basic SELECT
    print("\n1. All data:")
    result = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    print(result)
    
    # Filtering
    print("\n2. People older than 30:")
    result = con.execute(f"SELECT name, age, city FROM {table_name} WHERE age > 30").fetchdf()
    print(result)
    
    # Aggregation
    print("\n3. Average salary by city:")
    result = con.execute(f"""
        SELECT city, AVG(salary) as avg_salary, COUNT(*) as count 
        FROM {table_name} 
        GROUP BY city 
        ORDER BY avg_salary DESC
    """).fetchdf()
    print(result)
    
    # Sorting
    print("\n4. Top 3 highest paid employees:")
    result = con.execute(f"""
        SELECT name, salary, city 
        FROM {table_name} 
        ORDER BY salary DESC 
        LIMIT 3
    """).fetchdf()
    print(result)

def main():
    """Main function to demonstrate CSV to DuckDB functionality"""
    print("DuckDB CSV Reading Demo")
    print("="*50)
    
    # Create sample CSV file
    csv_path = create_sample_csv()
    
    # Method 1: Basic CSV reading
    print("\n" + "="*50)
    print("METHOD 1: Basic CSV Reading")
    print("="*50)
    con1 = read_csv_to_duckdb(csv_path, 'employees')
    
    if con1:
        # Run some example queries
        query_examples(con1, 'employees')
        con1.close()
    
    # Method 2: CSV reading with options
    print("\n" + "="*50)
    print("METHOD 2: CSV Reading with Options")
    print("="*50)
    con2 = read_csv_with_options(csv_path, 'employees_options')
    
    if con2:
        con2.close()
    
    # Method 3: Read multiple CSV files (if they exist)
    print("\n" + "="*50)
    print("METHOD 3: Reading Multiple CSV Files")
    print("="*50)
    con3 = read_multiple_csv_files('.', 'batch')
    
    if con3:
        # List all tables
        result = con3.execute("SHOW TABLES").fetchdf()
        print(f"\nAll tables in database: {list(result['name'])}")
        con3.close()
    
    print("\n" + "="*50)
    print("Demo completed!")
    print("="*50)

if __name__ == "__main__":
    main() 