# CSV to DuckDB Reading Examples

This repository contains Python scripts to demonstrate how to read CSV files into DuckDB database.

## Files

- `simple_csv_to_duckdb.py` - Simple, focused script for reading a single CSV file
- `csv_to_duckdb.py` - Comprehensive script with multiple methods and examples
- `requirements.txt` - Required Python packages

## Installation

1. Make sure you have Python 3.7+ installed
2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Simple Example

Run the simple script to see a basic example:

```bash
python simple_csv_to_duckdb.py
```

This will:
1. Create a `data/` folder
2. Generate a sample CSV file (`data/employees.csv`)
3. Load it into DuckDB
4. Run a sample query

### Comprehensive Example

Run the comprehensive script for more detailed examples:

```bash
python csv_to_duckdb.py
```

This demonstrates:
- Basic CSV reading
- CSV reading with options
- Reading multiple CSV files
- Various SQL queries on the loaded data

## How to Use with Your Own CSV Files

### Method 1: Using the Simple Function

```python
import duckdb
from pathlib import Path

def csv_to_duckdb_simple(csv_folder_path, csv_filename, table_name='my_data'):
    csv_path = Path(csv_folder_path) / csv_filename
    
    if not csv_path.exists():
        print(f"Error: CSV file not found at {csv_path}")
        return None
    
    try:
        con = duckdb.connect(':memory:')
        
        # Read CSV and create table
        con.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}')
        """)
        
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Successfully loaded {count} rows")
        
        return con
        
    except Exception as e:
        print(f"Error: {e}")
        return None

# Usage
con = csv_to_duckdb_simple("your_folder", "your_file.csv", "your_table")
```

### Method 2: Direct DuckDB Approach

```python
import duckdb

# Connect to DuckDB
con = duckdb.connect(':memory:')  # or 'your_database.db' for persistent storage

# Read CSV directly into a table
con.execute("""
    CREATE TABLE my_table AS 
    SELECT * FROM read_csv_auto('path/to/your/file.csv')
""")

# Query the data
result = con.execute("SELECT * FROM my_table LIMIT 5").fetchdf()
print(result)
```

## CSV Reading Options

DuckDB's `read_csv_auto` function supports various options:

```python
con.execute("""
    CREATE TABLE my_table AS 
    SELECT * FROM read_csv_auto(
        'file.csv',
        header=true,           # First row contains column names
        delimiter=',',         # Column separator
        auto_detect=true,      # Automatically detect data types
        sample_size=1000,      # Number of rows to sample for type detection
        null_padding=false,    # Whether to pad shorter rows with NULL
        ignore_errors=false    # Whether to ignore parsing errors
    )
""")
```

## Database Storage Options

- **In-memory database**: `duckdb.connect(':memory:')` - Fast, but data is lost when connection closes
- **File-based database**: `duckdb.connect('my_database.db')` - Persistent storage

## Example Queries

Once your data is loaded, you can run SQL queries:

```python
# Basic select
result = con.execute("SELECT * FROM my_table").fetchdf()

# Filtering
result = con.execute("SELECT * FROM my_table WHERE age > 30").fetchdf()

# Aggregation
result = con.execute("""
    SELECT city, AVG(salary) as avg_salary 
    FROM my_table 
    GROUP BY city 
    ORDER BY avg_salary DESC
""").fetchdf()

# Joins (if you have multiple tables)
result = con.execute("""
    SELECT t1.name, t2.department 
    FROM table1 t1 
    JOIN table2 t2 ON t1.id = t2.employee_id
""").fetchdf()
```

## Features

- **Automatic type detection**: DuckDB automatically detects data types from your CSV
- **Error handling**: Comprehensive error handling for file not found and parsing issues
- **Multiple file support**: Can read multiple CSV files into separate tables
- **SQL queries**: Full SQL support for querying the loaded data
- **Pandas integration**: Easy conversion to pandas DataFrames with `.fetchdf()`

## Troubleshooting

1. **File not found**: Make sure the CSV file path is correct
2. **Permission errors**: Ensure you have read permissions for the CSV file
3. **Encoding issues**: If you have special characters, try specifying the encoding:
   ```python
   con.execute(f"""
       CREATE TABLE my_table AS 
       SELECT * FROM read_csv_auto('{csv_path}', encoding='utf-8')
   """)
   ``` 