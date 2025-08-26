# Cell 11: Useful query functions
def query_by_locality(locality_name):
    """Query records by locality"""
    query = f"""
    SELECT locality, polling_area, first_name, last_name, age, gender 
    FROM {TABLE_NAME} 
    WHERE LOWER(locality) LIKE LOWER('%{locality_name}%')
    ORDER BY last_name, first_name
    """
    return conn.execute(query).fetchdf()

def get_age_distribution():
    """Get age distribution statistics"""
    query = f"""
    SELECT 
        CASE 
            WHEN age BETWEEN 18 AND 25 THEN '18-25'
            WHEN age BETWEEN 26 AND 35 THEN '26-35'
            WHEN age BETWEEN 36 AND 45 THEN '36-45'
            WHEN age BETWEEN 46 AND 55 THEN '46-55'
            WHEN age BETWEEN 56 AND 65 THEN '56-65'
            WHEN age > 65 THEN '65+'
            ELSE 'Unknown'
        END as age_group,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {TABLE_NAME}
    GROUP BY age_group
    ORDER BY age_group
    """
    return conn.execute(query).fetchdf()

def search_person(first_name=None, last_name=None, house_number=None):
    """Search for a specific person"""
    conditions = []
    if first_name:
        conditions.append(f"LOWER(first_name) LIKE LOWER('%{first_name}%')")
    if last_name:
        conditions.append(f"LOWER(last_name) LIKE LOWER('%{last_name}%')")
    if house_number:
        conditions.append(f"house_number = '{house_number}'")
    
    if not conditions:
        return "Please provide at least one search parameter"
    
    where_clause = " AND ".join(conditions)
    query = f"""
    SELECT * FROM {TABLE_NAME} 
    WHERE {where_clause}
    ORDER BY locality, polling_area, last_name, first_name
    """
    return conn.execute(query).fetchdf()

# Example usage
print("\nExample: Age distribution")
age_dist = get_age_distribution()
print(age_dist)

# Cell 12: Save and close
# The connection will remain open for further queries
# To close: conn.close()

print(f"\n{'='*60}")
print(f"DATABASE READY")
print(f"{'='*60}")
print(f"Database file: {DUCKDB_PATH}")
print(f"Table name: {TABLE_NAME}")
print(f"Connection: conn (still open)")
print(f"\nUse the provided functions or write custom SQL queries to explore the data.")
print(f"Example: query_by_locality('your_locality_name')")
print(f"Example: search_person(first_name='John', last_name='Smith')")

# Uncomment to close connection
conn.close()