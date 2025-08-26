import streamlit as st
import duckdb
import pandas as pd
import os
import math
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Voter Records Search",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Remove default padding and margins
st.markdown("""
<style>
    .block-container {
        padding-top: 0rem;
        padding-bottom: 0rem;
        padding-left: 1rem;
        padding-right: 1rem;
        max-width: 100%;
    }
    
    .main > div {
        padding-top: 0rem;
    }
    
    header[data-testid="stHeader"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Custom CSS for better styling
st.markdown("""
<style>
.search-pane {
    background-color: #ffffff;
    padding: 10px;
    margin-bottom: 5px;
}

.results-pane {
    background-color: #ffffff;
    padding: 10px;
    margin-bottom: 5px;
}

.search-header {
    color: #1f77b4;
    font-size: 1.3rem;
    font-weight: 600;
    margin-bottom: 10px;
    border-bottom: 2px solid #1f77b4;
    padding-bottom: 5px;
}

.results-header {
    color: #1f77b4;
    font-size: 1.3rem;
    font-weight: 600;
    margin-bottom: 10px;
    border-bottom: 2px solid #1f77b4;
    padding-bottom: 5px;
}

.stats-inline {
    color: #666;
    font-size: 0.9rem;
    margin-bottom: 10px;
}

.stButton > button {
    background: linear-gradient(135deg, #2196f3 0%, #1976d2 100%);
    color: white;
    border: none;
    padding: 12px 24px;
    border-radius: 8px;
    font-weight: 600;
    font-size: 16px;
    cursor: pointer;
    transition: all 0.3s ease;
    box-shadow: 0 4px 8px rgba(33, 150, 243, 0.3);
}

.stButton > button:hover {
    background: linear-gradient(135deg, #1976d2 0%, #1565c0 100%);
    transform: translateY(-2px);
    box-shadow: 0 6px 12px rgba(33, 150, 243, 0.4);
}

.pagination-container {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 20px 0;
    border-top: 2px solid #e0e0e0;
    margin-top: 20px;
}

.pagination-info {
    color: #666;
    font-weight: 500;
}

.stSelectbox > div > div > select {
    border: 2px solid #2196f3;
    border-radius: 5px;
}

.stTextInput > div > div > input {
    border: 2px solid #e0e0e0;
    border-radius: 5px;
    transition: border-color 0.3s ease;
}

.stTextInput > div > div > input:focus {
    border-color: #2196f3;
    box-shadow: 0 0 0 0.2rem rgba(33, 150, 243, 0.25);
}
</style>
""", unsafe_allow_html=True)

# Database configuration
DUCKDB_PATH = "voter_data.duckdb"
TABLE_NAME = "Delhi_Voter"

# Initialize session state for pagination
if 'page_number' not in st.session_state:
    st.session_state.page_number = 0
if 'rows_per_page' not in st.session_state:
    st.session_state.rows_per_page = 20
if 'search_results' not in st.session_state:
    st.session_state.search_results = pd.DataFrame()
if 'total_results' not in st.session_state:
    st.session_state.total_results = 0

@st.cache_resource
def init_database():
    """Initialize database connection"""
    if not os.path.exists(DUCKDB_PATH):
        st.error(f"Database file '{DUCKDB_PATH}' not found. Please run the CSV to DuckDB notebook first.")
        st.stop()
    
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        return conn
    except Exception as e:
        try:
            conn = duckdb.connect(DUCKDB_PATH)
            return conn
        except Exception as e2:
            st.error(f"Failed to connect to database: {e2}")
            st.error("Please close any other applications using the database file.")
            st.stop()

@st.cache_data
def load_localities(_conn):
    """Load all unique localities for dropdown"""
    try:
        table_check = _conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'").fetchone()
        if not table_check:
            st.error(f"Table '{TABLE_NAME}' not found in database")
            return []
        
        columns = _conn.execute(f"PRAGMA table_info({TABLE_NAME})").fetchdf()
        if 'locality' not in columns['name'].values:
            st.error("Column 'locality' not found in table")
            return []
        
        query = f"SELECT DISTINCT locality FROM {TABLE_NAME} WHERE locality IS NOT NULL AND locality != '' ORDER BY locality"
        result = _conn.execute(query).fetchall()
        localities = [row[0] for row in result if row[0]]
        return localities
    except Exception as e:
        st.error(f"Failed to load localities: {e}")
        return []

@st.cache_data
def get_database_stats(_conn):
    """Get basic database statistics"""
    try:
        stats = {}
        table_check = _conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'").fetchone()
        if not table_check:
            return {}
        
        columns = _conn.execute(f"PRAGMA table_info({TABLE_NAME})").fetchdf()
        column_names = columns['name'].values
        
        result = _conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()
        stats['total_records'] = result[0]
        
        if 'locality' in column_names:
            result = _conn.execute(f"SELECT COUNT(DISTINCT locality) FROM {TABLE_NAME} WHERE locality IS NOT NULL").fetchone()
            stats['unique_localities'] = result[0]
        else:
            stats['unique_localities'] = 0
        
        return stats
    except Exception as e:
        st.error(f"Failed to get database stats: {e}")
        return {}

def search_persons_paginated(conn, first_name=None, last_name=None, locality=None, 
                           relation_first_name=None, relation_last_name=None, 
                           offset=0, limit=20):
    """Search for persons with pagination"""
    conditions = []
    params = {}
    
    if first_name and first_name.strip():
        conditions.append("LOWER(first_name) LIKE LOWER($first_name)")
        params['first_name'] = f"%{first_name.strip()}%"
    
    if last_name and last_name.strip():
        conditions.append("LOWER(last_name) LIKE LOWER($last_name)")
        params['last_name'] = f"%{last_name.strip()}%"
    
    if locality and locality != "All":
        conditions.append("locality = $locality")
        params['locality'] = locality
    
    if relation_first_name and relation_first_name.strip():
        conditions.append("LOWER(relation_first_name) LIKE LOWER($relation_first_name)")
        params['relation_first_name'] = f"%{relation_first_name.strip()}%"
    
    if relation_last_name and relation_last_name.strip():
        conditions.append("LOWER(relation_last_name) LIKE LOWER($relation_last_name)")
        params['relation_last_name'] = f"%{relation_last_name.strip()}%"
    
    if not conditions:
        return pd.DataFrame(), 0
    
    where_clause = " AND ".join(conditions)
    
    try:
        table_check = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'").fetchone()
        if not table_check:
            st.error(f"Table '{TABLE_NAME}' not found in database")
            return pd.DataFrame(), 0
        
        columns = conn.execute(f"PRAGMA table_info({TABLE_NAME})").fetchdf()
        available_columns = columns['name'].values
        
        desired_columns = ['locality', 'house_number', 'first_name', 'last_name', 
                          'relation', 'relation_first_name', 'relation_last_name', 'gender']
        
        select_columns = [col for col in desired_columns if col in available_columns]
        
        if not select_columns:
            select_columns = list(available_columns)
        
        select_clause = ", ".join(select_columns)
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {where_clause}"
        total_count = conn.execute(count_query, params).fetchone()[0]
        
        # Get paginated results
        query = f"""
        SELECT {select_clause}
        FROM {TABLE_NAME} 
        WHERE {where_clause}
        ORDER BY {select_columns[0] if select_columns else '1'}
        LIMIT {limit} OFFSET {offset}
        """
        
        result = conn.execute(query, params).fetchdf()
        return result, total_count
        
    except Exception as e:
        st.error(f"Search query failed: {e}")
        return pd.DataFrame(), 0

def create_pagination_controls(total_records, current_page, rows_per_page):
    """Create pagination controls with buttons on left, rows selector on right, info below"""
    total_pages = math.ceil(total_records / rows_per_page) if total_records > 0 else 1
    
    # First row: Navigation buttons on left, rows per page on right
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        # Navigation buttons
        btn_col1, btn_col2, btn_col3, btn_col4 = st.columns([1, 1, 1, 1])
        with btn_col1:
            if st.button("⏮️", disabled=(current_page == 0), help="First page", key="first_btn"):
                st.session_state.page_number = 0
                st.rerun()
        
        with btn_col2:
            if st.button("⬅️", disabled=(current_page == 0), help="Previous page", key="prev_btn"):
                st.session_state.page_number = max(0, current_page - 1)
                st.rerun()
        
        with btn_col3:
            if st.button("➡️", disabled=(current_page >= total_pages - 1), help="Next page", key="next_btn"):
                st.session_state.page_number = min(total_pages - 1, current_page + 1)
                st.rerun()
        
        with btn_col4:
            if st.button("⏭️", disabled=(current_page >= total_pages - 1), help="Last page", key="last_btn"):
                st.session_state.page_number = total_pages - 1
                st.rerun()
    
    with col_right:
        # Rows per page selector
        new_rows_per_page = st.selectbox(
            "Rows per page:",
            options=[20, 50, 100],
            index=[20, 50, 100].index(st.session_state.rows_per_page),
            key="rows_selector_compact"
        )
        if new_rows_per_page != st.session_state.rows_per_page:
            st.session_state.rows_per_page = new_rows_per_page
            st.session_state.page_number = 0  # Reset to first page
            st.rerun()
    
    # Second row: Page information centered
    start_record = current_page * rows_per_page + 1
    end_record = min((current_page + 1) * rows_per_page, total_records)
    st.markdown(f"""
    <div style="text-align: center; padding: 8px; color: #666; font-size: 0.9rem;">
        <strong>Page {current_page + 1} of {total_pages}</strong> | Showing {start_record}-{end_record} of {total_records} records
    </div>
    """, unsafe_allow_html=True)

def main():
    # Initialize database
    conn = init_database()
    
    # Create two columns for left and right panes immediately
    left_pane, right_pane = st.columns([1, 2], gap="medium")
    
    # LEFT PANE - Search Criteria
    with left_pane:
        st.markdown('<div class="search-pane">', unsafe_allow_html=True)
        st.markdown('<div class="search-header">🔎 Search Criteria</div>', unsafe_allow_html=True)
        
        # Load localities for dropdown
        localities = load_localities(conn)
        
        # Search form - more compact
        with st.form("search_form"):
            st.markdown("**👤 Personal Information**")
            first_name = st.text_input(
                "First Name",
                placeholder="Enter first name...",
                help="Search by person's first name"
            )
            
            last_name = st.text_input(
                "Last Name", 
                placeholder="Enter last name...",
                help="Search by person's last name"
            )
            
            st.markdown("**📍 Location**")
            locality_options = ["All"] + localities
            locality = st.selectbox(
                "Locality",
                options=locality_options,
                help="Select a specific locality"
            )
            
            st.markdown("**👥 Relation Information**")
            relation_first_name = st.text_input(
                "Relation First Name",
                placeholder="Enter relation's first name...",
                help="Search by relation's first name"
            )
            
            relation_last_name = st.text_input(
                "Relation Last Name",
                placeholder="Enter relation's last name...",
                help="Search by relation's last name"
            )
            
            # Search button
            search_clicked = st.form_submit_button("🔍 Search Records", use_container_width=True)
        
        # Search tips
        st.markdown("💡 **Tips:** Use partial names • Combine filters • Try different spellings")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # RIGHT PANE - Results
    with right_pane:
        st.markdown('<div class="results-pane">', unsafe_allow_html=True)
        st.markdown('<div class="results-header">📋 Search Results</div>', unsafe_allow_html=True)
        
        # Database stats moved to right pane
        stats = get_database_stats(conn)
        if stats:
            st.markdown(f'<div class="stats-inline">📊 Total Records: {stats.get("total_records", 0):,} | 🏘️ Localities: {stats.get("unique_localities", 0)}</div>', unsafe_allow_html=True)
        
        # Handle search
        if search_clicked:
            # Reset pagination when new search is performed
            st.session_state.page_number = 0
            
            # Validate search criteria
            if not any([first_name, last_name, locality != "All", relation_first_name, relation_last_name]):
                st.warning("⚠️ Please provide at least one search criterion.")
            else:
                # Show search summary
                search_terms = []
                if first_name: search_terms.append(f"First Name: '{first_name}'")
                if last_name: search_terms.append(f"Last Name: '{last_name}'")
                if locality != "All": search_terms.append(f"Locality: '{locality}'")
                if relation_first_name: search_terms.append(f"Relation First Name: '{relation_first_name}'")
                if relation_last_name: search_terms.append(f"Relation Last Name: '{relation_last_name}'")
                
                st.info(f"🔎 Searching: {' | '.join(search_terms)}")
                
                # Perform search
                with st.spinner("Searching records..."):
                    # Store search parameters in session state
                    st.session_state.search_params = {
                        'first_name': first_name,
                        'last_name': last_name,
                        'locality': locality,
                        'relation_first_name': relation_first_name,
                        'relation_last_name': relation_last_name
                    }
        
        # Display results if we have search parameters
        if hasattr(st.session_state, 'search_params'):
            # Calculate offset for pagination
            offset = st.session_state.page_number * st.session_state.rows_per_page
            
            # Perform paginated search
            params = st.session_state.search_params
            results, total_count = search_persons_paginated(
                conn, 
                params['first_name'], 
                params['last_name'], 
                params['locality'],
                params['relation_first_name'], 
                params['relation_last_name'],
                offset, 
                st.session_state.rows_per_page
            )
            
            if results.empty:
                st.warning("🚫 No records found matching your search criteria.")
                st.markdown("""
                **Try:**
                - Using partial names instead of full names
                - Checking spelling of names  
                - Removing some filters to broaden the search
                - Selecting 'All' for locality
                """)
            else:
                # Results summary
                col1, col2 = st.columns(2)
                with col1:
                    st.success(f"✅ Found {total_count:,} total records")
                with col2:
                    unique_localities = results['locality'].nunique() if 'locality' in results.columns else 0
                    st.info(f"🏘️ {unique_localities} localities in current page")
                
                # Display results table
                display_results = results.copy()
                display_results.columns = [col.replace('_', ' ').title() for col in display_results.columns]
                display_results = display_results.fillna('N/A')
                
                st.dataframe(
                    display_results,
                    use_container_width=True,
                    hide_index=True,
                    height=500
                )
                
                # Pagination controls with new layout
                if total_count > st.session_state.rows_per_page:
                    create_pagination_controls(total_count, st.session_state.page_number, st.session_state.rows_per_page)
                else:
                    # Show just the rows per page selector when no pagination needed
                    col_left, col_right = st.columns([2, 1])
                    with col_right:
                        rows_per_page = st.selectbox(
                            "Rows per page:",
                            options=[20, 50, 100],
                            index=[20, 50, 100].index(st.session_state.rows_per_page),
                            key="rows_selector_no_pagination"
                        )
                        if rows_per_page != st.session_state.rows_per_page:
                            st.session_state.rows_per_page = rows_per_page
                            st.session_state.page_number = 0
                            st.rerun()
                    
                    # Show record count even when no pagination
                    st.markdown(f"""
                    <div style="text-align: center; padding: 8px; color: #666; font-size: 0.9rem;">
                        Showing all {total_count} records
                    </div>
                    """, unsafe_allow_html=True)
                
                # Download option
                if total_count > 0:
                    # Get all results for download (limited to 10000 for performance)
                    all_results, _ = search_persons_paginated(
                        conn, 
                        params['first_name'], 
                        params['last_name'], 
                        params['locality'],
                        params['relation_first_name'], 
                        params['relation_last_name'],
                        0, 
                        min(total_count, 10000)
                    )
                    
                    if not all_results.empty:
                        csv = all_results.to_csv(index=False)
                        st.download_button(
                            label=f"📥 Download All Results ({min(total_count, 10000):,} records)",
                            data=csv,
                            file_name=f"voter_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                        
                        if total_count > 10000:
                            st.info("ℹ️ Download limited to first 10,000 records.")
        
        else:
            # Initial state - show welcome message
            st.markdown("""
            <div style="text-align: center; padding: 20px; color: #666;">
                <h4>🔍 Enter search criteria and click "Search Records"</h4>
                <p>Search by name, locality, or relation information</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer - more compact
    st.markdown(
        "<div style='text-align: center; color: #999; padding: 10px; font-size: 0.8rem;'>"
        "💡 This application searches voter records in a secure local database"
        "</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()