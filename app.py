import streamlit as st
import pandas as pd
import plotly.express as px
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import os
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
from shapely.geometry import Point, Polygon
import json

# Page config
st.set_page_config(
    page_title="Crown Lease Management",
    page_icon="👑",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for FreeWheel-like UI
st.markdown("""
<style>
    /* Hide default elements */
    [data-testid="stStatusWidget"] { display: none; }
    .stSpinner { display: none; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    [data-testid="stSidebar"] { display: none; }
    
    /* White background */
    .stApp {
        background-color: #ffffff;
    }
    [data-testid="stAppViewContainer"] {
        background-color: #ffffff;
    }
    [data-testid="stHeader"] {
        background-color: #ffffff;
    }
    
    /* Main container styling */
    .main .block-container {
        padding-top: 2rem;
        padding-left: 3rem;
        padding-right: 3rem;
        max-width: 100%;
        background-color: #ffffff;
    }
    
    /* Header styling */
    .app-header {
        margin-bottom: 1.5rem;
    }
    .app-title {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1a1a1a;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .app-subtitle {
        font-size: 0.85rem;
        color: #666;
        margin-top: 0.25rem;
    }
    
    /* Tab navigation */
    .tab-container {
        display: flex;
        gap: 0.75rem;
        margin: 1.5rem 0;
    }
    .tab-button {
        padding: 0.6rem 1.5rem;
        border-radius: 6px;
        font-size: 0.9rem;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.2s;
        text-decoration: none;
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
    }
    .tab-active {
        background: linear-gradient(135deg, #e63946 0%, #f4a261 100%);
        color: white;
        border: none;
    }
    .tab-inactive {
        background: white;
        color: #333;
        border: 1px solid #ddd;
    }
    .tab-inactive:hover {
        border-color: #e63946;
        color: #e63946;
    }
    
    /* Metric cards */
    .metric-card {
        background: white;
        border: 1px solid #e5e5e5;
        border-radius: 8px;
        padding: 1rem 1.25rem;
        height: 100%;
    }
    .metric-label {
        font-size: 0.75rem;
        font-weight: 600;
        padding: 0.2rem 0.5rem;
        border-radius: 4px;
        display: inline-flex;
        align-items: center;
        gap: 0.3rem;
    }
    .metric-label-green { background: #d4edda; color: #155724; }
    .metric-label-blue { background: #cce5ff; color: #004085; }
    .metric-label-orange { background: #fff3cd; color: #856404; }
    .metric-label-red { background: #f8d7da; color: #721c24; }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1a1a1a;
        margin-top: 0.5rem;
    }
    
    /* Section headers */
    .section-header {
        font-size: 1rem;
        font-weight: 600;
        color: #1a1a1a;
        margin: 1.5rem 0 1rem 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    /* Filter pills */
    .filter-pill {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        margin-right: 0.5rem;
    }
    .pill-green { background: #28a745; color: white; }
    .pill-yellow { background: #ffc107; color: #333; }
    .pill-red { background: #dc3545; color: white; }
    
    /* Hide Streamlit branding */
    .viewerBadge_container__1QSob { display: none; }
    
    /* Clean up selectbox - white background, black text */
    .stSelectbox > div > div { 
        border-radius: 6px; 
        background-color: #ffffff;
    }
    .stSelectbox [data-baseweb="select"] {
        background-color: #ffffff;
    }
    .stSelectbox label {
        background-color: #ffffff;
        color: #1a1a1a !important;
        font-weight: 600;
    }
    .stSelectbox [data-baseweb="select"] > div {
        color: #1a1a1a !important;
    }
    .stSelectbox [data-testid="stWidgetLabel"] {
        color: #1a1a1a !important;
    }
    .stSelectbox p {
        color: #1a1a1a !important;
    }
    /* Dropdown selected value text */
    [data-baseweb="select"] span {
        color: #1a1a1a !important;
    }
    /* Dropdown menu/popover - white background, black text */
    [data-baseweb="popover"] {
        background-color: #ffffff !important;
    }
    [data-baseweb="menu"] {
        background-color: #ffffff !important;
    }
    [data-baseweb="menu"] li {
        background-color: #ffffff !important;
        color: #1a1a1a !important;
    }
    [data-baseweb="menu"] li:hover {
        background-color: #f0f0f0 !important;
    }
    [role="listbox"] {
        background-color: #ffffff !important;
    }
    [role="option"] {
        background-color: #ffffff !important;
        color: #1a1a1a !important;
    }
    [role="option"]:hover {
        background-color: #f0f0f0 !important;
    }
    
    /* Button styling - white background for inactive, black text */
    .stButton > button {
        border-radius: 6px;
        background-color: #ffffff;
        color: #1a1a1a !important;
    }
    .stButton > button[kind="secondary"] {
        background-color: #ffffff;
        color: #1a1a1a !important;
        border: 1px solid #ddd;
    }
    .stButton > button[kind="secondary"]:hover {
        border-color: #e63946;
        color: #e63946 !important;
    }
    .stButton > button[kind="primary"] {
        color: #ffffff !important;
    }
    
    /* Dataframe styling - white background, black text */
    .stDataFrame { 
        border-radius: 8px; 
        overflow: hidden; 
        background-color: #ffffff !important;
        --gdg-bg-cell: #ffffff !important;
        --gdg-text-dark: #1a1a1a !important;
        --gdg-text-medium: #1a1a1a !important;
        --gdg-text-light: #1a1a1a !important;
        --gdg-bg-header: #f8f9fa !important;
        --gdg-text-header: #1a1a1a !important;
    }
    [data-testid="stDataFrame"] {
        background-color: #ffffff !important;
    }
    [data-testid="stDataFrame"] > div {
        background-color: #ffffff !important;
    }
    [data-testid="stDataFrame"] * {
        color: #1a1a1a !important;
    }
    [data-testid="stDataFrame"] table {
        background-color: #ffffff !important;
    }
    [data-testid="stDataFrame"] th {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    [data-testid="stDataFrame"] td {
        background-color: #ffffff !important;
        color: #1a1a1a !important;
    }
    [data-testid="stDataFrame"] [data-testid="glideDataEditor"] {
        background-color: #ffffff !important;
    }
    /* Glide data editor cells - force black text */
    .dvn-scroller {
        background-color: #ffffff !important;
    }
    .dvn-scroller canvas {
        filter: none !important;
    }
    [data-testid="glideDataEditor"] {
        --gdg-bg-cell: #ffffff !important;
        --gdg-text-dark: #1a1a1a !important;
        --gdg-text-medium: #333333 !important;
        --gdg-text-light: #666666 !important;
        --gdg-bg-header: #f8f9fa !important;
        --gdg-text-header: #1a1a1a !important;
        --gdg-bg-header-has-focus: #f0f0f0 !important;
        --gdg-bg-header-hovered: #f0f0f0 !important;
    }
    [data-testid="glideDataEditor"] * {
        color: #1a1a1a !important;
    }
    .gdg-header {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    /* Force canvas text to be dark */
    canvas {
        filter: invert(0) !important;
    }
    
    /* Info/warning boxes - white background, black text */
    .stAlert {
        background-color: #ffffff;
    }
    .stAlert > div {
        color: #1a1a1a !important;
    }
    .stAlert p {
        color: #1a1a1a !important;
    }
    .stAlert span {
        color: #1a1a1a !important;
    }
    [data-testid="stAlert"] {
        color: #1a1a1a !important;
    }
    [data-testid="stAlert"] * {
        color: #1a1a1a !important;
    }
    /* Info box specifically */
    .stInfo {
        background-color: #e8f4f8 !important;
    }
    .stInfo * {
        color: #1a1a1a !important;
    }
    
    /* Chat input - white background, black text */
    .stChatInput {
        background-color: #ffffff !important;
    }
    .stChatInput textarea {
        color: #1a1a1a !important;
        background-color: #ffffff !important;
    }
    .stChatInput input {
        color: #1a1a1a !important;
        background-color: #ffffff !important;
    }
    [data-testid="stChatInput"] {
        background-color: #ffffff !important;
    }
    [data-testid="stChatInput"] > div {
        background-color: #ffffff !important;
    }
    [data-testid="stChatInput"] textarea {
        color: #1a1a1a !important;
        background-color: #ffffff !important;
    }
    [data-testid="stChatInput"] * {
        color: #1a1a1a !important;
        background-color: #ffffff !important;
    }
    /* Chat input container and wrapper */
    [data-testid="stChatInputContainer"] {
        background-color: #ffffff !important;
    }
    [data-testid="stChatInputContainer"] * {
        background-color: #ffffff !important;
        color: #1a1a1a !important;
    }
    /* Bottom container for chat */
    [data-testid="stBottomBlockContainer"] {
        background-color: #ffffff !important;
    }
    [data-testid="stBottomBlockContainer"] * {
        background-color: #ffffff !important;
    }
    /* Placeholder text */
    [data-testid="stChatInput"] textarea::placeholder {
        color: #888888 !important;
    }
    
    /* Chat messages */
    [data-testid="stChatMessage"] {
        background-color: #ffffff !important;
    }
    [data-testid="stChatMessage"] * {
        color: #1a1a1a !important;
    }
    .stChatMessage {
        background-color: #ffffff !important;
        color: #1a1a1a !important;
    }
    
    /* Text input - white background, black text */
    .stTextInput > div > div {
        background-color: #ffffff;
    }
    .stTextInput input {
        color: #1a1a1a !important;
        background-color: #ffffff !important;
    }
    .stTextInput label {
        color: #1a1a1a !important;
    }
    
    /* Captions and labels */
    .stCaption {
        color: #666666 !important;
    }
    [data-testid="stCaption"] {
        color: #666666 !important;
    }
    
    /* Success/info/warning boxes text */
    .stSuccess, .stInfo, .stWarning, .stError {
        color: #1a1a1a !important;
    }
    .stSuccess *, .stInfo *, .stWarning *, .stError * {
        color: #1a1a1a !important;
    }
    
    /* All markdown text */
    .stMarkdown {
        color: #1a1a1a !important;
    }
    .stMarkdown * {
        color: #1a1a1a !important;
    }
    
    /* Section headers */
    .section-header {
        color: #1a1a1a !important;
    }
    
    /* General text color override */
    p, span, div, label {
        color: #1a1a1a;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: #ffffff !important;
        color: #1a1a1a !important;
    }
    .streamlit-expanderContent {
        background-color: #ffffff !important;
        color: #1a1a1a !important;
    }
    [data-testid="stExpander"] {
        background-color: #ffffff !important;
    }
    [data-testid="stExpander"] * {
        color: #1a1a1a !important;
    }
    
    /* Code blocks - white background, black text */
    .stCodeBlock {
        background-color: #f8f9fa !important;
    }
    .stCodeBlock code {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    .stCodeBlock pre {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    [data-testid="stCodeBlock"] {
        background-color: #f8f9fa !important;
    }
    [data-testid="stCodeBlock"] * {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    [data-testid="stCodeBlock"] code {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    [data-testid="stCodeBlock"] pre {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    /* Code element styling */
    code {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    pre {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    /* Syntax highlighting override */
    .hljs {
        background-color: #f8f9fa !important;
        color: #1a1a1a !important;
    }
    .hljs * {
        color: #1a1a1a !important;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Databricks configuration and workspace client (module level like working example)
cfg = Config()
w = WorkspaceClient()

# Initialize connection with auto-reconnect
def get_connection():
    """Get a fresh SQL connection each time to avoid stale connections"""
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.environ.get('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    )

def execute_query_with_retry(query, max_retries=2):
    """Execute a query with automatic retry on connection errors"""
    for attempt in range(max_retries):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            conn.close()
            return result, columns
        except Exception as e:
            if attempt < max_retries - 1:
                continue  # Retry
            raise e

# Query data
@st.cache_data(ttl=60)  # Shorter TTL to refresh data more often
def query_data(state_filter=None, doc_type_filter=None, tenant_filter=None):
    query = """
    SELECT 
        site_name,
        state,
        document_type,
        tenant_name,
        latitude,
        longitude,
        total_monthly_revenue,
        lease_status,
        days_until_expiration,
        revenue_per_sqft,
        insurance_liability_min_usd,
        equipment_space_sqft,
        compliance_status
    FROM bricks_demo.crown_demo.synth_data
    WHERE latitude IS NOT NULL 
        AND longitude IS NOT NULL
        AND lease_status = 'Active'
    """

    if state_filter and state_filter != "All":
        query += f" AND state = '{state_filter}'"
    if doc_type_filter and doc_type_filter != "All":
        query += f" AND document_type = '{doc_type_filter}'"
    if tenant_filter and tenant_filter != "All":
        query += f" AND tenant_name = '{tenant_filter}'"

    result, columns = execute_query_with_retry(query)
    return pd.DataFrame(result, columns=columns)

# Get filter options
@st.cache_data(ttl=60)  # Shorter TTL
def get_filter_options():
    # States
    result, _ = execute_query_with_retry("SELECT DISTINCT state FROM bricks_demo.crown_demo.synth_data WHERE state IS NOT NULL ORDER BY state")
    states = ["All"] + [row[0] for row in result]

    # Document types
    result, _ = execute_query_with_retry("SELECT DISTINCT document_type FROM bricks_demo.crown_demo.synth_data WHERE document_type IS NOT NULL ORDER BY document_type")
    doc_types = ["All"] + [row[0] for row in result]

    # Tenants
    result, _ = execute_query_with_retry("SELECT DISTINCT tenant_name FROM bricks_demo.crown_demo.synth_data WHERE tenant_name IS NOT NULL ORDER BY tenant_name")
    tenants = ["All"] + [row[0] for row in result]

    return states, doc_types, tenants

# Get site locations for Genie map
@st.cache_data(ttl=60)  # Shorter TTL
def get_site_locations():
    """Fetch all site locations for displaying on the Genie map"""
    query = """
    SELECT 
        site_name,
        latitude,
        longitude,
        state,
        tenant_name,
        total_monthly_revenue
    FROM bricks_demo.crown_demo.synth_data
    WHERE latitude IS NOT NULL 
        AND longitude IS NOT NULL
    """

    result, columns = execute_query_with_retry(query)
    return pd.DataFrame(result, columns=columns)

def check_point_in_polygon(lat, lon, polygon_coords):
    """Check if a point is inside a polygon using Shapely (for map highlighting)"""
    try:
        point = Point(lon, lat)  # Note: shapely uses (lon, lat) order
        polygon = Polygon(polygon_coords)
        # Use intersects to match Databricks ST_Intersects behavior (includes boundary)
        return polygon.intersects(point)
    except:
        return False

def count_sites_in_polygon_db(polygon_coords):
    """Query Databricks directly to count sites in polygon using ST_Intersects"""
    if not polygon_coords:
        return 0

    try:
        # Build WKT polygon string
        wkt_coords = ", ".join([f"{coord[0]:.6f} {coord[1]:.6f}" for coord in polygon_coords])

        # Ensure polygon is closed
        first_coord = polygon_coords[0]
        last_coord = polygon_coords[-1]
        if first_coord[0] != last_coord[0] or first_coord[1] != last_coord[1]:
            wkt_coords += f", {first_coord[0]:.6f} {first_coord[1]:.6f}"

        wkt_polygon = f"POLYGON(({wkt_coords}))"

        query = f"""
        SELECT COUNT(DISTINCT site_name) as cnt
        FROM bricks_demo.crown_demo.synth_data
        WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL
            AND ST_Intersects(
                ST_GeomFromWKT('{wkt_polygon}'),
                ST_Point(longitude, latitude)
            )
        """

        result, _ = execute_query_with_retry(query)
        return result[0][0] if result else 0
    except Exception as e:
        # Fall back to local calculation if DB query fails
        return -1  # Return -1 to indicate error

def format_polygon_for_query(polygon_coords):
    """Format polygon coordinates using Databricks ST geospatial functions for Genie"""
    if not polygon_coords:
        return ""

    # Build WKT (Well-Known Text) polygon string
    # Folium returns coords as [longitude, latitude], WKT expects "longitude latitude"
    wkt_coords = ", ".join([f"{coord[0]:.6f} {coord[1]:.6f}" for coord in polygon_coords])

    # Ensure polygon is closed (first point = last point)
    first_coord = polygon_coords[0]
    last_coord = polygon_coords[-1]
    if first_coord[0] != last_coord[0] or first_coord[1] != last_coord[1]:
        wkt_coords += f", {first_coord[0]:.6f} {first_coord[1]:.6f}"

    wkt_polygon = f"POLYGON(({wkt_coords}))"

    # Build the ST_Intersects filter clause (includes points on boundary, unlike ST_Contains)
    st_filter = f"""ST_Intersects(
    ST_GeomFromWKT('{wkt_polygon}'),
    ST_Point(longitude, latitude)
)"""

    return f"""
[GEOGRAPHIC FILTER ACTIVE - USE DATABRICKS ST FUNCTIONS]
The user has drawn a polygon on the map. You MUST filter results to only include sites within this polygon.

Use this exact WHERE clause filter:
```sql
WHERE {st_filter}
```

The polygon WKT: {wkt_polygon}

IMPORTANT: 
- Use ST_Intersects (not ST_Contains) to include points on the polygon boundary
- ST_GeomFromWKT creates the polygon geometry from WKT format
- ST_Point(longitude, latitude) creates a point - longitude is first, latitude is second
- Include this filter in your SQL query to get only sites within the drawn polygon
"""

# Main app
def main():
    # App Header
    st.markdown("""
    <div class="app-header">
        <div class="app-title">👑 Crown Lease Management</div>
        <div class="app-subtitle">Demo MVP • Powered by Databricks</div>
    </div>
    """, unsafe_allow_html=True)

    # Initialize tab state
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = 0

    # Tab navigation - horizontal buttons
    tabs = [
        ("📊", "Dashboard"),
        ("🤖", "Genie Data Chat"),
        ("📚", "Knowledge Assistant"),
        ("👥", "Multi-Agent Supervisor")
    ]

    cols = st.columns(len(tabs))
    for i, (icon, label) in enumerate(tabs):
        with cols[i]:
            btn_type = "primary" if st.session_state.active_tab == i else "secondary"
            if st.button(f"{icon} {label}", key=f"tab_{i}", type=btn_type, use_container_width=True):
                st.session_state.active_tab = i
                st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    # Show selected tab content
    if st.session_state.active_tab == 0:
        show_dashboard()
    elif st.session_state.active_tab == 1:
        show_genie_space()
    elif st.session_state.active_tab == 2:
        show_knowledge_assistant()
    elif st.session_state.active_tab == 3:
        show_multi_agent_supervisor()

def show_dashboard():
    st.markdown('<div class="section-header">📊 Lease Portfolio Overview</div>', unsafe_allow_html=True)

    # Get filter options
    try:
        states, doc_types, tenants = get_filter_options()

        # Initialize session state for site filter
        if 'selected_site' not in st.session_state:
            st.session_state.selected_site = None

        # Filters in columns
        col1, col2, col3 = st.columns(3)
        with col1:
            state_filter = st.selectbox("State", states)
        with col2:
            doc_type_filter = st.selectbox("Document Type", doc_types)
        with col3:
            tenant_filter = st.selectbox("Tenant Name", tenants)

        # Query data with site filter
        df = query_data(state_filter, doc_type_filter, tenant_filter)

        # Apply site filter if selected from map
        if st.session_state.selected_site:
            df = df[df['site_name'] == st.session_state.selected_site]
            st.info(f"🎯 Showing: **{st.session_state.selected_site}** — Click the site again or 'Clear' to see all sites")
        else:
            st.info(f"📊 Showing: **All Sites** ({len(df)} total) — Click any marker on the map to filter")

        if df.empty:
            st.warning("No data found for the selected filters.")
            return

        # Convert numeric columns to proper types
        numeric_columns = ['total_monthly_revenue', 'revenue_per_sqft', 'days_until_expiration',
                           'insurance_liability_min_usd', 'equipment_space_sqft']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Metrics row with custom styled cards
        total_revenue = df['total_monthly_revenue'].sum()
        num_leases = len(df)
        avg_days_remaining = df['days_until_expiration'].mean()
        avg_revenue_per_sqft = df['revenue_per_sqft'].mean()

        # Handle NaN values safely
        total_revenue = 0 if pd.isna(total_revenue) else total_revenue
        avg_days_remaining = 0 if pd.isna(avg_days_remaining) else avg_days_remaining
        avg_revenue_per_sqft = 0 if pd.isna(avg_revenue_per_sqft) else avg_revenue_per_sqft

        metric1, metric2, metric3, metric4 = st.columns(4)

        with metric1:
            st.markdown(f'''
            <div class="metric-card">
                <div class="metric-label metric-label-green">💰 Monthly Revenue</div>
                <div class="metric-value">${total_revenue:,.0f}</div>
            </div>
            ''', unsafe_allow_html=True)
        with metric2:
            st.markdown(f'''
            <div class="metric-card">
                <div class="metric-label metric-label-blue">📋 Active Leases</div>
                <div class="metric-value">{num_leases}</div>
            </div>
            ''', unsafe_allow_html=True)
        with metric3:
            st.markdown(f'''
            <div class="metric-card">
                <div class="metric-label metric-label-orange">⏱️ Avg Days Remaining</div>
                <div class="metric-value">{avg_days_remaining:,.0f}</div>
            </div>
            ''', unsafe_allow_html=True)
        with metric4:
            # Check if single site is selected
            if st.session_state.selected_site and num_leases == 1:
                # Single site: show actual compliance status from data
                status_text = df['compliance_status'].iloc[0] if 'compliance_status' in df.columns and len(df) > 0 else "Unknown"
                status_text = str(status_text) if pd.notna(status_text) else "Unknown"
                
                # Color based on status text
                status_lower = status_text.lower()
                if 'compliant' in status_lower and 'non' not in status_lower:
                    compliance_color = "metric-label-green"
                elif 'pending' in status_lower:
                    compliance_color = "metric-label-orange"
                else:
                    compliance_color = "metric-label-red"
                
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label {compliance_color}">✅ Compliance Status</div>
                    <div class="metric-value">{status_text}</div>
                </div>
                ''', unsafe_allow_html=True)
            else:
                # Multiple sites: show percentage of compliant sites
                if 'compliance_status' in df.columns:
                    compliant_count = len(df[df['compliance_status'].str.lower().str.contains('compliant', na=False) & ~df['compliance_status'].str.lower().str.contains('non', na=False)])
                    compliance_rate = (compliant_count / num_leases * 100) if num_leases > 0 else 0
                else:
                    compliance_rate = 0
                
                compliance_color = "metric-label-green" if compliance_rate >= 80 else "metric-label-orange" if compliance_rate >= 50 else "metric-label-red"
                st.markdown(f'''
                <div class="metric-card">
                    <div class="metric-label {compliance_color}">✅ Compliance Status</div>
                    <div class="metric-value">{compliance_rate:.0f}%</div>
                </div>
                ''', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Map header with Clear button tightly aligned
        st.markdown("""
        <style>
            .site-header-row {
                display: flex;
                align-items: center;
                gap: 10px;
                margin: 1.5rem 0 0.5rem 0;
            }
            .site-header-row .section-header {
                margin: 0 !important;
            }
            .clear-btn-container button {
                padding: 0.25rem 0.75rem !important;
                font-size: 0.85rem !important;
            }
        </style>
        """, unsafe_allow_html=True)

        col1, col2, col3 = st.columns([0.12, 0.08, 0.80])
        with col1:
            st.markdown('<div class="section-header" style="margin: 0; white-space: nowrap;">📍 Site Locations</div>', unsafe_allow_html=True)
        with col2:
            if st.session_state.selected_site:
                if st.button("✕ Clear", key="clear_site"):
                    st.session_state.selected_site = None
                    st.rerun()
        # col3 is empty spacer

        st.caption("🖱️ Click on any site marker to filter the dashboard to that site")

        # Get unfiltered data for map (to show all sites even when one is selected)
        df_all_sites = query_data(state_filter, doc_type_filter, tenant_filter)

        # Create a copy for display and clean data
        df_display = df_all_sites.copy()
        df_display = df_display.dropna(subset=['total_monthly_revenue', 'latitude', 'longitude'])

        if df_display.empty:
            st.warning("No valid site location data available.")
        else:
            # Convert to numeric for plotting
            df_display['total_monthly_revenue'] = pd.to_numeric(df_display['total_monthly_revenue'], errors='coerce')
            df_display['revenue_per_sqft'] = pd.to_numeric(df_display['revenue_per_sqft'], errors='coerce')
            df_display = df_display.dropna(subset=['total_monthly_revenue'])

            # Reset index to ensure proper mapping with selection
            df_display = df_display.reset_index(drop=True)

            df_display['revenue_display'] = df_display['total_monthly_revenue'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")

            # Highlight selected site if any
            selected_site = st.session_state.get('selected_site', None)
            if selected_site:
                df_display['marker_size'] = df_display['site_name'].apply(
                    lambda x: 20 if x == selected_site else 10
                )
                df_display['marker_color'] = df_display['site_name'].apply(
                    lambda x: '#e63946' if x == selected_site else '#2196F3'
                )
            else:
                df_display['marker_size'] = [12] * len(df_display)
                df_display['marker_color'] = ['#2196F3'] * len(df_display)

            fig = px.scatter_mapbox(
                df_display,
                lat='latitude',
                lon='longitude',
                hover_name='site_name',
                hover_data={
                    'latitude': False,
                    'longitude': False,
                    'state': True,
                    'tenant_name': True,
                    'revenue_display': True,
                    'revenue_per_sqft': ':.2f',
                    'marker_size': False,
                    'marker_color': False
                },
                color_discrete_sequence=['#2196F3'],
                size='marker_size',
                zoom=3,
                height=500,
                labels={'revenue_display': 'Monthly Revenue', 'revenue_per_sqft': 'Revenue/Sq Ft'}
            )

            # Update marker colors for selected site
            if selected_site:
                colors = df_display['marker_color'].tolist()
                fig.update_traces(marker=dict(color=colors))

            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r": 0, "t": 0, "l": 0, "b": 0}
            )

            # Make map clickable with selection
            event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", selection_mode="points", key="map_select")

            # Handle map click selection (toggle behavior)
            if event and event.selection and event.selection.points:
                clicked_point = event.selection.points[0]
                point_index = clicked_point.get('point_index', None)
                if point_index is not None and point_index < len(df_display):
                    clicked_site = df_display.iloc[point_index]['site_name']
                    # Toggle: if clicking same site, deselect it; otherwise select new site
                    if clicked_site == st.session_state.selected_site:
                        st.session_state.selected_site = None  # Deselect
                    else:
                        st.session_state.selected_site = clicked_site  # Select new
                    st.rerun()

        # Data table - using Pandas Styler for visibility
        st.markdown('<div class="section-header">📋 Detailed Data</div>', unsafe_allow_html=True)

        # Prepare data for display
        display_df = df[[
            'site_name', 'state', 'tenant_name', 'document_type',
            'total_monthly_revenue', 'revenue_per_sqft',
            'days_until_expiration', 'insurance_liability_min_usd'
        ]].sort_values('total_monthly_revenue', ascending=False).head(50).copy()

        # Format numeric columns
        display_df['total_monthly_revenue'] = display_df['total_monthly_revenue'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        display_df['revenue_per_sqft'] = display_df['revenue_per_sqft'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        display_df['days_until_expiration'] = display_df['days_until_expiration'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
        display_df['insurance_liability_min_usd'] = display_df['insurance_liability_min_usd'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")

        # Rename columns for display
        display_df.columns = ['Site Name', 'State', 'Tenant', 'Document Type', 'Monthly Revenue', 'Revenue/SqFt', 'Days Remaining', 'Insurance Liability']

        # Use st.table with custom styling (simpler and more reliable)
        st.markdown("""
        <style>
            .stTable {
                background-color: #ffffff !important;
            }
            .stTable table {
                background-color: #ffffff !important;
                color: #1a1a1a !important;
            }
            .stTable th {
                background-color: #f8f9fa !important;
                color: #1a1a1a !important;
            }
            .stTable td {
                background-color: #ffffff !important;
                color: #1a1a1a !important;
            }
        </style>
        """, unsafe_allow_html=True)

        st.table(display_df)

    except Exception as e:
        st.error(f"Error loading dashboard: {str(e)}")
        st.info("Please ensure the SQL Warehouse is running and you have proper access to the data.")

def show_genie_space():
    # Header
    st.markdown("""
    <div style="margin-bottom: 1rem;">
        <h2 style="color: #1a1a1a; margin: 0; font-size: 1.5rem;">🤖 Genie Data Chat</h2>
        <p style="color: #666; margin: 0.25rem 0 0 0; font-size: 0.9rem;">Powered by Databricks Genie Space</p>
    </div>
    """, unsafe_allow_html=True)

    # Get Genie Space ID from environment or config
    genie_space_id = os.environ.get("GENIE_SPACE_ID", "")

    if not genie_space_id:
        st.warning("⚠️ No Genie Space configured for this app.")
        st.info("""
        **To enable Genie Space:**
        
        1. Create or find your Genie Space in the Databricks UI
        2. Get the Space ID from the URL: `/genie/rooms/{SPACE_ID}`
        3. Add it to the app configuration
        
        **Or use the other AI assistants:**
        - 📚 Knowledge Assistant - Fast document Q&A
        - 👥 Multi-Agent Supervisor - Complex tasks
        """)
        return

    # Initialize session state for chat history and polygon
    if 'genie_messages' not in st.session_state:
        st.session_state.genie_messages = []
    if 'drawn_polygon' not in st.session_state:
        st.session_state.drawn_polygon = None
    if 'sites_in_polygon' not in st.session_state:
        st.session_state.sites_in_polygon = 0

    # ============ INTERACTIVE MAP WITH POLYGON DRAWING ============
    st.markdown("""
    <div style="margin-bottom: 0.5rem;">
        <h3 style="color: #1a1a1a; margin: 0; font-size: 1.1rem;">🗺️ Draw a Region to Query</h3>
        <p style="color: #666; margin: 0.25rem 0 0 0; font-size: 0.85rem;">
            Draw a polygon on the map to select sites, then ask questions about sites in that area.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Create two columns - map and info panel
    map_col, info_col = st.columns([3, 1])

    with map_col:
        try:
            # Get site locations for the map
            sites_df = get_site_locations()

            if not sites_df.empty:
                # Calculate map center
                center_lat = sites_df['latitude'].astype(float).mean()
                center_lon = sites_df['longitude'].astype(float).mean()
            else:
                center_lat, center_lon = 39.8283, -98.5795  # Center of US

            # Create folium map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=4,
                tiles='OpenStreetMap'
            )

            # Add Draw plugin for polygon drawing
            draw = Draw(
                export=False,
                position='topleft',
                draw_options={
                    'polyline': False,
                    'rectangle': True,
                    'polygon': True,
                    'circle': False,
                    'marker': False,
                    'circlemarker': False,
                },
                edit_options={
                    'edit': True,
                    'remove': True
                }
            )
            draw.add_to(m)

            # Add site markers to the map
            if not sites_df.empty:
                for _, row in sites_df.iterrows():
                    try:
                        lat = float(row['latitude'])
                        lon = float(row['longitude'])
                        site_name = str(row['site_name'])
                        state = str(row.get('state', 'N/A'))
                        revenue = row.get('total_monthly_revenue', 0)

                        # Check if site is in selected polygon
                        is_in_polygon = False
                        if st.session_state.drawn_polygon:
                            is_in_polygon = check_point_in_polygon(lat, lon, st.session_state.drawn_polygon)

                        # Color based on selection
                        marker_color = '#e63946' if is_in_polygon else '#2196F3'

                        popup_html = f"""
                        <div style="font-family: Arial; width: 200px;">
                            <b>{site_name}</b><br>
                            📍 {state}<br>
                            💰 ${revenue:,.0f}/month
                        </div>
                        """

                        folium.CircleMarker(
                            location=[lat, lon],
                            radius=6 if not is_in_polygon else 8,
                            popup=folium.Popup(popup_html, max_width=250),
                            color=marker_color,
                            fill=True,
                            fillColor=marker_color,
                            fillOpacity=0.7,
                            weight=2
                        ).add_to(m)
                    except (ValueError, TypeError):
                        continue

            # Render the map and capture drawn shapes
            map_data = st_folium(
                m,
                height=400,
                width=None,
                returned_objects=["all_drawings"],
                key="genie_map"
            )

            # Process drawn shapes
            if map_data and map_data.get("all_drawings"):
                drawings = map_data["all_drawings"]
                if drawings:
                    # Get the last drawn shape
                    last_drawing = drawings[-1]
                    if last_drawing.get("geometry", {}).get("type") in ["Polygon", "Rectangle"]:
                        coords = last_drawing["geometry"]["coordinates"][0]
                        # Folium returns [lon, lat], keep as is for shapely
                        st.session_state.drawn_polygon = coords

                        # Count sites in polygon using Databricks ST_Intersects (same as Genie query)
                        count = count_sites_in_polygon_db(coords)
                        if count == -1:
                            # Fallback to local count if DB query failed
                            count = 0
                            if not sites_df.empty:
                                for _, row in sites_df.iterrows():
                                    try:
                                        lat = float(row['latitude'])
                                        lon = float(row['longitude'])
                                        if check_point_in_polygon(lat, lon, coords):
                                            count += 1
                                    except:
                                        continue
                        st.session_state.sites_in_polygon = count
                else:
                    # No drawings - clear polygon if it was set
                    if st.session_state.drawn_polygon and not drawings:
                        st.session_state.drawn_polygon = None
                        st.session_state.sites_in_polygon = 0

        except Exception as e:
            st.warning(f"Could not load map: {str(e)}")
            st.info("You can still chat with Genie without the map.")

    with info_col:
        st.markdown("""
        <div style="background: #f8f9fa; border-radius: 8px; padding: 1rem; height: 100%;">
            <h4 style="color: #1a1a1a; margin: 0 0 0.75rem 0; font-size: 0.95rem;">📌 How to Use</h4>
            <ol style="color: #666; font-size: 0.8rem; padding-left: 1.2rem; margin: 0;">
                <li style="margin-bottom: 0.5rem;">Click the polygon 🔷 or rectangle ⬜ tool on the map</li>
                <li style="margin-bottom: 0.5rem;">Draw a shape around the area you want to query</li>
                <li style="margin-bottom: 0.5rem;">Ask a question about sites in that area</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

        # Show polygon status
        if st.session_state.drawn_polygon:
            st.success(f"✅ **{st.session_state.sites_in_polygon}** sites selected")
            if st.button("🗑️ Clear Selection", key="clear_polygon"):
                st.session_state.drawn_polygon = None
                st.session_state.sites_in_polygon = 0
                st.rerun()
        else:
            st.info("No area selected")

        # Suggested queries when polygon is drawn
        if st.session_state.drawn_polygon:
            st.markdown("""
            <div style="margin-top: 0.75rem;">
                <p style="color: #666; font-size: 0.8rem; margin-bottom: 0.5rem;"><b>Try asking:</b></p>
            </div>
            """, unsafe_allow_html=True)

            example_queries = [
                "How many sites are in this area?",
                "What's the total revenue in this region?",
                "Show me all sites in this polygon",
                "Which tenants are in this area?"
            ]
            for query in example_queries:
                st.markdown(f"<span style='color: #2196F3; font-size: 0.75rem;'>• {query}</span>", unsafe_allow_html=True)

    st.markdown("---")

    # Clear chat history button (only show if there's history)
    if st.session_state.genie_messages:
        if st.button("🗑️ Clear Chat History", key="clear_genie"):
            st.session_state.genie_messages = []
            st.rerun()
        st.markdown("---")

    # Display chat history (only completed exchanges)
    for message in st.session_state.genie_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    placeholder_text = "Ask about sites in the selected area..." if st.session_state.drawn_polygon else "Ask about your lease data..."

    if prompt := st.chat_input(placeholder_text):
        # Display user message (don't add to history yet)
        with st.chat_message("user"):
            st.markdown(prompt)
            if st.session_state.drawn_polygon:
                st.caption(f"🗺️ Query includes {st.session_state.sites_in_polygon} sites from selected polygon")

        # Build the enhanced prompt with polygon context if available
        enhanced_prompt = prompt
        if st.session_state.drawn_polygon:
            polygon_context = format_polygon_for_query(st.session_state.drawn_polygon)
            enhanced_prompt = f"{prompt}\n\n{polygon_context}"

        # Get response from Genie
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    import time

                    # Start a conversation using SDK
                    start_response = w.api_client.do(
                        'POST',
                        f'/api/2.0/genie/spaces/{genie_space_id}/start-conversation',
                        body={"content": enhanced_prompt},
                        headers={'Content-Type': 'application/json'}
                    )

                    conversation_id = start_response.get("conversation", {}).get("id")
                    message_data = start_response.get("message", {})
                    message_id = message_data.get("id")

                    # Poll for the response
                    max_attempts = 30
                    for _ in range(max_attempts):
                        message_status = w.api_client.do(
                            'GET',
                            f'/api/2.0/genie/spaces/{genie_space_id}/conversations/{conversation_id}/messages/{message_id}',
                            headers={'Content-Type': 'application/json'}
                        )

                        status = message_status.get("status")

                        if status == "COMPLETED":
                            # Extract response content
                            attachments = message_status.get("attachments", [])
                            response_parts = []
                            sql_query = None

                            # Get statement_id from message level
                            statement_id_from_message = message_status.get("query_result", {}).get("statement_id")

                            for attachment in attachments:
                                # Get text explanations
                                if "text" in attachment:
                                    text_data = attachment["text"]
                                    if "content" in text_data:
                                        response_parts.append(text_data['content'])

                                # Get query results
                                if "query" in attachment:
                                    query_data = attachment["query"]

                                    # Store SQL query
                                    if "query" in query_data:
                                        sql_query = query_data['query']

                                    # Get statement_id
                                    statement_id = None
                                    if "query_result_metadata" in query_data and "statement_id" in query_data["query_result_metadata"]:
                                        statement_id = query_data["query_result_metadata"]["statement_id"]
                                    elif statement_id_from_message:
                                        statement_id = statement_id_from_message

                                    if statement_id:
                                        try:
                                            # Fetch query results
                                            result_response = w.api_client.do(
                                                'GET',
                                                f'/api/2.0/sql/statements/{statement_id}',
                                                headers={'Content-Type': 'application/json'}
                                            )

                                            stmt_data = result_response.get("statement_response", result_response)
                                            stmt_status = stmt_data.get("status", {}).get("state")

                                            if stmt_status == "SUCCEEDED":
                                                if "result" in stmt_data and "data_array" in stmt_data["result"]:
                                                    data = stmt_data["result"]["data_array"]
                                                    if data:
                                                        # Get column names
                                                        columns = []
                                                        if "manifest" in stmt_data and "schema" in stmt_data["manifest"]:
                                                            columns = [col["name"] for col in stmt_data["manifest"]["schema"]["columns"]]

                                                        # Check if data has lat/long for map visualization
                                                        lat_col = None
                                                        lon_col = None
                                                        name_col = None

                                                        for i, col in enumerate(columns):
                                                            col_lower = col.lower()
                                                            if 'lat' in col_lower:
                                                                lat_col = i
                                                            if 'lon' in col_lower or 'lng' in col_lower:
                                                                lon_col = i
                                                            if 'name' in col_lower or 'site' in col_lower:
                                                                name_col = i

                                                        # If we have lat/long, create a map
                                                        if lat_col is not None and lon_col is not None:
                                                            response_parts.append("\n**📍 Map View:**\n")

                                                            # Build dataframe for map
                                                            map_data = []
                                                            for row in data:
                                                                try:
                                                                    lat = float(row[lat_col]) if row[lat_col] else None
                                                                    lon = float(row[lon_col]) if row[lon_col] else None
                                                                    name = str(row[name_col]) if name_col is not None and row[name_col] else f"Site"
                                                                    if lat and lon:
                                                                        map_data.append({
                                                                            'latitude': lat,
                                                                            'longitude': lon,
                                                                            'name': name,
                                                                            **{columns[j]: row[j] for j in range(len(columns)) if j not in [lat_col, lon_col]}
                                                                        })
                                                                except (ValueError, TypeError):
                                                                    continue

                                                            if map_data:
                                                                map_df = pd.DataFrame(map_data)

                                                                # Create map
                                                                fig = px.scatter_mapbox(
                                                                    map_df,
                                                                    lat='latitude',
                                                                    lon='longitude',
                                                                    hover_name='name',
                                                                    hover_data={col: True for col in map_df.columns if col not in ['latitude', 'longitude', 'name']},
                                                                    zoom=3,
                                                                    height=400
                                                                )
                                                                fig.update_layout(
                                                                    mapbox_style="open-street-map",
                                                                    margin={"r": 0, "t": 0, "l": 0, "b": 0}
                                                                )
                                                                st.plotly_chart(fig, use_container_width=True)

                                                        # Always show data table
                                                        response_parts.append("\n**Query Results:**\n")

                                                        if columns:
                                                            # Create markdown table
                                                            header = "| " + " | ".join(columns) + " |"
                                                            separator = "| " + " | ".join(["---"] * len(columns)) + " |"
                                                            rows = [header, separator]

                                                            for row in data[:20]:
                                                                row_str = "| " + " | ".join([str(val) if val is not None else "" for val in row]) + " |"
                                                                rows.append(row_str)

                                                            response_parts.append("\n".join(rows))

                                                            if len(data) > 20:
                                                                response_parts.append(f"\n*({len(data) - 20} more rows...)*")
                                        except Exception as e:
                                            response_parts.append(f"\n_Could not fetch query results: {str(e)}_")

                            if not response_parts:
                                response_parts.append("Query completed successfully!")

                            answer = "\n".join(response_parts)
                            st.markdown(answer)

                            # Show SQL query in expander if available
                            if sql_query:
                                with st.expander("📝 View SQL Query"):
                                    st.code(sql_query, language='sql')

                            # Add both user and assistant messages to history together
                            st.session_state.genie_messages.append({"role": "user", "content": prompt})
                            st.session_state.genie_messages.append({"role": "assistant", "content": answer})
                            break
                        elif status in ["FAILED", "CANCELLED"]:
                            error = message_status.get("error", {})
                            error_msg = error.get("message", "Query failed")
                            error_details = str(message_status)
                            st.error(f"Genie query failed: {error_msg}")
                            with st.expander("🔍 Error Details"):
                                st.json(message_status)
                            st.info("💡 **Possible solutions:**\n\n1. The app may not have permission to access this Genie Space\n2. Share the Genie Space with: `app-39zraj lease-agreement` or service principal ID `75d6afe4-f60c-46b2-a09f-654e631b0733`\n3. Or use Knowledge Assistant/Multi-Agent Supervisor tabs instead")
                            st.session_state.genie_messages.append({"role": "user", "content": prompt})
                            st.session_state.genie_messages.append({"role": "assistant", "content": f"❌ Error: {error_msg}"})
                            break

                        time.sleep(1)
                    else:
                        timeout_msg = "Query is taking longer than expected. Please try again."
                        st.warning(timeout_msg)
                        st.session_state.genie_messages.append({"role": "user", "content": prompt})
                        st.session_state.genie_messages.append({"role": "assistant", "content": timeout_msg})

                except Exception as e:
                    error_msg = f"Error communicating with Genie Space: {str(e)}"
                    st.error(error_msg)

                    if "does not exist" in str(e) or "permission" in str(e).lower() or "unauthorized" in str(e).lower():
                        st.info("""
                        💡 **This is likely a permissions issue.**
                        
                        **To fix:**
                        1. Go to your Genie Space: https://fe-vm-chi-vdm-serverless.cloud.databricks.com/genie/rooms/01f0c9ca21941df6a2436841e0353279
                        2. Click on "Share" or "Permissions"
                        3. Add the app's service principal:
                           - Name: `app-39zraj lease-agreement`
                           - Or ID: `75d6afe4-f60c-46b2-a09f-654e631b0733`
                        4. Grant "Can Use" permission
                        
                        **Or use the other tabs:**
                        - 📚 Knowledge Assistant (fast document Q&A)
                        - 👥 Multi-Agent Supervisor (complex tasks)
                        """)

                    st.session_state.genie_messages.append({"role": "user", "content": prompt})
                    st.session_state.genie_messages.append({"role": "assistant", "content": error_msg})

def show_knowledge_assistant():
    # Header
    st.markdown("""
    <div style="margin-bottom: 1rem;">
        <h2 style="color: #1a1a1a; margin: 0; font-size: 1.5rem;">📚 Knowledge Assistant</h2>
        <p style="color: #666; margin: 0.25rem 0 0 0; font-size: 0.9rem;">Powered by Document RAG</p>
    </div>
    """, unsafe_allow_html=True)

    # Initialize session state for chat history
    if 'ka_messages' not in st.session_state:
        st.session_state.ka_messages = []

    # Clear chat history button (only show if there's history)
    if st.session_state.ka_messages:
        if st.button("🗑️ Clear Chat History", key="clear_ka"):
            st.session_state.ka_messages = []
            st.rerun()
        st.markdown("---")

    # Display chat history (only completed exchanges)
    for message in st.session_state.ka_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask about lease agreements, contracts, or policies..."):
        # Display user message (don't add to history yet)
        with st.chat_message("user"):
            st.markdown(prompt)

        # Get response from Knowledge Assistant
        with st.chat_message("assistant"):
            with st.spinner("Searching knowledge base..."):
                try:
                    endpoint_name = "ka-870aa40a-endpoint"

                    # Build input messages including history and current prompt
                    input_messages = [{"role": msg["role"], "content": msg["content"]} for msg in st.session_state.ka_messages]
                    input_messages.append({"role": "user", "content": prompt})

                    # Use SDK's api_client.do() which handles auth automatically
                    response = w.api_client.do(
                        'POST',
                        f'/serving-endpoints/{endpoint_name}/invocations',
                        body={
                            "input": input_messages,
                            "databricks_options": {"return_trace": True}
                        },
                        headers={'Content-Type': 'application/json'}
                    )

                    # Parse agent response from output array
                    assistant_message = ""
                    output = response.get("output", [])

                    # Extract message content from agent response
                    for item in output:
                        if item.get("type") == "message" and item.get("role") == "assistant":
                            content = item.get("content", [])
                            for content_item in content:
                                if content_item.get("type") == "output_text":
                                    assistant_message += content_item.get("text", "")

                    # Fallback: try other response formats
                    if not assistant_message and isinstance(output, str):
                        assistant_message = output
                    elif not assistant_message and isinstance(output, list):
                        for item in output:
                            if "text" in item:
                                assistant_message += str(item.get("text", ""))

                    if not assistant_message:
                        assistant_message = "I apologize, but I couldn't generate a response. Please try again."

                    st.markdown(assistant_message)

                    # Add both user and assistant messages to history together
                    st.session_state.ka_messages.append({"role": "user", "content": prompt})
                    st.session_state.ka_messages.append({"role": "assistant", "content": assistant_message})

                except Exception as e:
                    error_msg = f"Error: {str(e)}"
                    st.error(error_msg)
                    st.session_state.ka_messages.append({"role": "user", "content": prompt})
                    st.session_state.ka_messages.append({"role": "assistant", "content": error_msg})

def show_multi_agent_supervisor():
    # Header
    st.markdown("""
    <div style="margin-bottom: 1rem;">
        <h2 style="color: #1a1a1a; margin: 0; font-size: 1.5rem;">💬 AI Assistant</h2>
        <p style="color: #666; margin: 0.25rem 0 0 0; font-size: 0.9rem;">Powered by Multi-Agent Supervisor</p>
    </div>
    """, unsafe_allow_html=True)

    # Initialize session state for chat history and polygon
    if 'mas_messages' not in st.session_state:
        st.session_state.mas_messages = []
    if 'mas_drawn_polygon' not in st.session_state:
        st.session_state.mas_drawn_polygon = None
    if 'mas_sites_in_polygon' not in st.session_state:
        st.session_state.mas_sites_in_polygon = 0

    # ============ INTERACTIVE MAP WITH POLYGON DRAWING ============
    st.markdown("""
    <div style="margin-bottom: 0.5rem;">
        <h3 style="color: #1a1a1a; margin: 0; font-size: 1.1rem;">🗺️ Draw a Region to Query</h3>
        <p style="color: #666; margin: 0.25rem 0 0 0; font-size: 0.85rem;">
            Draw a polygon on the map to select sites, then ask questions about sites in that area.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Create two columns - map and info panel
    map_col, info_col = st.columns([3, 1])

    with map_col:
        try:
            # Get site locations for the map
            sites_df = get_site_locations()

            if not sites_df.empty:
                # Calculate map center
                center_lat = sites_df['latitude'].astype(float).mean()
                center_lon = sites_df['longitude'].astype(float).mean()
            else:
                center_lat, center_lon = 39.8283, -98.5795  # Center of US

            # Create folium map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=4,
                tiles='OpenStreetMap'
            )

            # Add Draw plugin for polygon drawing
            draw = Draw(
                export=False,
                position='topleft',
                draw_options={
                    'polyline': False,
                    'rectangle': True,
                    'polygon': True,
                    'circle': False,
                    'marker': False,
                    'circlemarker': False,
                },
                edit_options={
                    'edit': True,
                    'remove': True
                }
            )
            draw.add_to(m)

            # Add site markers to the map
            if not sites_df.empty:
                for _, row in sites_df.iterrows():
                    try:
                        lat = float(row['latitude'])
                        lon = float(row['longitude'])
                        site_name = str(row['site_name'])
                        state = str(row.get('state', 'N/A'))
                        revenue = row.get('total_monthly_revenue', 0)

                        # Check if site is in selected polygon
                        is_in_polygon = False
                        if st.session_state.mas_drawn_polygon:
                            is_in_polygon = check_point_in_polygon(lat, lon, st.session_state.mas_drawn_polygon)

                        # Color based on selection
                        marker_color = '#e63946' if is_in_polygon else '#2196F3'

                        popup_html = f"""
                        <div style="font-family: Arial; width: 200px;">
                            <b>{site_name}</b><br>
                            📍 {state}<br>
                            💰 ${revenue:,.0f}/month
                        </div>
                        """

                        folium.CircleMarker(
                            location=[lat, lon],
                            radius=6 if not is_in_polygon else 8,
                            popup=folium.Popup(popup_html, max_width=250),
                            color=marker_color,
                            fill=True,
                            fillColor=marker_color,
                            fillOpacity=0.7,
                            weight=2
                        ).add_to(m)
                    except (ValueError, TypeError):
                        continue

            # Render the map and capture drawn shapes
            map_data = st_folium(
                m,
                height=400,
                width=None,
                returned_objects=["all_drawings"],
                key="mas_map"
            )

            # Process drawn shapes
            if map_data and map_data.get("all_drawings"):
                drawings = map_data["all_drawings"]
                if drawings:
                    # Get the last drawn shape
                    last_drawing = drawings[-1]
                    if last_drawing.get("geometry", {}).get("type") in ["Polygon", "Rectangle"]:
                        coords = last_drawing["geometry"]["coordinates"][0]
                        # Folium returns [lon, lat], keep as is for shapely
                        st.session_state.mas_drawn_polygon = coords

                        # Count sites in polygon using Databricks ST_Intersects (same as query)
                        count = count_sites_in_polygon_db(coords)
                        if count == -1:
                            # Fallback to local count if DB query failed
                            count = 0
                            if not sites_df.empty:
                                for _, row in sites_df.iterrows():
                                    try:
                                        lat = float(row['latitude'])
                                        lon = float(row['longitude'])
                                        if check_point_in_polygon(lat, lon, coords):
                                            count += 1
                                    except:
                                        continue
                        st.session_state.mas_sites_in_polygon = count
                else:
                    # No drawings - clear polygon if it was set
                    if st.session_state.mas_drawn_polygon and not drawings:
                        st.session_state.mas_drawn_polygon = None
                        st.session_state.mas_sites_in_polygon = 0

        except Exception as e:
            st.warning(f"Could not load map: {str(e)}")
            st.info("You can still chat with the Multi-Agent Supervisor without the map.")

    with info_col:
        st.markdown("""
        <div style="background: #f8f9fa; border-radius: 8px; padding: 1rem; height: 100%;">
            <h4 style="color: #1a1a1a; margin: 0 0 0.75rem 0; font-size: 0.95rem;">📌 How to Use</h4>
            <ol style="color: #666; font-size: 0.8rem; padding-left: 1.2rem; margin: 0;">
                <li style="margin-bottom: 0.5rem;">Click the polygon 🔷 or rectangle ⬜ tool on the map</li>
                <li style="margin-bottom: 0.5rem;">Draw a shape around the area you want to query</li>
                <li style="margin-bottom: 0.5rem;">Ask a question about sites in that area</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

        # Show polygon status
        if st.session_state.mas_drawn_polygon:
            st.success(f"✅ **{st.session_state.mas_sites_in_polygon}** sites selected")
            if st.button("🗑️ Clear Selection", key="clear_mas_polygon"):
                st.session_state.mas_drawn_polygon = None
                st.session_state.mas_sites_in_polygon = 0
                st.rerun()
        else:
            st.info("No area selected")

    st.markdown("---")

    # Clear chat history button (only show if there's history)
    if st.session_state.mas_messages:
        if st.button("🗑️ Clear Chat History", key="clear_mas"):
            st.session_state.mas_messages = []
            st.rerun()
        st.markdown("---")

    # Display chat history (only completed exchanges)
    for message in st.session_state.mas_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    placeholder_text = "Ask about sites in the selected area..." if st.session_state.mas_drawn_polygon else "Ask about alerts, events, or system health..."

    if prompt := st.chat_input(placeholder_text):
        # Display user message (don't add to history yet)
        with st.chat_message("user"):
            st.markdown(prompt)
            if st.session_state.mas_drawn_polygon:
                st.caption(f"🗺️ Query includes {st.session_state.mas_sites_in_polygon} sites from selected polygon")

        # Build the enhanced prompt with polygon context if available
        enhanced_prompt = prompt
        if st.session_state.mas_drawn_polygon:
            polygon_context = format_polygon_for_query(st.session_state.mas_drawn_polygon)
            enhanced_prompt = f"{prompt}\n\n{polygon_context}"

        # Get response from Multi-Agent Supervisor
        with st.chat_message("assistant"):
            # Show progress message for long-running queries
            status_placeholder = st.empty()
            status_placeholder.info("🔄 Processing your request... Complex queries may take 2-3 minutes.")

            try:
                import time
                import requests as req

                endpoint_name = "mas-5b54bbfa-endpoint"

                # Build input messages including history and current prompt
                input_messages = [{"role": msg["role"], "content": msg["content"]} for msg in st.session_state.mas_messages]
                input_messages.append({"role": "user", "content": enhanced_prompt})

                # Construct request payload for agent endpoint
                payload = {
                    "input": input_messages,
                    "databricks_options": {"return_trace": True}
                }

                # Use requests with extended timeout (10 minutes)
                # Get auth from SDK config
                host = cfg.host.replace("https://", "").replace("http://", "")
                url = f"https://{host}/serving-endpoints/{endpoint_name}/invocations"
                headers = {
                    'Content-Type': 'application/json'
                }

                # Add auth header from SDK
                auth_headers = dict(cfg.authenticate())
                headers.update(auth_headers)

                # Make request with 10 minute timeout
                resp = req.post(url, json=payload, headers=headers, timeout=600)
                resp.raise_for_status()
                response_data = resp.json()

                status_placeholder.empty()

                # Parse agent response from output array (exact pattern from working example)
                assistant_message = ""
                output = response_data.get("output", [])

                # Try primary parsing method
                for item in output:
                    if item.get("type") == "message" and item.get("role") == "assistant":
                        content = item.get("content", [])
                        for content_item in content:
                            if content_item.get("type") == "output_text":
                                assistant_message += content_item.get("text", "")

                # Fallback 1: Check if output is a string directly
                if not assistant_message and isinstance(output, str):
                    assistant_message = output

                # Fallback 2: Check for text field at top level
                if not assistant_message and isinstance(output, list):
                    for item in output:
                        if "text" in item:
                            assistant_message += str(item.get("text", ""))

                # Fallback 3: Try to get any content from response
                if not assistant_message and "content" in response_data:
                    assistant_message = str(response_data.get("content", ""))

                # Last resort fallback message
                if not assistant_message:
                    assistant_message = "I apologize, but I couldn't generate a response. Please try again."
                    # Add debug info in expander
                    with st.expander("🔍 Debug Info"):
                        st.json(response_data)

                # Display assistant response
                st.markdown(assistant_message)

                # Add both user and assistant messages to history together
                st.session_state.mas_messages.append({"role": "user", "content": prompt})
                st.session_state.mas_messages.append({"role": "assistant", "content": assistant_message})

            except Exception as e:
                status_placeholder.empty()
                error_msg = f"Error calling Multi-Agent Supervisor: {str(e)}"
                st.error(error_msg)
                st.info("Make sure the 'mas-5b54bbfa-endpoint' is available and the app has permissions.")
                st.session_state.mas_messages.append({"role": "user", "content": prompt})
                st.session_state.mas_messages.append({"role": "assistant", "content": error_msg})

if __name__ == "__main__":
    main()

