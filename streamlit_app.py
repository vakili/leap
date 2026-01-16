import streamlit as st
import pandas as pd
import folium
from folium import plugins
from streamlit_folium import st_folium
import snowflake.connector
import json
from branca.colormap import LinearColormap

# Page configuration
st.set_page_config(
    page_title="SF Gym Accessibility Analysis",
    page_icon="🏋️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("🏋️ San Francisco Gym Accessibility Analysis")
st.markdown("""
This interactive dashboard identifies underserved areas for new gym locations in San Francisco
by analyzing gym density, demographics, and accessibility metrics.
""")

@st.cache_resource
def get_snowflake_connection():
    """Create Snowflake connection using connection parameters from snow CLI"""
    import toml
    import os

    # Read snow CLI config
    config_path = os.path.expanduser("~/.snowflake/config.toml")
    config = toml.load(config_path)

    # Get default connection name
    default_conn_name = config.get('default_connection_name', 'my_connection')

    # Get connection details
    conn_details = config['connections'][default_conn_name]

    conn = snowflake.connector.connect(
        account=conn_details['account'],
        user=conn_details['user'],
        password=conn_details.get('password'),
        role=conn_details.get('role'),
        database='LEAP_ANALYTICS',
        schema='DEV_MARTS',
        warehouse=conn_details.get('warehouse', 'PC_DBT_WH')
    )
    return conn

@st.cache_data(ttl=3600)
def load_gym_data():
    """Load gym accessibility data from Snowflake"""
    conn = get_snowflake_connection()

    query = """
    SELECT
        census_block_group,
        state,
        county,
        ST_ASGEOJSON(geography) as geometry,
        geometry_json,
        total_population,
        pop_age_18_54,
        pct_prime_gym_age,
        median_household_income,
        employed_population,
        demand_score,
        is_high_demand_area,
        gyms_within_1_mile,
        gyms_within_half_mile,
        distance_to_nearest_gym_meters,
        distance_to_nearest_gym_miles,
        accessibility_rating,
        is_underserved,
        opportunity_score,
        opportunity_tier
    FROM mart_gym_accessibility
    ORDER BY opportunity_score DESC
    """

    df = pd.read_sql(query, conn)
    # Don't close connection - it's cached and will be reused

    # Snowflake returns columns in uppercase - convert to lowercase for consistency
    df.columns = df.columns.str.lower()

    return df

@st.cache_data(ttl=3600)
def load_gym_locations():
    """Load individual gym locations"""
    conn = get_snowflake_connection()

    query = """
    SELECT
        place_id,
        display_name,
        gym_type,
        ST_X(geography) as longitude,
        ST_Y(geography) as latitude
    FROM LEAP_ANALYTICS.DEV_INTERMEDIATE.INT_SF_GYMS
    """

    df = pd.read_sql(query, conn)
    # Don't close connection - it's cached and will be reused

    # Snowflake returns columns in uppercase - convert to lowercase for consistency
    df.columns = df.columns.str.lower()

    return df

def create_choropleth_map(df, metric='opportunity_score', show_gyms=True):
    """Create a Folium choropleth map"""

    # San Francisco center
    sf_center = [37.7749, -122.4194]

    # Create base map
    m = folium.Map(
        location=sf_center,
        zoom_start=12,
        tiles='CartoDB positron'
    )

    # Define color scales for different metrics
    # For opportunity score, use percentile-based scaling to avoid outliers dominating the color scale
    if metric == 'opportunity_score':
        # Use 5th and 95th percentile for better color distribution
        vmin_score = df[metric].quantile(0.05)
        vmax_score = df[metric].quantile(0.95)
    else:
        vmin_score = df[metric].min()
        vmax_score = df[metric].max()

    color_scales = {
        'opportunity_score': {
            'colormap': LinearColormap(['red', 'orange', 'yellow', 'lightgreen', 'green'],
                                      vmin=vmin_score,
                                      vmax=vmax_score),
            'caption': 'Opportunity Score (Green = High Opportunity)'
        },
        'gyms_within_half_mile': {
            'colormap': LinearColormap(['red', 'orange', 'yellow', 'green'],
                                      vmin=0,
                                      vmax=df[metric].max()),
            'caption': 'Gyms within 0.5 miles (Green = More Gyms)'
        },
        'distance_to_nearest_gym_miles': {
            'colormap': LinearColormap(['green', 'yellow', 'orange', 'red'],
                                      vmin=0,
                                      vmax=df[metric].max()),
            'caption': 'Distance to Nearest Gym (Red = Farther)'
        }
    }

    colormap = color_scales.get(metric, color_scales['opportunity_score'])['colormap']
    caption = color_scales.get(metric, color_scales['opportunity_score'])['caption']

    # Add census blocks as polygons
    for idx, row in df.iterrows():
        # Parse GeoJSON geometry
        geom = json.loads(row['geometry'])

        # Get coordinates (handle both Polygon and MultiPolygon)
        if geom['type'] == 'Polygon':
            coords = geom['coordinates'][0]
        else:  # MultiPolygon
            coords = geom['coordinates'][0][0]

        # Convert to lat/lon format (swap order)
        coords = [[lat, lon] for lon, lat in coords]

        # Determine color based on metric value
        color = colormap(row[metric])

        # Create popup content
        popup_html = f"""
        <div style="font-family: Arial; width: 250px;">
            <h4>Census Block: {row['census_block_group']}</h4>
            <hr>
            <b>Opportunity Tier:</b> {row['opportunity_tier']}<br>
            <b>Population:</b> {row['total_population']:,.0f}<br>
            <b>Gyms (0.5mi):</b> {row['gyms_within_half_mile']}<br>
            <b>Gyms (1mi):</b> {row['gyms_within_1_mile']}<br>
            <b>Nearest Gym:</b> {row['distance_to_nearest_gym_miles']:.2f} miles<br>
            <b>Accessibility:</b> {row['accessibility_rating']}<br>
            <hr>
            <b>Median Income:</b> ${row['median_household_income']:,.0f}<br>
            <b>Working Age Pop:</b> {row['pop_age_18_54']:,.0f}<br>
            <b>Opportunity Score:</b> {row['opportunity_score']:,.0f}
        </div>
        """

        # Add polygon to map
        folium.Polygon(
            locations=coords,
            color='gray',
            weight=1,
            fill=True,
            fill_color=color,
            fill_opacity=0.6,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"CBG: {row['census_block_group']} | {row['opportunity_tier']}"
        ).add_to(m)

    # Add gym locations if requested
    if show_gyms:
        gym_df = load_gym_locations()

        # Create a feature group for gyms
        gym_group = folium.FeatureGroup(name='Gym Locations')

        for idx, gym in gym_df.iterrows():
            folium.CircleMarker(
                location=[gym['latitude'], gym['longitude']],
                radius=4,
                color='blue',
                fill=True,
                fill_color='blue',
                fill_opacity=0.7,
                popup=f"<b>{gym['display_name']}</b><br>Type: {gym['gym_type']}",
                tooltip=gym['display_name']
            ).add_to(gym_group)

        gym_group.add_to(m)

    # Add layer control
    folium.LayerControl().add_to(m)

    # Add colormap legend
    colormap.caption = caption
    colormap.add_to(m)

    return m

# Load data
with st.spinner("Loading data from Snowflake..."):
    df = load_gym_data()

# Sidebar filters
st.sidebar.header("Filters")

# Option to exclude problematic water-overlapping blocks
exclude_water_blocks = st.sidebar.checkbox(
    "Exclude water-overlapping areas",
    value=True,
    help="Removes 2 census blocks that extend far into the bay/ocean"
)

# List of census blocks that extend into water (identified by abnormally large area)
WATER_OVERLAPPING_BLOCKS = ['060750179021', '060750601001']

opportunity_tiers = st.sidebar.multiselect(
    "Opportunity Tier",
    options=df['opportunity_tier'].unique(),
    default=df['opportunity_tier'].unique().tolist()  # Show all tiers by default
)

min_population = st.sidebar.slider(
    "Minimum Population",
    min_value=0,
    max_value=int(df['total_population'].max()),
    value=0,
    step=100
)

max_distance = st.sidebar.slider(
    "Max Distance to Nearest Gym (miles)",
    min_value=0.0,
    max_value=float(df['distance_to_nearest_gym_miles'].max()),
    value=float(df['distance_to_nearest_gym_miles'].max()),
    step=0.1
)

# Metric selection
metric_options = {
    'Opportunity Score': 'opportunity_score',
    'Gyms within 0.5 miles': 'gyms_within_half_mile',
    'Distance to Nearest Gym': 'distance_to_nearest_gym_miles'
}

selected_metric_display = st.sidebar.selectbox(
    "Map Color Metric",
    options=list(metric_options.keys()),
    index=0
)

selected_metric = metric_options[selected_metric_display]

show_gyms = st.sidebar.checkbox("Show Gym Locations", value=True)

# Apply filters
filter_conditions = (
    (df['opportunity_tier'].isin(opportunity_tiers)) &
    (df['total_population'] >= min_population) &
    (df['distance_to_nearest_gym_miles'] <= max_distance)
)

# Optionally exclude water-overlapping blocks
if exclude_water_blocks:
    filter_conditions = filter_conditions & (~df['census_block_group'].isin(WATER_OVERLAPPING_BLOCKS))

filtered_df = df[filter_conditions]

# Summary metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Census Blocks",
        f"{len(filtered_df):,}",
        delta=f"{len(filtered_df) - len(df):,} filtered"
    )

with col2:
    st.metric(
        "Total Population",
        f"{filtered_df['total_population'].sum():,.0f}"
    )

with col3:
    st.metric(
        "Avg Gyms (0.5mi)",
        f"{filtered_df['gyms_within_half_mile'].mean():.1f}"
    )

with col4:
    st.metric(
        "Underserved Blocks",
        f"{filtered_df['is_underserved'].sum():,}"
    )

# Create tabs
tab1, tab2, tab3 = st.tabs(["🗺️ Map", "📊 Data Table", "📈 Analytics"])

with tab1:
    st.subheader("Gym Accessibility Choropleth Map")

    if len(filtered_df) > 0:
        # Create and display map
        m = create_choropleth_map(filtered_df, metric=selected_metric, show_gyms=show_gyms)
        st_folium(m, width=1400, height=600, key="gym_map", returned_objects=[])
    else:
        st.warning("No data matches the current filters. Please adjust your selections.")

with tab2:
    st.subheader("Filtered Data")

    # Display data table
    display_df = filtered_df[[
        'census_block_group', 'opportunity_tier', 'total_population',
        'gyms_within_half_mile', 'gyms_within_1_mile',
        'distance_to_nearest_gym_miles', 'accessibility_rating',
        'median_household_income', 'opportunity_score'
    ]].copy()

    display_df.columns = [
        'Census Block', 'Opportunity', 'Population',
        'Gyms (0.5mi)', 'Gyms (1mi)', 'Distance (mi)',
        'Accessibility', 'Med Income', 'Opportunity Score'
    ]

    st.dataframe(
        display_df,
        use_container_width=True,
        height=500
    )

    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="sf_gym_opportunities.csv",
        mime="text/csv"
    )

with tab3:
    st.subheader("Opportunity Analysis")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Opportunity Tier Distribution")
        tier_summary = filtered_df.groupby('opportunity_tier').agg({
            'census_block_group': 'count',
            'total_population': 'sum',
            'opportunity_score': 'mean'
        }).round(0)
        tier_summary.columns = ['Census Blocks', 'Population', 'Avg Score']
        st.dataframe(tier_summary, use_container_width=True)

    with col2:
        st.markdown("#### Accessibility Rating Distribution")
        access_summary = filtered_df.groupby('accessibility_rating').agg({
            'census_block_group': 'count',
            'total_population': 'sum'
        }).round(0)
        access_summary.columns = ['Census Blocks', 'Population']
        st.dataframe(access_summary, use_container_width=True)

    st.markdown("#### Top 10 Opportunities")
    top_10 = filtered_df.nlargest(10, 'opportunity_score')[[
        'census_block_group', 'total_population', 'median_household_income',
        'gyms_within_half_mile', 'distance_to_nearest_gym_miles',
        'opportunity_tier', 'opportunity_score'
    ]]
    st.dataframe(top_10, use_container_width=True)

# Footer
st.markdown("---")
st.markdown("""
**Data Sources:**
- Overture Maps Foundation (Places & Divisions)
- SafeGraph Open Census Data (2019 ACS 5-year estimates)

**Built with:** dbt, Snowflake, Streamlit, Folium
""")
