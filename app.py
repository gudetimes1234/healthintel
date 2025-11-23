import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from database import CDCFluData, get_db_session
from sqlalchemy import func, and_, desc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Public Health Intelligence Platform",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data(ttl=3600)
def load_data():
    """Load CDC flu data from PostgreSQL database."""
    try:
        with get_db_session() as session:
            records = session.query(CDCFluData).all()
            
            data = [{
                'week_ending': r.week_ending,
                'season': r.season,
                'region': r.region,
                'percent_positive': r.percent_positive,
                'total_specimens': r.total_specimens,
                'timestamp': r.timestamp
            } for r in records]
            
            return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_regions():
    """Get unique regions from database."""
    try:
        with get_db_session() as session:
            regions = session.query(CDCFluData.region).distinct().all()
            return sorted([r[0] for r in regions])
    except Exception as e:
        logger.error(f"Error getting regions: {str(e)}")
        return []

def calculate_week_over_week_change(df, region=None):
    """Calculate week-over-week change in percent positive."""
    if df.empty:
        return 0.0
    
    df_sorted = df.sort_values('week_ending', ascending=False)
    
    if region:
        df_sorted = df_sorted[df_sorted['region'] == region]
    
    if len(df_sorted) < 2:
        return 0.0
    
    latest = df_sorted.iloc[0]['percent_positive']
    previous = df_sorted.iloc[1]['percent_positive']
    
    if previous == 0:
        return 0.0
    
    change = ((latest - previous) / previous) * 100
    return change

st.title("🏥 Public Health Intelligence Platform")
st.markdown("### Real-time CDC Influenza Surveillance Dashboard")

df = load_data()

if df.empty:
    st.warning("⚠️ No data available. Please run the ETL pipeline to fetch CDC flu data.")
    st.info("Run the following command to fetch data: `python run_etl.py`")
    st.stop()

st.sidebar.header("📊 Filters")

regions = get_regions()
if 'All Regions' not in regions:
    regions.insert(0, 'All Regions')

selected_region = st.sidebar.selectbox(
    "Select Region",
    options=regions,
    index=0
)

df['week_ending'] = pd.to_datetime(df['week_ending'])

min_date = df['week_ending'].min().date()
max_date = df['week_ending'].max().date()

date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

metric_options = ['Percent Positive', 'Total Specimens']
selected_metric = st.sidebar.selectbox(
    "Select Metric",
    options=metric_options,
    index=0
)

if len(date_range) == 2:
    start_date, end_date = date_range
    df_filtered = df[
        (df['week_ending'].dt.date >= start_date) & 
        (df['week_ending'].dt.date <= end_date)
    ]
else:
    df_filtered = df

if selected_region != 'All Regions':
    df_filtered = df_filtered[df_filtered['region'] == selected_region]

col1, col2, col3, col4 = st.columns(4)

with col1:
    if not df_filtered.empty:
        latest_positive = df_filtered.sort_values('week_ending', ascending=False).iloc[0]['percent_positive']
        st.metric(
            label="Latest % Positive",
            value=f"{latest_positive:.2f}%"
        )
    else:
        st.metric(label="Latest % Positive", value="N/A")

with col2:
    if not df_filtered.empty:
        latest_specimens = df_filtered.sort_values('week_ending', ascending=False).iloc[0]['total_specimens']
        st.metric(
            label="Total Specimens",
            value=f"{latest_specimens:,}"
        )
    else:
        st.metric(label="Total Specimens", value="N/A")

with col3:
    wow_change = calculate_week_over_week_change(
        df_filtered, 
        region=None if selected_region == 'All Regions' else selected_region
    )
    st.metric(
        label="Week-over-Week Change",
        value=f"{wow_change:+.2f}%",
        delta=f"{wow_change:.2f}%"
    )

with col4:
    if not df_filtered.empty:
        total_records = len(df_filtered)
        st.metric(
            label="Total Records",
            value=f"{total_records:,}"
        )
    else:
        st.metric(label="Total Records", value="0")

st.markdown("---")

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 Flu Trends Over Time")
    
    if not df_filtered.empty:
        metric_column = 'percent_positive' if selected_metric == 'Percent Positive' else 'total_specimens'
        
        if selected_region == 'All Regions':
            df_grouped = df_filtered.groupby(['week_ending', 'region'])[metric_column].mean().reset_index()
            
            fig = px.line(
                df_grouped,
                x='week_ending',
                y=metric_column,
                color='region',
                title=f'{selected_metric} by Region',
                labels={
                    'week_ending': 'Week Ending',
                    metric_column: selected_metric,
                    'region': 'Region'
                }
            )
        else:
            df_region = df_filtered[df_filtered['region'] == selected_region]
            
            fig = px.line(
                df_region,
                x='week_ending',
                y=metric_column,
                title=f'{selected_metric} for {selected_region}',
                labels={
                    'week_ending': 'Week Ending',
                    metric_column: selected_metric
                }
            )
        
        fig.update_layout(
            height=400,
            hovermode='x unified',
            xaxis_title="Week Ending",
            yaxis_title=selected_metric
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for selected filters")

with col_right:
    st.subheader("🗺️ Geographic Breakdown")
    
    if not df_filtered.empty:
        latest_week = df_filtered['week_ending'].max()
        df_latest = df_filtered[df_filtered['week_ending'] == latest_week]
        
        region_summary = df_latest.groupby('region').agg({
            'percent_positive': 'mean',
            'total_specimens': 'sum'
        }).reset_index()
        
        region_summary = region_summary.sort_values('percent_positive', ascending=False)
        
        fig_bar = px.bar(
            region_summary,
            x='percent_positive',
            y='region',
            orientation='h',
            title='% Positive by Region (Latest Week)',
            labels={
                'percent_positive': '% Positive',
                'region': 'Region'
            },
            color='percent_positive',
            color_continuous_scale='Reds'
        )
        
        fig_bar.update_layout(
            height=400,
            showlegend=False,
            xaxis_title="Percent Positive",
            yaxis_title="Region"
        )
        
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No data available for selected filters")

st.markdown("---")

st.subheader("📋 Recent Data Records")

if not df_filtered.empty:
    df_display = df_filtered.sort_values('week_ending', ascending=False).head(100)
    
    df_display['week_ending'] = df_display['week_ending'].dt.strftime('%Y-%m-%d')
    
    df_display_formatted = df_display[['week_ending', 'season', 'region', 'percent_positive', 'total_specimens']].copy()
    df_display_formatted.columns = ['Week Ending', 'Season', 'Region', '% Positive', 'Total Specimens']
    
    df_display_formatted['% Positive'] = df_display_formatted['% Positive'].round(2)
    
    st.dataframe(
        df_display_formatted,
        use_container_width=True,
        height=400
    )
    
    csv = df_display_formatted.to_csv(index=False)
    st.download_button(
        label="📥 Download Data as CSV",
        data=csv,
        file_name=f"cdc_flu_data_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )
else:
    st.info("No data available for selected filters")

st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ About")
st.sidebar.info(
    "This platform provides real-time influenza surveillance data from the CDC. "
    "Data is automatically updated every 6 hours via the Prefect ETL pipeline."
)

st.sidebar.markdown("### 📊 Data Source")
st.sidebar.markdown(
    "**CDC FluView (Delphi Epidata)**  \n"
    "Updated: Every 6 hours  \n"
    "Coverage: National + 10 HHS Regions"
)

if not df.empty:
    last_updated = df['timestamp'].max()
    st.sidebar.markdown(f"**Last Updated:** {last_updated.strftime('%Y-%m-%d %H:%M UTC')}")
