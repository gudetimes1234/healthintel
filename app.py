import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from database import CDCFluData, CovidData, get_db_session
from sqlalchemy import func, and_, desc
import logging
from io import BytesIO

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
def load_covid_data():
    """Load COVID-19 hospitalization data from PostgreSQL database."""
    try:
        with get_db_session() as session:
            records = session.query(CovidData).all()
            
            data = [{
                'date': r.date,
                'geo_type': r.geo_type,
                'geo_value': r.geo_value,
                'confirmed_7day_avg': r.confirmed_7day_avg,
                'timestamp': r.timestamp
            } for r in records]
            
            return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error loading COVID data: {str(e)}")
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

@st.cache_data(ttl=3600)
def get_covid_locations():
    """Get unique locations (states) from COVID database."""
    try:
        with get_db_session() as session:
            locations = session.query(CovidData.geo_value).filter(
                CovidData.geo_type == 'state'
            ).distinct().all()
            return sorted([l[0].upper() for l in locations])
    except Exception as e:
        logger.error(f"Error getting COVID locations: {str(e)}")
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

def detect_anomalies(df, metric='percent_positive', threshold_std=2.0):
    """Detect anomalies in data using statistical thresholds."""
    if df.empty:
        return []
    
    # Calculate mean and std for the metric
    mean_val = df[metric].mean()
    std_val = df[metric].std()
    
    # Find outliers beyond threshold standard deviations
    df_sorted = df.sort_values('week_ending', ascending=False)
    anomalies = []
    
    for idx, row in df_sorted.head(10).iterrows():
        z_score = abs((row[metric] - mean_val) / std_val) if std_val > 0 else 0
        if z_score > threshold_std:
            anomalies.append({
                'date': row['week_ending'],
                'region': row['region'] if 'region' in row else 'N/A',
                'value': row[metric],
                'z_score': z_score,
                'deviation': 'High' if row[metric] > mean_val else 'Low'
            })
    
    return anomalies

def check_data_freshness(df):
    """Check if data is recent and up-to-date."""
    if df.empty:
        return None, None
    
    latest_date = df['week_ending'].max()
    days_old = (datetime.now() - latest_date).days
    
    status = "🟢 Current"
    if days_old > 14:
        status = "🟡 Slightly Outdated"
    if days_old > 30:
        status = "🔴 Outdated"
    
    return latest_date, status

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

# Historical Comparison Toggle
st.sidebar.markdown("---")
st.sidebar.subheader("📊 Historical Comparison")
enable_comparison = st.sidebar.checkbox(
    "Enable Year-over-Year Comparison",
    value=False,
    help="Compare current season with previous season(s)"
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
        
        if enable_comparison:
            # Historical comparison mode - add week number for alignment
            df_filtered_comp = df_filtered.copy()
            df_filtered_comp['week_number'] = df_filtered_comp['week_ending'].dt.isocalendar().week
            df_filtered_comp['year'] = df_filtered_comp['week_ending'].dt.year
            
            # Get available seasons for comparison
            available_seasons = sorted(df_filtered_comp['season'].unique(), reverse=True)
            
            if selected_region == 'All Regions':
                df_comp_grouped = df_filtered_comp.groupby(['week_number', 'season'])[metric_column].mean().reset_index()
                
                fig = px.line(
                    df_comp_grouped,
                    x='week_number',
                    y=metric_column,
                    color='season',
                    title=f'{selected_metric} - Season Comparison',
                    labels={
                        'week_number': 'Week of Year',
                        metric_column: selected_metric,
                        'season': 'Season'
                    }
                )
            else:
                df_region_comp = df_filtered_comp[df_filtered_comp['region'] == selected_region]
                df_comp_grouped = df_region_comp.groupby(['week_number', 'season'])[metric_column].mean().reset_index()
                
                fig = px.line(
                    df_comp_grouped,
                    x='week_number',
                    y=metric_column,
                    color='season',
                    title=f'{selected_metric} for {selected_region} - Season Comparison',
                    labels={
                        'week_number': 'Week of Year',
                        metric_column: selected_metric,
                        'season': 'Season'
                    }
                )
            
            fig.update_layout(
                height=400,
                hovermode='x unified',
                xaxis_title="Week of Year",
                yaxis_title=selected_metric,
                legend_title_text="Season"
            )
        else:
            # Standard time series view
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
        
        if enable_comparison:
            st.caption("📊 Chart shows data aligned by week of year to compare seasonal patterns across different flu seasons.")
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
    
    st.markdown("### 📥 Export Data")
    st.markdown("Download the current filtered dataset in your preferred format:")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        csv = df_display_formatted.to_csv(index=False)
        st.download_button(
            label="📄 Download CSV",
            data=csv,
            file_name=f"cdc_flu_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df_display_formatted.to_excel(writer, sheet_name='Flu Data', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Flu Data']
            
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1
            })
            
            for col_num, value in enumerate(df_display_formatted.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, len(str(value)) + 5)
        
        st.download_button(
            label="📊 Download Excel",
            data=buffer.getvalue(),
            file_name=f"cdc_flu_data_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    with col3:
        st.metric(
            label="Records in Export",
            value=f"{len(df_display_formatted):,}",
            help="Number of records in the current filtered view (max 100 shown)"
        )
else:
    st.info("No data available for selected filters")

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔔 Data Quality Alerts")

# Data freshness check
latest_date, freshness_status = check_data_freshness(df)
if latest_date:
    days_old = (datetime.now() - latest_date).days
    st.sidebar.markdown(f"**Data Status:** {freshness_status}")
    st.sidebar.markdown(f"Latest Data: {latest_date.strftime('%Y-%m-%d')} ({days_old} days old)")

# Anomaly detection
metric_column = 'percent_positive' if selected_metric == 'Percent Positive' else 'total_specimens'
anomalies = detect_anomalies(df_filtered, metric=metric_column, threshold_std=2.5)

if anomalies:
    st.sidebar.warning(f"⚠️ {len(anomalies)} anomalies detected in recent data")
    with st.sidebar.expander("View Anomaly Details"):
        for anomaly in anomalies[:3]:  # Show top 3
            st.markdown(f"**{anomaly['region']}** - {anomaly['date'].strftime('%Y-%m-%d')}")
            st.markdown(f"Value: {anomaly['value']:.2f} ({anomaly['deviation']} deviation)")
            st.markdown(f"Z-score: {anomaly['z_score']:.2f}")
            st.markdown("---")
else:
    st.sidebar.success("✅ No anomalies detected")

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

# COVID-19 Section
st.markdown("---")
st.markdown("---")
st.markdown("## 🏥 COVID-19 Hospitalization Surveillance (NHSN)")

df_covid = load_covid_data()

if not df_covid.empty:
    # Convert date column
    df_covid['date'] = pd.to_datetime(df_covid['date'])
    
    # State selection for COVID
    st.subheader("📊 COVID-19 Hospital Admissions by State")
    
    covid_locations = get_covid_locations()
    national_data = df_covid[df_covid['geo_type'] == 'nation']
    
    covid_col1, covid_col2 = st.columns([1, 3])
    
    with covid_col1:
        location_options = ['National (US)'] + covid_locations
        selected_location = st.selectbox(
            "Select Location",
            options=location_options,
            index=0,
            key='covid_location'
        )
    
    with covid_col2:
        # Filter data based on selection
        if selected_location == 'National (US)':
            df_covid_filtered = national_data
        else:
            df_covid_filtered = df_covid[
                (df_covid['geo_type'] == 'state') & 
                (df_covid['geo_value'] == selected_location.lower())
            ]
        
        if not df_covid_filtered.empty:
            # Latest metric
            latest_admissions = df_covid_filtered.sort_values('date', ascending=False).iloc[0]['confirmed_7day_avg']
            latest_date = df_covid_filtered.sort_values('date', ascending=False).iloc[0]['date']
            
            st.metric(
                label=f"Weekly COVID-19 Hospital Admissions ({selected_location})",
                value=f"{latest_admissions:.0f}" if pd.notna(latest_admissions) else "N/A",
                help=f"7-day average as of {latest_date.strftime('%Y-%m-%d')}"
            )
    
    # COVID Trend Chart
    if not df_covid_filtered.empty:
        fig_covid = px.area(
            df_covid_filtered.sort_values('date'),
            x='date',
            y='confirmed_7day_avg',
            title=f'COVID-19 Hospital Admissions Trend - {selected_location}',
            labels={
                'date': 'Week Ending',
                'confirmed_7day_avg': 'Weekly Hospital Admissions'
            }
        )
        
        fig_covid.update_layout(
            height=400,
            hovermode='x unified',
            xaxis_title="Week Ending",
            yaxis_title="Hospital Admissions (7-day avg)",
            showlegend=False
        )
        
        fig_covid.update_traces(
            fillcolor='rgba(255, 127, 14, 0.3)',
            line=dict(color='rgb(255, 127, 14)', width=2)
        )
        
        st.plotly_chart(fig_covid, use_container_width=True)
        
        st.info("ℹ️ Data Source: NHSN (National Healthcare Safety Network) - Mandatory reporting resumed November 1, 2024")
    else:
        st.warning(f"No COVID-19 data available for {selected_location}")
else:
    st.warning("⚠️ No COVID-19 data available. Please run the COVID ETL pipeline to fetch hospitalization data.")
    st.info("Run the following command to fetch COVID-19 data: `python covid_etl.py`")
