"""
Demand Forecasting Dashboard Page
"""
import streamlit as st
import plotly.graph_objects as go
from elasticsearch import Elasticsearch
import pandas as pd
from datetime import datetime
import sys
sys.path.append('..')
from components.charts import create_forecast_chart
from components.metrics import display_kpi_card, format_number

st.set_page_config(page_title="Demand Forecasting", page_icon="📈", layout="wide")

# Initialize ES
@st.cache_resource
def get_es_client():
    return Elasticsearch(['http://localhost:9200'])

es = get_es_client()

# Header
st.title("📈 Demand Forecasting")
st.markdown("30-Day Product Demand Predictions using Prophet Time Series Model")
st.markdown("---")

# Fetch forecast data
@st.cache_data(ttl=3600)
def fetch_forecast_data():
    query = {"query": {"match_all": {}}, "size": 10000}
    
    try:
        response = es.search(index="retail_demand_forecasts", body=query)
        hits = response['hits']['hits']
        data = [hit['_source'] for hit in hits]
        return pd.DataFrame(data)
    except:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_safety_stock():
    query = {"query": {"match_all": {}}, "size": 1000}
    
    try:
        response = es.search(index="retail_safety_stock", body=query)
        hits = response['hits']['hits']
        data = [hit['_source'] for hit in hits]
        return pd.DataFrame(data)
    except:
        return pd.DataFrame()

df_forecast = fetch_forecast_data()
df_safety = fetch_safety_stock()

if df_forecast.empty:
    st.error("❌ No forecast data available. Please run the batch processing job first.")
    st.stop()

# Product selector
st.sidebar.header("⚙️ Forecast Settings")

# Get unique products
unique_products = sorted(df_forecast['StockCode'].unique())

# Search box
search_term = st.sidebar.text_input("🔍 Search Product Code", "")

if search_term:
    filtered_products = [p for p in unique_products if search_term.upper() in p]
    if filtered_products:
        unique_products = filtered_products
    else:
        st.sidebar.warning(f"No products found matching '{search_term}'")

selected_product = st.sidebar.selectbox(
    "Select Product",
    unique_products,
    index=0
)

# Forecast horizon slider
forecast_days = st.sidebar.slider(
    "Forecast Horizon (days)",
    min_value=7,
    max_value=30,
    value=30
)

# Filter forecast data
product_forecast = df_forecast[df_forecast['StockCode'] == selected_product].copy()
product_forecast['Date'] = pd.to_datetime(product_forecast['Date'])
product_forecast = product_forecast.sort_values('Date')
product_forecast = product_forecast.head(forecast_days)

# Main content
col1, col2 = st.columns([3, 1])

with col1:
    st.subheader(f"Forecast for Product: {selected_product}")

with col2:
    if st.button("📥 Export Data"):
        csv = product_forecast.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"forecast_{selected_product}.csv",
            mime="text/csv"
        )

# KPI Metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    avg_forecast = product_forecast['ForecastQuantity'].mean()
    display_kpi_card("📊 Avg Daily Demand", format_number(avg_forecast, 1) + " units")

with col2:
    total_forecast = product_forecast['ForecastQuantity'].sum()
    display_kpi_card("📦 Total Forecast", format_number(total_forecast, 0) + " units")

with col3:
    max_forecast = product_forecast['ForecastQuantity'].max()
    display_kpi_card("📈 Peak Demand", format_number(max_forecast, 0) + " units")

with col4:
    min_forecast = product_forecast['ForecastQuantity'].min()
    display_kpi_card("📉 Min Demand", format_number(min_forecast, 0) + " units")

st.markdown("---")

# Forecast Chart
fig = create_forecast_chart(
    product_forecast,
    'Date',
    'ForecastQuantity',
    'LowerBound',
    'UpperBound',
    title=f"30-Day Demand Forecast for {selected_product}"
)

fig.update_layout(height=500)
st.plotly_chart(fig, width='stretch')

st.markdown("---")

# Forecast Table
st.subheader("📋 Detailed Forecast Data")

display_forecast = product_forecast.copy()
display_forecast['Date'] = display_forecast['Date'].dt.strftime('%Y-%m-%d')
display_forecast['ForecastQuantity'] = display_forecast['ForecastQuantity'].round(0).astype(int)
display_forecast['LowerBound'] = display_forecast['LowerBound'].round(0).astype(int)
display_forecast['UpperBound'] = display_forecast['UpperBound'].round(0).astype(int)

display_forecast = display_forecast[['Date', 'ForecastQuantity', 'LowerBound', 'UpperBound']]
display_forecast.columns = ['Date', 'Forecast', 'Lower Bound', 'Upper Bound']

st.dataframe(display_forecast, width='stretch', hide_index=True)

st.markdown("---")

# Safety Stock Section
st.subheader("📦 Inventory Recommendations")

if not df_safety.empty and selected_product in df_safety['StockCode'].values:
    safety_info = df_safety[df_safety['StockCode'] == selected_product].iloc[0]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "🛡️ Recommended Safety Stock",
            f"{int(safety_info['safety_stock']):,} units",
            help="Buffer inventory to prevent stockouts"
        )
    
    with col2:
        st.metric(
            "🔔 Reorder Point",
            f"{int(safety_info['reorder_point']):,} units",
            help="Trigger reorder when inventory falls below this level"
        )
    
    with col3:
        st.metric(
            "📊 Avg Daily Demand",
            f"{int(safety_info['yhat']):,} units",
            help="Average expected daily demand"
        )
    
    # Recommendations
    st.info(f"""
    **📌 Inventory Strategy Recommendations:**
    - Maintain at least **{int(safety_info['safety_stock']):,} units** as safety stock
    - Place reorder when inventory reaches **{int(safety_info['reorder_point']):,} units**
    - Expected daily consumption: **{int(safety_info['yhat']):,} units**
    - Lead time coverage: **{int(safety_info['safety_stock'] / safety_info['yhat']):,} days**
    """)
else:
    st.warning("⚠️ No safety stock recommendations available for this product")

st.markdown("---")

# Comparison with other products
st.subheader("📊 Demand Comparison")

# Get top 5 products by forecast
top_products = df_forecast.groupby('StockCode')['ForecastQuantity'].sum().nlargest(10).index.tolist()

if selected_product not in top_products:
    top_products.append(selected_product)

comparison_data = []
for product in top_products[:10]:
    product_data = df_forecast[df_forecast['StockCode'] == product]
    comparison_data.append({
        'Product': product,
        'Total Forecast': product_data['ForecastQuantity'].sum(),
        'Avg Daily': product_data['ForecastQuantity'].mean(),
        'Peak': product_data['ForecastQuantity'].max()
    })

comparison_df = pd.DataFrame(comparison_data)
comparison_df = comparison_df.sort_values('Total Forecast', ascending=False)

fig = go.Figure()

fig.add_trace(go.Bar(
    name='Total Forecast',
    x=comparison_df['Product'],
    y=comparison_df['Total Forecast'],
    marker_color='lightblue'
))

fig.add_trace(go.Bar(
    name='Peak Demand',
    x=comparison_df['Product'],
    y=comparison_df['Peak'],
    marker_color='darkblue'
))

fig.update_layout(
    title='Demand Comparison - Top Products',
    xaxis_title='Product Code',
    yaxis_title='Quantity',
    barmode='group',
    height=400
)

st.plotly_chart(fig, width='stretch')

# Footer
st.markdown("---")
st.caption(f"Forecast generated using Prophet Time Series Model | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
