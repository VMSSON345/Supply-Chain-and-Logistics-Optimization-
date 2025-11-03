"""
Inventory Optimization Dashboard Page
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from elasticsearch import Elasticsearch
import pandas as pd
from datetime import datetime
import sys
sys.path.append('..')
from components.metrics import display_kpi_card, format_number

st.set_page_config(page_title="Inventory Optimization", page_icon="📦", layout="wide")

# Initialize ES
@st.cache_resource
def get_es_client():
    return Elasticsearch(['http://localhost:9200'])

es = get_es_client()

# Header
st.title("📦 Inventory Optimization")
st.markdown("Real-time inventory alerts and safety stock recommendations")
st.markdown("---")

# Fetch data
@st.cache_data(ttl=60)
def fetch_inventory_alerts():
    query = {
        "query": {"range": {"AlertTime": {"gte": "now-24h"}}},
        "size": 100,
        "sort": [{"AlertTime": {"order": "desc"}}]
    }
    
    try:
        response = es.search(index="retail_inventory_alerts", body=query)
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

df_alerts = fetch_inventory_alerts()
df_safety = fetch_safety_stock()

# Alert Section
st.header("🚨 Real-time Inventory Alerts")

# Refresh button
if st.button("🔄 Refresh Alerts"):
    st.cache_data.clear()
    st.rerun()

# Alert summary
col1, col2, col3, col4 = st.columns(4)

if not df_alerts.empty:
    total_alerts = len(df_alerts)
    high_demand = len(df_alerts[df_alerts['AlertType'] == 'HIGH_DEMAND'])
    critical_alerts = len(df_alerts[df_alerts.get('DemandIncreasePct', 0) > 5])
    
    with col1:
        st.metric("🔔 Total Alerts (24h)", format_number(total_alerts))
    with col2:
        st.metric("⚡ High Demand", format_number(high_demand))
    with col3:
        st.metric("🔥 Critical", format_number(critical_alerts))
    with col4:
        st.metric("✅ Resolved", "0")
else:
    with col1:
        st.metric("🔔 Total Alerts (24h)", "0")
    with col2:
        st.metric("⚡ High Demand", "0")
    with col3:
        st.metric("🔥 Critical", "0")
    with col4:
        st.metric("✅ Resolved", "0")

st.markdown("---")

# Recent Alerts
st.subheader("📋 Recent Alerts")

if not df_alerts.empty:
    # Display alerts as cards
    for idx, alert in df_alerts.head(10).iterrows():
        alert_type = alert.get('AlertType', 'UNKNOWN')
        
        # Determine alert color
        if alert_type == 'HIGH_DEMAND':
            alert_color = "🔴"
            color_class = "error"
        elif alert_type == 'STOCK_OUT_RISK':
            alert_color = "🟠"
            color_class = "warning"
        else:
            alert_color = "🟡"
            color_class = "info"
        
        with st.container():
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                st.markdown(f"**{alert_color} {alert_type}**")
                st.markdown(f"**Product:** {alert.get('StockCode', 'N/A')} - {alert.get('Description', 'N/A')}")
            
            with col2:
                current_qty = alert.get('CurrentQuantity', alert.get('TotalQuantity', 0))
                st.metric("Quantity", format_number(current_qty))
            
            with col3:
                avg_qty = alert.get('AvgQuantity', 0)
                if avg_qty > 0:
                    increase_pct = ((current_qty - avg_qty) / avg_qty) * 100
                    st.metric("vs Avg", f"+{increase_pct:.0f}%", delta_color="normal")
                else:
                    st.metric("vs Avg", "N/A")
            
            alert_time = alert.get('AlertTime', 'Unknown')
            if isinstance(alert_time, str):
                try:
                    alert_dt = pd.to_datetime(alert_time)
                    time_ago = datetime.now() - alert_dt
                    hours_ago = time_ago.total_seconds() / 3600
                    st.caption(f"🕐 {hours_ago:.1f} hours ago | {alert_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                except:
                    st.caption(f"🕐 {alert_time}")
            
            st.markdown("---")
else:
    st.success("✅ No alerts in the last 24 hours. All inventory levels are normal.")

st.markdown("---")

# Safety Stock Recommendations
st.header("📊 Safety Stock Recommendations")

if not df_safety.empty:
    # Filters
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_product = st.text_input("🔍 Search Product Code", "")
    
    with col2:
        sort_by = st.selectbox("Sort by", ['Safety Stock', 'Reorder Point', 'Avg Demand'])
    
    # Filter data
    display_safety = df_safety.copy()
    
    if search_product:
        display_safety = display_safety[
            display_safety['StockCode'].str.contains(search_product, case=False, na=False)
        ]
    
    # Sort
    sort_mapping = {
        'Safety Stock': 'safety_stock',
        'Reorder Point': 'reorder_point',
        'Avg Demand': 'yhat'
    }
    
    if sort_mapping[sort_by] in display_safety.columns:
        display_safety = display_safety.sort_values(sort_mapping[sort_by], ascending=False)
    
    # Top products chart
    st.subheader("🏆 Top 20 Products by Safety Stock Requirement")
    
    top_20 = display_safety.head(20)
    
    fig = px.bar(
        top_20,
        x='StockCode',
        y='safety_stock',
        title='Recommended Safety Stock Levels',
        labels={'safety_stock': 'Safety Stock (units)', 'StockCode': 'Product Code'},
        color='safety_stock',
        color_continuous_scale='Reds'
    )
    
    fig.update_layout(
        height=400,
        showlegend=False,
        xaxis={'categoryorder':'total descending'}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.subheader("📋 Detailed Safety Stock Table")
    
    table_display = display_safety.copy()
    table_display['yhat'] = table_display['yhat'].round(0).astype(int)
    table_display['safety_stock'] = table_display['safety_stock'].round(0).astype(int)
    table_display['reorder_point'] = table_display['reorder_point'].round(0).astype(int)
    
    table_display = table_display[['StockCode', 'yhat', 'safety_stock', 'reorder_point']]
    table_display.columns = ['Product Code', 'Avg Daily Demand', 'Safety Stock', 'Reorder Point']
    
    st.dataframe(
        table_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Product Code": st.column_config.TextColumn(width="small"),
            "Avg Daily Demand": st.column_config.NumberColumn(format="%d units"),
            "Safety Stock": st.column_config.NumberColumn(format="%d units"),
            "Reorder Point": st.column_config.NumberColumn(format="%d units"),
        }
    )
    
    # Download button
    csv = table_display.to_csv(index=False)
    st.download_button(
        label="📥 Download Safety Stock Data",
        data=csv,
        file_name="safety_stock_recommendations.csv",
        mime="text/csv"
    )
    
    st.markdown("---")
    
    # Inventory Distribution
    st.subheader("📊 Inventory Requirements Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.histogram(
            display_safety,
            x='safety_stock',
            nbins=30,
            title='Safety Stock Distribution',
            labels={'safety_stock': 'Safety Stock (units)', 'count': 'Number of Products'},
            color_discrete_sequence=['#1f77b4']
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.box(
            display_safety,
            y='safety_stock',
            title='Safety Stock Box Plot',
            labels={'safety_stock': 'Safety Stock (units)'},
            color_discrete_sequence=['#1f77b4']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Key Statistics
    st.markdown("---")
    st.subheader("📈 Key Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_safety_stock = display_safety['safety_stock'].sum()
        st.metric("Total Safety Stock", format_number(total_safety_stock, 0) + " units")
    
    with col2:
        avg_safety_stock = display_safety['safety_stock'].mean()
        st.metric("Avg per Product", format_number(avg_safety_stock, 0) + " units")
    
    with col3:
        max_safety_stock = display_safety['safety_stock'].max()
        st.metric("Max Requirement", format_number(max_safety_stock, 0) + " units")
    
    with col4:
        total_products = len(display_safety)
        st.metric("Total Products", format_number(total_products))
    
else:
    st.warning("⚠️ No safety stock data available. Please run the batch processing job first.")

# Recommendations
st.markdown("---")
st.subheader("💡 Inventory Management Recommendations")

col1, col2 = st.columns(2)

with col1:
    st.info("""
    **🎯 Best Practices:**
    - Review safety stock levels monthly
    - Adjust based on seasonality
    - Monitor lead times from suppliers
    - Use ABC analysis for prioritization
    - Implement automated reorder triggers
    """)

with col2:
    st.success("""
    **📊 Optimization Tips:**
    - Focus on high-value items first
    - Balance holding costs vs stockout costs
    - Consider supplier reliability
    - Account for demand variability
    - Set up safety stock alerts
    """)

# Footer
st.markdown("---")
st.caption(f"Recommendations based on 95% service level | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
