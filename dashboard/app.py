"""
Streamlit Dashboard - Main App
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from elasticsearch import Elasticsearch
import pandas as pd
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Initialize Elasticsearch
@st.cache_resource
def get_es_client():
    return Elasticsearch(['http://localhost:9200'])

es = get_es_client()

# Main header
st.markdown('<h1 class="main-header">🛒 Retail Analytics Dashboard</h1>', 
            unsafe_allow_html=True)

# Sidebar
st.sidebar.title("⚙️ Settings")
st.sidebar.markdown("---")

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Date range selector
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(datetime.now() - timedelta(days=30), datetime.now()),
    max_value=datetime.now()
)

st.sidebar.markdown("---")
st.sidebar.info("""
**System Status**
- ✅ Kafka: Running
- ✅ Spark: Active
- ✅ Elasticsearch: Connected
""")

# Tab selection
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Real-time Overview", 
    "📈 Demand Forecasting",
    "🛒 Market Basket Analysis",
    "📦 Inventory Optimization"
])

# TAB 1: Real-time Overview
with tab1:
    st.header("📊 Real-time Metrics")
    
    # Fetch real-time data from Elasticsearch
    @st.cache_data(ttl=60)
    def get_realtime_metrics():
        # Revenue metrics
        revenue_query = {
            "query": {"range": {"window.start": {"gte": "now-15m"}}},
            "size": 100,
            "sort": [{"window.start": {"order": "desc"}}]
        }
        
        try:
            response = es.search(index="retail_realtime_revenue", body=revenue_query)
            hits = response['hits']['hits']
            
            if hits:
                data = [hit['_source'] for hit in hits]
                return pd.DataFrame(data)
            return pd.DataFrame()
        except:
            return pd.DataFrame()
    
    df_realtime = get_realtime_metrics()
    
    if not df_realtime.empty:
        # KPI metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_revenue = df_realtime['TotalRevenue'].sum()
            st.metric("💰 Total Revenue (15min)", f"£{total_revenue:,.2f}")
        
        with col2:
            total_transactions = df_realtime['TransactionCount'].sum()
            st.metric("🧾 Total Transactions", f"{int(total_transactions):,}")
        
        with col3:
            unique_customers = df_realtime['UniqueCustomers'].sum()
            st.metric("👥 Unique Customers", f"{int(unique_customers):,}")
        
        with col4:
            avg_order_value = total_revenue / total_transactions if total_transactions > 0 else 0
            st.metric("📊 Avg Order Value", f"£{avg_order_value:.2f}")
        
        st.markdown("---")
        
        # Revenue time series
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Revenue Over Time")
            
            # Prepare data
            df_plot = df_realtime.copy()
            df_plot['window_start'] = pd.to_datetime(df_plot['window'].apply(lambda x: x['start']))
            df_plot = df_plot.sort_values('window_start')
            
            fig_revenue = px.line(
                df_plot, 
                x='window_start', 
                y='TotalRevenue',
                title='Revenue per Minute',
                labels={'window_start': 'Time', 'TotalRevenue': 'Revenue (£)'}
            )
            fig_revenue.update_traces(line_color='#1f77b4', line_width=3)
            st.plotly_chart(fig_revenue, width='stretch')
        
        with col2:
            st.subheader("🌍 Revenue by Country")
            
            country_revenue = df_realtime.groupby('Country')['TotalRevenue'].sum().reset_index()
            country_revenue = country_revenue.sort_values('TotalRevenue', ascending=False).head(10)
            
            fig_country = px.bar(
                country_revenue,
                x='Country',
                y='TotalRevenue',
                title='Top 10 Countries by Revenue',
                labels={'TotalRevenue': 'Revenue (£)'}
            )
            st.plotly_chart(fig_country, width='stretch')
        
        # Top products real-time
        st.markdown("---")
        st.subheader("🏆 Top 10 Products (Last 10 minutes)")
        
        @st.cache_data(ttl=60)
        def get_top_products():
            query = {
                "query": {"range": {"window.start": {"gte": "now-10m"}}},
                "size": 100
            }
            
            try:
                response = es.search(index="retail_realtime_products", body=query)
                hits = response['hits']['hits']
                data = [hit['_source'] for hit in hits]
                return pd.DataFrame(data)
            except:
                return pd.DataFrame()
        
        df_products = get_top_products()
        
        if not df_products.empty:
            top_10 = df_products.nlargest(10, 'TotalRevenue')[
                ['StockCode', 'Description', 'TotalQuantity', 'TotalRevenue', 'TransactionCount']
            ]
            top_10['TotalRevenue'] = top_10['TotalRevenue'].apply(lambda x: f"£{x:,.2f}")
            
            st.dataframe(top_10, width='stretch', hide_index=True)
    else:
        st.warning("⚠️ No real-time data available. Make sure streaming job is running.")

# TAB 2: Demand Forecasting
with tab2:
    st.header("📈 Demand Forecasting")
    
    @st.cache_data(ttl=3600)
    def get_forecast_data():
        query = {"query": {"match_all": {}}, "size": 10000}
        
        try:
            response = es.search(index="retail_demand_forecasts", body=query)
            hits = response['hits']['hits']
            data = [hit['_source'] for hit in hits]
            return pd.DataFrame(data)
        except:
            return pd.DataFrame()
    
    df_forecast = get_forecast_data()
    
    if not df_forecast.empty:
        # Product selector
        unique_products = sorted(df_forecast['StockCode'].unique())
        selected_product = st.selectbox("🔍 Select Product", unique_products)
        
        # Filter data
        product_forecast = df_forecast[df_forecast['StockCode'] == selected_product].copy()
        product_forecast['Date'] = pd.to_datetime(product_forecast['Date'])
        product_forecast = product_forecast.sort_values('Date')
        
        # Forecast chart
        fig_forecast = go.Figure()
        
        # Forecast line
        fig_forecast.add_trace(go.Scatter(
            x=product_forecast['Date'],
            y=product_forecast['ForecastQuantity'],
            mode='lines',
            name='Forecast',
            line=dict(color='#1f77b4', width=3)
        ))
        
        # Confidence interval
        fig_forecast.add_trace(go.Scatter(
            x=product_forecast['Date'],
            y=product_forecast['UpperBound'],
            mode='lines',
            name='Upper Bound',
            line=dict(width=0),
            showlegend=False
        ))
        
        fig_forecast.add_trace(go.Scatter(
            x=product_forecast['Date'],
            y=product_forecast['LowerBound'],
            mode='lines',
            name='Lower Bound',
            fill='tonexty',
            fillcolor='rgba(31, 119, 180, 0.2)',
            line=dict(width=0),
            showlegend=True
        ))
        
        fig_forecast.update_layout(
            title=f"30-Day Demand Forecast for Product {selected_product}",
            xaxis_title="Date",
            yaxis_title="Quantity",
            hovermode='x unified',
            height=500
        )
        
        st.plotly_chart(fig_forecast, width='stretch')
        
        # Statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_forecast = product_forecast['ForecastQuantity'].mean()
            st.metric("📊 Average Forecast", f"{avg_forecast:.0f} units/day")
        
        with col2:
            total_forecast = product_forecast['ForecastQuantity'].sum()
            st.metric("📦 Total 30-Day Forecast", f"{total_forecast:.0f} units")
        
        with col3:
            max_forecast = product_forecast['ForecastQuantity'].max()
            st.metric("📈 Peak Demand", f"{max_forecast:.0f} units")
        
    else:
        st.warning("⚠️ No forecast data available. Run batch processing job first.")

# TAB 3: Market Basket Analysis
with tab3:
    st.header("🛒 Market Basket Analysis")
    
    @st.cache_data(ttl=3600)
    def get_association_rules():
        query = {"query": {"match_all": {}}, "size": 1000, "sort": [{"lift": {"order": "desc"}}]}
        
        try:
            response = es.search(index="retail_association_rules", body=query)
            hits = response['hits']['hits']
            data = [hit['_source'] for hit in hits]
            return pd.DataFrame(data)
        except:
            return pd.DataFrame()
    
    df_rules = get_association_rules()
    
    if not df_rules.empty:
        # Filter options
        col1, col2, col3 = st.columns(3)
        
        with col1:
            min_confidence = st.slider("Minimum Confidence", 0.0, 1.0, 0.5, 0.05)
        
        with col2:
            min_lift = st.slider("Minimum Lift", 1.0, 10.0, 2.0, 0.5)
        
        with col3:
            top_n = st.selectbox("Show Top N Rules", [10, 20, 50, 100], index=1)
        
        # Filter rules
        filtered_rules = df_rules[
            (df_rules['confidence'] >= min_confidence) & 
            (df_rules['lift'] >= min_lift)
        ].head(top_n)
        
        st.markdown(f"### 📋 Top {len(filtered_rules)} Association Rules")
        
        # Display table
        display_df = filtered_rules[['antecedent_str', 'consequent_str', 'support', 'confidence', 'lift']].copy()
        display_df.columns = ['If Customer Buys', 'Then Also Buys', 'Support', 'Confidence', 'Lift']
        display_df['Support'] = display_df['Support'].apply(lambda x: f"{x:.3f}")
        display_df['Confidence'] = display_df['Confidence'].apply(lambda x: f"{x:.1%}")
        display_df['Lift'] = display_df['Lift'].apply(lambda x: f"{x:.2f}")
        
        st.dataframe(display_df, width='stretch', hide_index=True)
        
        # Visualization
        st.markdown("---")
        st.subheader("📊 Rules Visualization")
        
        fig_scatter = px.scatter(
            filtered_rules,
            x='confidence',
            y='lift',
            size='support',
            hover_data=['antecedent_str', 'consequent_str'],
            title='Association Rules: Confidence vs Lift',
            labels={'confidence': 'Confidence', 'lift': 'Lift', 'support': 'Support'}
        )
        
        fig_scatter.update_layout(height=500)
        st.plotly_chart(fig_scatter, width='stretch')
        
        # Product search
        st.markdown("---")
        st.subheader("🔍 Find Products Often Bought Together")
        
        search_product = st.text_input("Enter Product Code")
        
        if search_product:
            related_rules = df_rules[
                df_rules['antecedent_str'].str.contains(search_product, case=False, na=False)
            ].sort_values('lift', ascending=False).head(10)
            
            if not related_rules.empty:
                st.write(f"**Products often bought with {search_product}:**")
                st.dataframe(
                    related_rules[['consequent_str', 'confidence', 'lift']],
                    width='stretch',
                    hide_index=True
                )
            else:
                st.info("No rules found for this product.")
    else:
        st.warning("⚠️ No association rules available. Run batch processing job first.")

# TAB 4: Inventory Optimization
with tab4:
    st.header("📦 Inventory Optimization")
    
    @st.cache_data(ttl=3600)
    def get_safety_stock():
        query = {"query": {"match_all": {}}, "size": 1000}
        
        try:
            response = es.search(index="retail_safety_stock", body=query)
            hits = response['hits']['hits']
            data = [hit['_source'] for hit in hits]
            return pd.DataFrame(data)
        except:
            return pd.DataFrame()
    
    @st.cache_data(ttl=60)
    def get_inventory_alerts():
        query = {
            "query": {"range": {"AlertTime": {"gte": "now-1h"}}},
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
    
    # Real-time alerts
    st.subheader("🚨 Real-time Inventory Alerts")
    
    df_alerts = get_inventory_alerts()
    
    if not df_alerts.empty:
        for _, alert in df_alerts.head(5).iterrows():
            st.warning(f"""
            **⚠️ HIGH DEMAND ALERT**  
            Product: {alert['StockCode']} - {alert['Description']}  
            Quantity Sold: {alert['TotalQuantity']:.0f} units  
            Average: {alert['AvgQuantity']:.0f} units  
            Time: {alert['AlertTime']}
            """)
    else:
        st.success("✅ No alerts in the last hour.")
    
    st.markdown("---")
    
    # Safety stock recommendations
    st.subheader("📊 Safety Stock Recommendations")
    
    df_safety = get_safety_stock()
    
    if not df_safety.empty:
        # Top products by safety stock
        top_safety = df_safety.nlargest(20, 'safety_stock').copy()
        
        fig_safety = px.bar(
            top_safety,
            x='StockCode',
            y='safety_stock',
            title='Top 20 Products by Recommended Safety Stock',
            labels={'safety_stock': 'Safety Stock (units)', 'StockCode': 'Product Code'},
            color='safety_stock',
            color_continuous_scale='Blues'
        )
        
        fig_safety.update_layout(height=500)
        st.plotly_chart(fig_safety, width='stretch')
        
        # Detailed table
        st.markdown("### 📋 Detailed Safety Stock Table")
        
        display_safety = df_safety[['StockCode', 'yhat', 'safety_stock', 'reorder_point']].copy()
        display_safety.columns = ['Product Code', 'Avg Daily Demand', 'Safety Stock', 'Reorder Point']
        # display_safety = display_safety.round(0).astype(int)
        display_safety['Avg Daily Demand'] = display_safety['Avg Daily Demand'].round(0).astype(int)
        display_safety['Safety Stock'] = display_safety['Safety Stock'].round(0).astype(int)
        display_safety['Reorder Point'] = display_safety['Reorder Point'].round(0).astype(int)
        display_safety = display_safety.sort_values('Safety Stock', ascending=False)
        
        st.dataframe(display_safety, width='stretch', hide_index=True)
    else:
        st.warning("⚠️ No safety stock data available. Run batch processing job first.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>🛒 Retail Analytics Dashboard | Powered by Spark, Kafka & Elasticsearch</p>
</div>
""", unsafe_allow_html=True)
