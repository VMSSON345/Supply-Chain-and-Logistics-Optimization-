"""
Reusable Chart Components
"""
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd


def create_revenue_timeseries(df: pd.DataFrame, x_col: str, y_col: str, 
                              title: str = "Revenue Over Time"):
    """Create revenue time series chart"""
    fig = px.line(
        df,
        x=x_col,
        y=y_col,
        title=title,
        labels={x_col: 'Time', y_col: 'Revenue (£)'}
    )
    
    fig.update_traces(
        line_color='#1f77b4',
        line_width=3,
        hovertemplate='<b>Time:</b> %{x}<br><b>Revenue:</b> £%{y:,.2f}<extra></extra>'
    )
    
    fig.update_layout(
        hovermode='x unified',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(size=12),
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray')
    )
    
    return fig


def create_bar_chart(df: pd.DataFrame, x_col: str, y_col: str, 
                     title: str, orientation: str = 'v'):
    """Create bar chart"""
    fig = px.bar(
        df,
        x=x_col if orientation == 'v' else y_col,
        y=y_col if orientation == 'v' else x_col,
        title=title,
        orientation=orientation,
        color=y_col if orientation == 'v' else x_col,
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=False
    )
    
    return fig


def create_forecast_chart(df: pd.DataFrame, date_col: str, 
                         forecast_col: str, lower_col: str, upper_col: str,
                         title: str = "Demand Forecast"):
    """Create forecast chart with confidence interval"""
    fig = go.Figure()
    
    # Forecast line
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[forecast_col],
        mode='lines',
        name='Forecast',
        line=dict(color='#1f77b4', width=3)
    ))
    
    # Upper bound
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[upper_col],
        mode='lines',
        name='Upper Bound',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    # Lower bound with fill
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[lower_col],
        mode='lines',
        name='Confidence Interval',
        fill='tonexty',
        fillcolor='rgba(31, 119, 180, 0.2)',
        line=dict(width=0)
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Quantity",
        hovermode='x unified',
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    return fig


def create_scatter_plot(df: pd.DataFrame, x_col: str, y_col: str,
                       size_col: str = None, color_col: str = None,
                       title: str = "Scatter Plot"):
    """Create scatter plot"""
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        size=size_col,
        color=color_col,
        title=title,
        hover_data=df.columns.tolist()
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    return fig


def create_pie_chart(df: pd.DataFrame, names_col: str, values_col: str,
                    title: str = "Distribution"):
    """Create pie chart"""
    fig = px.pie(
        df,
        names=names_col,
        values=values_col,
        title=title,
        hole=0.4
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label'
    )
    
    return fig


def create_heatmap(df: pd.DataFrame, x_col: str, y_col: str, z_col: str,
                  title: str = "Heatmap"):
    """Create heatmap"""
    pivot = df.pivot(index=y_col, columns=x_col, values=z_col)
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale='Blues'
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        yaxis_title=y_col
    )
    
    return fig


def create_gauge_chart(value: float, title: str, 
                      max_value: float = 100, 
                      thresholds: dict = None):
    """Create gauge chart"""
    if thresholds is None:
        thresholds = {'low': 30, 'medium': 70}
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
        gauge={
            'axis': {'range': [None, max_value]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, thresholds['low']], 'color': "lightgray"},
                {'range': [thresholds['low'], thresholds['medium']], 'color': "gray"},
                {'range': [thresholds['medium'], max_value], 'color': "darkgray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))
    
    return fig
