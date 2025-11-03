"""
Metric Components
"""
import streamlit as st
import pandas as pd


def display_kpi_card(title: str, value: str, delta: str = None, 
                     delta_color: str = "normal"):
    """Display KPI metric card"""
    st.metric(
        label=title,
        value=value,
        delta=delta,
        delta_color=delta_color
    )


def display_kpi_row(metrics: list):
    """
    Display row of KPIs
    
    Args:
        metrics: List of dicts with keys: title, value, delta, delta_color
    """
    cols = st.columns(len(metrics))
    
    for col, metric in zip(cols, metrics):
        with col:
            display_kpi_card(
                title=metric['title'],
                value=metric['value'],
                delta=metric.get('delta'),
                delta_color=metric.get('delta_color', 'normal')
            )


def format_currency(value: float, currency: str = '£') -> str:
    """Format value as currency"""
    return f"{currency}{value:,.2f}"


def format_number(value: float, decimals: int = 0) -> str:
    """Format number with thousand separators"""
    if decimals == 0:
        return f"{int(value):,}"
    else:
        return f"{value:,.{decimals}f}"


def format_percentage(value: float, decimals: int = 1) -> str:
    """Format value as percentage"""
    return f"{value:.{decimals}f}%"


def calculate_growth_rate(current: float, previous: float) -> float:
    """Calculate growth rate"""
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100


def create_summary_stats(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Create summary statistics table"""
    summary = pd.DataFrame({
        'Metric': columns,
        'Mean': [df[col].mean() for col in columns],
        'Median': [df[col].median() for col in columns],
        'Min': [df[col].min() for col in columns],
        'Max': [df[col].max() for col in columns],
        'Std': [df[col].std() for col in columns]
    })
    
    return summary
