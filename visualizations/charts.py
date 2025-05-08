import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

def create_sales_trend_chart(df):
    """
    Create a time series chart showing sales trends.
    
    Args:
        df (DataFrame): Product data
        
    Returns:
        Figure: Plotly figure object
    """
    # Make sure timestamp is datetime
    if 'timestamp' in df.columns and df['timestamp'].dtype != 'datetime64[ns]':
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Create date groups
    df['date'] = df['timestamp'].dt.date
    
    # Group by date and count products
    date_counts = df.groupby('date').size().reset_index(name='count')
    
    # Create the chart
    fig = px.line(
        date_counts, 
        x='date', 
        y='count',
        title='Product Listing Trend',
        labels={'date': 'Date', 'count': 'Number of Products'},
        markers=True
    )
    
    # Add hover information
    fig.update_traces(
        hovertemplate='Date: %{x}<br>Products: %{y}<extra></extra>'
    )
    
    # Customize layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Number of Products",
        hovermode="x unified"
    )
    
    return fig

def create_category_comparison_chart(df):
    """
    Create a chart comparing different product categories over time.
    
    Args:
        df (DataFrame): Product data
        
    Returns:
        Figure: Plotly figure object
    """
    # Make sure timestamp is datetime
    if 'timestamp' in df.columns and df['timestamp'].dtype != 'datetime64[ns]':
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Create date groups
    df['date'] = df['timestamp'].dt.date
    
    # Group by date and category
    category_date_counts = df.groupby(['date', 'category']).size().reset_index(name='count')
    
    # Create the chart
    fig = px.line(
        category_date_counts,
        x='date',
        y='count',
        color='category',
        title='Product Category Comparison Over Time',
        labels={'date': 'Date', 'count': 'Number of Products', 'category': 'Category'},
        markers=True
    )
    
    # Customize layout
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Number of Products",
        legend_title="Category",
        hovermode="x unified"
    )
    
    return fig

def create_price_distribution_chart(df):
    """
    Create a histogram showing price distribution.
    
    Args:
        df (DataFrame): Product data
        
    Returns:
        Figure: Plotly figure object
    """
    # Create bins for prices
    max_price = min(df['price'].max(), df['price'].quantile(0.95) * 1.5)  # Cap to avoid extreme outliers
    
    # Create the chart
    fig = px.histogram(
        df[df['price'] <= max_price],  # Filter outliers
        x='price',
        title='Price Distribution',
        labels={'price': 'Price (₦)', 'count': 'Number of Products'},
        opacity=0.8,
        nbins=20,
    )
    
    # Add median line
    median_price = df['price'].median()
    fig.add_vline(
        x=median_price,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Median: ₦{median_price:,.2f}",
        annotation_position="top right"
    )
    
    # Customize layout
    fig.update_layout(
        xaxis_title="Price (₦)",
        yaxis_title="Number of Products",
        bargap=0.1
    )
    
    return fig

def create_bestseller_chart(df):
    """
    Create a chart showing bestselling products.
    
    Args:
        df (DataFrame): Product data
        
    Returns:
        Figure: Plotly figure object
    """
    # Find bestselling products
    if 'is_bestseller' in df.columns:
        bestsellers = df[df['is_bestseller'] == True]
    else:
        # If no bestseller flag, use sales_rank or view_count
        if 'sales_rank' in df.columns:
            bestsellers = df.sort_values('sales_rank').head(10)
        elif 'view_count' in df.columns:
            bestsellers = df.sort_values('view_count', ascending=False).head(10)
        else:
            # If no ranking metrics available, just use the first 10 products
            bestsellers = df.head(10)
    
    # Limit to top 10
    bestsellers = bestsellers.head(10)
    
    # Sort by price for the chart
    bestsellers = bestsellers.sort_values('price', ascending=True)
    
    # Create the chart
    fig = px.bar(
        bestsellers,
        x='product_name',
        y='price',
        color='category',
        title='Top Selling Products by Price',
        labels={'product_name': 'Product', 'price': 'Price (₦)', 'category': 'Category'},
        hover_data=['source']
    )
    
    # Customize layout
    fig.update_layout(
        xaxis_title="Product",
        yaxis_title="Price (₦)",
        xaxis_tickangle=-45,
        legend_title="Category"
    )
    
    return fig

def create_category_price_comparison(df):
    """
    Create a box plot comparing prices across categories.
    
    Args:
        df (DataFrame): Product data
        
    Returns:
        Figure: Plotly figure object
    """
    # Calculate statistics by category
    category_stats = df.groupby('category')['price'].agg(['mean', 'median', 'count']).reset_index()
    category_stats = category_stats.sort_values('count', ascending=False)
    
    # Create the chart
    fig = px.box(
        df,
        x='category',
        y='price',
        color='category',
        title='Price Distribution by Category',
        labels={'category': 'Category', 'price': 'Price (₦)'},
        category_orders={'category': category_stats['category'].tolist()}
    )
    
    # Customize layout
    fig.update_layout(
        xaxis_title="Category",
        yaxis_title="Price (₦)",
        showlegend=False
    )
    
    return fig

def create_source_comparison_chart(df):
    """
    Create a chart comparing products from different sources.
    
    Args:
        df (DataFrame): Product data
        
    Returns:
        Figure: Plotly figure object
    """
    # Group by source and category
    source_category = df.groupby(['source', 'category']).size().reset_index(name='count')
    
    # Create the chart
    fig = px.bar(
        source_category,
        x='source',
        y='count',
        color='category',
        title='Product Distribution by Source and Category',
        labels={'source': 'Source', 'count': 'Number of Products', 'category': 'Category'},
        barmode='group'
    )
    
    # Customize layout
    fig.update_layout(
        xaxis_title="Source",
        yaxis_title="Number of Products",
        legend_title="Category"
    )
    
    return fig
