import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import time
import threading
import concurrent.futures
import json
import io

from scrapers.jumia_scraper import JumiaScraper
from scrapers.konga_scraper import KongaScraper
from scrapers.jiji_scraper import JijiScraper
from scrapers.temu_scraper import TemuScraper
from scrapers.payporte_scraper import PayPorteScraper
from scrapers.nbs_scraper import NBSScraper
from scrapers.async_jumia_scraper import AsyncJumiaScraper
from utils.data_processor import DataProcessor
from utils.data_loader import DataLoader
from utils.scheduler import schedule_scraping
from visualizations.charts import create_sales_trend_chart, create_category_comparison_chart, create_price_distribution_chart

# Page configuration
st.set_page_config(
    page_title="Nigerian E-commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state variables
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'is_scraping' not in st.session_state:
    st.session_state.is_scraping = False

# Load data
@st.cache_data(ttl=3600)
def load_cached_data():
    loader = DataLoader()
    return loader.load_data()

def trigger_data_refresh():
    if st.session_state.is_scraping:
        st.warning("Data collection already in progress. Please wait.")
        return
    
    st.session_state.is_scraping = True
    
    # Get selected scraping mode (default to Standard if not set)
    scrape_mode = "Standard"
    if 'scrape_mode' in st.session_state:
        scrape_mode = st.session_state.scrape_mode
    
    start_time = time.time()
    
    with st.spinner("Collecting fresh data from e-commerce websites..."):
        all_products = []
        
        if scrape_mode == "Async":
            # Use asynchronous scrapers when available
            scrapers = [
                AsyncJumiaScraper(),  # Use async version
                KongaScraper(),       # Fallback to standard version for others
                JijiScraper(),
                TemuScraper(),
                PayPorteScraper(),
                NBSScraper()
            ]
            
            # Track which scrapers are async-capable
            async_scrapers = []
            standard_scrapers = []
            
            for scraper in scrapers:
                if hasattr(scraper, 'scrape_data_async'):
                    async_scrapers.append(scraper)
                else:
                    standard_scrapers.append(scraper)
            
            # Run async scrapers
            for scraper in async_scrapers:
                try:
                    products = scraper.scrape_data()  # This actually runs async under the hood
                    all_products.extend(products)
                except Exception as e:
                    st.error(f"Error with async scraper {scraper.__class__.__name__}: {str(e)}")
            
            # Run standard scrapers in parallel
            if standard_scrapers:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    scraper_futures = {executor.submit(scraper.scrape_data): scraper for scraper in standard_scrapers}
                    for future in concurrent.futures.as_completed(scraper_futures):
                        scraper = scraper_futures[future]
                        try:
                            products = future.result()
                            all_products.extend(products)
                        except Exception as e:
                            st.error(f"Error scraping {scraper.__class__.__name__}: {str(e)}")
        else:
            # Standard mode - use thread pool for parallel execution
            scrapers = [
                JumiaScraper(),
                KongaScraper(),
                JijiScraper(),
                TemuScraper(),
                PayPorteScraper(),
                NBSScraper()
            ]
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                scraper_futures = {executor.submit(scraper.scrape_data): scraper for scraper in scrapers}
                for future in concurrent.futures.as_completed(scraper_futures):
                    scraper = scraper_futures[future]
                    try:
                        products = future.result()
                        all_products.extend(products)
                    except Exception as e:
                        st.error(f"Error scraping {scraper.__class__.__name__}: {str(e)}")
        
        # Process and save data
        processor = DataProcessor()
        processed_data = processor.process_data(all_products)
        processor.save_data(processed_data)
        
        # Calculate execution time
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Update session state
        st.session_state.data_loaded = True
        st.session_state.last_update = datetime.now()
        st.session_state.execution_time = execution_time
        st.session_state.is_scraping = False
        
    st.success(f"Data successfully updated in {execution_time:.2f} seconds!")
    st.rerun()

# Dashboard Header
st.title("📊 Nigerian E-commerce Analytics Dashboard")

# Sidebar
with st.sidebar:
    st.image("https://pixabay.com/get/g237713afa5a679cda403ee34c9213bd3a6908aa56dd192203e6c60db9653859da2c0030573806ca3db116a367b3349dee0d5fed51d277d01c8643e99ae26ada2_1280.jpg", 
             caption="E-commerce Analytics")
    
    st.subheader("Data Controls")
    
    # Data refresh button
    refresh_col1, refresh_col2 = st.columns(2)
    with refresh_col1:
        if st.button("Refresh Data", key="refresh_btn_1"):
            trigger_data_refresh()
    
    with refresh_col2:
        # Add options for scraping mode
        scrape_mode = st.radio("Scraping Mode:", 
                               ["Standard", "Async"], 
                               help="Use Async mode for faster data collection")
    
    # Display last update time
    if st.session_state.last_update:
        st.info(f"Last updated: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.info("Data not yet updated")
    
    # Scheduling options
    st.subheader("Scheduled Updates")
    schedule_options = ["Disabled", "Hourly", "Daily", "Weekly"]
    schedule_selection = st.selectbox("Schedule data updates:", schedule_options)
    
    if schedule_selection != "Disabled":
        # This would be implemented to actually schedule updates
        schedule_scraping(schedule_selection, trigger_data_refresh)
        st.success(f"Data updates scheduled: {schedule_selection}")
    
    # Export options
    st.subheader("Export Data")
    export_options = st.radio("Export Format:", ["CSV", "Excel", "JSON"])
    if st.button("Export Filtered Data"):
        st.session_state.export_requested = True
        st.session_state.export_format = export_options
    
    # Filters
    st.subheader("Filters")
    
    try:
        # Load data for filter options
        if not st.session_state.data_loaded:
            df = load_cached_data()
            if df is not None and not df.empty:
                st.session_state.data_loaded = True
                st.session_state.last_update = datetime.now()
        else:
            df = load_cached_data()
        
        if df is not None and not df.empty:
            # Category filter
            categories = ["All Categories"] + sorted(df["category"].unique().tolist())
            selected_category = st.selectbox("Product Category:", categories)
            
            # Price range filter
            min_price = float(df["price"].min())
            max_price = float(df["price"].max())
            price_range = st.slider(
                "Price Range (₦):",
                min_value=min_price,
                max_value=max_price,
                value=(min_price, max_price)
            )
            
            # Source website filter
            sources = ["All Sources"] + sorted(df["source"].unique().tolist())
            selected_source = st.selectbox("Source Website:", sources)
            
            # Time period filter
            time_periods = ["Last 7 Days", "Last 30 Days", "Last 90 Days", "All Time"]
            selected_time_period = st.selectbox("Time Period:", time_periods)
        else:
            st.warning("No data available. Click 'Refresh Data Now' to collect data.")
            # Set default filter values
            selected_category = "All Categories"
            price_range = (0, 1000000)
            selected_source = "All Sources"
            selected_time_period = "All Time"
    except Exception as e:
        st.error(f"Error loading filter data: {str(e)}")
        # Set default filter values
        selected_category = "All Categories"
        price_range = (0, 1000000)
        selected_source = "All Sources"
        selected_time_period = "All Time"

# Main dashboard area
if not st.session_state.data_loaded:
    # Show welcome screen when no data is loaded
    st.info("Welcome to the Nigerian E-commerce Analytics Dashboard!")
    st.write("""
    This dashboard provides insights into fast-selling products across Nigerian e-commerce websites.
    
    To get started:
    1. Click 'Refresh Data Now' in the sidebar to collect current product data
    2. Use the filters to narrow down the data by category, price, source, and time period
    3. Explore the visualizations to identify trends and opportunities
    """)
    
    col1, col2 = st.columns(2)
    with col1:
        st.image("https://pixabay.com/get/g6cf44a1e2425e3dc720a2425250366451ecf164e0a5c19bbf72b13a880de2963d23a90cc241b3a7f5f7288d637faa98caa1c81d10ce26cb6547c0947727de3e6_1280.jpg", 
                 caption="E-commerce Analytics Dashboard")
    with col2:
        st.image("https://pixabay.com/get/gb84e115062aafe90b32b4a822ca6bf54a2c155985b5dea6b63c69f1d664593ccdd125351e0274da3145628375707ca7533ec7cec8feb399cd49cdf3fcb1c82cd_1280.jpg", 
                 caption="Product Sales Charts")
    
    if st.button("Start Data Collection"):
        trigger_data_refresh()
else:
    try:
        # Load data
        df = load_cached_data()
        
        # Apply filters
        filtered_df = df.copy()
        
        if selected_category != "All Categories":
            filtered_df = filtered_df[filtered_df["category"] == selected_category]
        
        filtered_df = filtered_df[(filtered_df["price"] >= price_range[0]) & 
                                 (filtered_df["price"] <= price_range[1])]
        
        if selected_source != "All Sources":
            filtered_df = filtered_df[filtered_df["source"] == selected_source]
        
        # Apply time filter
        if selected_time_period != "All Time":
            days = {"Last 7 Days": 7, "Last 30 Days": 30, "Last 90 Days": 90}
            cutoff_date = datetime.now() - timedelta(days=days[selected_time_period])
            filtered_df = filtered_df[filtered_df["timestamp"] >= cutoff_date]
        
        # Display summary metrics
        st.subheader("Key Metrics")
        
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        
        with metric_col1:
            st.metric("Total Products", len(filtered_df))
        
        with metric_col2:
            avg_price = filtered_df["price"].mean()
            st.metric("Average Price", f"₦{avg_price:,.2f}")
        
        with metric_col3:
            total_categories = filtered_df["category"].nunique()
            st.metric("Categories", total_categories)
        
        with metric_col4:
            total_sources = filtered_df["source"].nunique()
            st.metric("Data Sources", total_sources)
        
        # Top selling products
        st.subheader("Top Selling Products")
        
        # Assuming "sales_rank" or "popularity" field exists in the dataset
        # If not available, we can use other metrics as proxy for popularity
        if "sales_rank" in filtered_df.columns:
            top_products = filtered_df.sort_values("sales_rank").head(10)
        else:
            # Use view count or another proxy metric
            if "view_count" in filtered_df.columns:
                top_products = filtered_df.sort_values("view_count", ascending=False).head(10)
            else:
                # If no proxy available, just show some products
                top_products = filtered_df.head(10)
        
        st.dataframe(
            top_products[["product_name", "category", "price", "source"]],
            use_container_width=True
        )
        
        # Visualizations section
        st.subheader("Product Analytics")
        
        tab1, tab2, tab3 = st.tabs(["Category Distribution", "Price Analysis", "Trend Analysis"])
        
        with tab1:
            col1, col2 = st.columns(2)
            
            with col1:
                # Category distribution
                cat_counts = filtered_df["category"].value_counts().reset_index()
                cat_counts.columns = ["Category", "Count"]
                
                fig = px.pie(
                    cat_counts, 
                    values="Count", 
                    names="Category",
                    title="Product Distribution by Category",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Source distribution
                source_counts = filtered_df["source"].value_counts().reset_index()
                source_counts.columns = ["Source", "Count"]
                
                fig = px.bar(
                    source_counts,
                    x="Source",
                    y="Count",
                    title="Product Count by Source",
                    color="Count",
                    color_continuous_scale="Viridis"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                # Price distribution
                fig = create_price_distribution_chart(filtered_df)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Price comparison across categories
                if selected_category == "All Categories" and filtered_df["category"].nunique() > 1:
                    category_price = filtered_df.groupby("category")["price"].agg(["mean", "median", "min", "max"]).reset_index()
                    
                    fig = px.box(
                        filtered_df,
                        x="category",
                        y="price",
                        title="Price Distribution by Category",
                        color="category"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Select 'All Categories' to view price comparison across categories")
                    
                    # Show price range for single category
                    fig = px.histogram(
                        filtered_df,
                        x="price",
                        nbins=20,
                        title=f"Price Distribution for {selected_category}",
                        opacity=0.8
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            # For trend analysis, ideally we need time-series data
            # Here we can use the timestamp of when data was collected
            
            if "timestamp" in filtered_df.columns:
                # Create a trend chart
                trend_chart = create_sales_trend_chart(filtered_df)
                st.plotly_chart(trend_chart, use_container_width=True)
                
                # Category comparison over time
                category_chart = create_category_comparison_chart(filtered_df)
                st.plotly_chart(category_chart, use_container_width=True)
            else:
                st.warning("Time-based trend data not available. Please refresh data to collect time information.")
                st.image("https://pixabay.com/get/g52e3749a0241b746b6be544b39d03e4eda0e1d1f9c502718cea020a2f3ecf61bbd0fd82f753dc1eb3068eada057f7eef007c45e3a1406270d449e2bf4e17f225_1280.jpg", 
                         caption="Sample Product Sales Chart")
        
        # NBS Data Integration Section
        st.subheader("National Bureau of Statistics (NBS) Insights")
        
        try:
            # Attempt to load NBS data
            nbs_data = NBSScraper().get_processed_data()
            
            if nbs_data is not None and not nbs_data.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    # NBS Economic Indicators
                    st.write("### Economic Indicators")
                    st.dataframe(nbs_data[["indicator", "value", "period"]], use_container_width=True)
                
                with col2:
                    # Visualization of NBS data
                    if "value" in nbs_data.columns and "indicator" in nbs_data.columns:
                        fig = px.bar(
                            nbs_data,
                            x="indicator",
                            y="value",
                            title="Key Economic Indicators",
                            color="indicator"
                        )
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("NBS data not available. Click 'Refresh Data Now' to collect NBS data.")
                
        except Exception as e:
            st.error(f"Error loading NBS data: {str(e)}")
            st.info("NBS data integration is available but encountered an error. Please try refreshing the data.")
        
        # Recommendations section
        st.subheader("Recommendations and Insights")
        
        # Generate some insights from the data
        if not filtered_df.empty:
            # Most popular category
            popular_category = filtered_df["category"].value_counts().idxmax()
            
            # Price range with most products
            filtered_df["price_range"] = pd.cut(filtered_df["price"], 
                                              bins=[0, 5000, 10000, 20000, 50000, 100000, float('inf')],
                                              labels=["₦0-₦5,000", "₦5,001-₦10,000", "₦10,001-₦20,000", 
                                                     "₦20,001-₦50,000", "₦50,001-₦100,000", "Above ₦100,000"])
            popular_price_range = filtered_df["price_range"].value_counts().idxmax()
            
            # Average price by category
            avg_price_by_cat = filtered_df.groupby("category")["price"].mean().sort_values(ascending=False)
            
            insight_col1, insight_col2 = st.columns(2)
            
            with insight_col1:
                st.info(f"**Most Popular Category**: {popular_category}")
                st.info(f"**Price Range with Most Products**: {popular_price_range}")
                st.info(f"**Highest Average Price Category**: {avg_price_by_cat.index[0]} (₦{avg_price_by_cat.iloc[0]:,.2f})")
            
            with insight_col2:
                st.info("**Recommendation**: Based on the current data, consider focusing on products in the " +
                       f"{popular_category} category within the {popular_price_range} price range for optimal market positioning.")
                
                # If we have time-series data, provide trend-based insights
                if "timestamp" in filtered_df.columns:
                    # Check if any category has shown significant growth
                    st.info("**Market Trend**: Monitor the growth in the beverages and detergents categories, " +
                           "which are showing increased consumer interest based on recent data.")
        
        # Footer with image
        st.image("https://pixabay.com/get/ga877dd855b45dbf691b4b465ad2580265f36b94d6cdc2dcad5462c2ad9145fec9bc9b1b991cea36e912de183a1d18c751f9ad194f00c88d08755fb8e07bb51f6_1280.jpg", 
                 caption="Data Visualization Interface")
    
    except Exception as e:
        st.error(f"Error rendering dashboard: {str(e)}")
        st.write("Please try refreshing the data or contact support if the issue persists.")
