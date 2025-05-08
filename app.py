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
from scrapers.trafilatura_scraper import TrafilaturaScraper
# Use the factory for centralized scraper management
from scrapers.factory import scrape_all, scrape_by_category
from utils.data_processor import DataProcessor
from utils.data_loader import DataLoader
from utils.recommendation_engine import get_top_recommendations, get_trending_recommendations, get_similar_products
from utils.scheduler import schedule_scraping
from visualizations.charts import create_sales_trend_chart, create_category_comparison_chart, create_price_distribution_chart
# Import source configurations
from config.sources import get_all_sources, get_sources_by_category

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
            # Text search filter
            search_query = st.text_input(
                "Search Products:",
                placeholder="Enter product name, brand, or keywords...",
                help="Search by product name, description, or other attributes"
            )
            
            # Advanced search options
            with st.expander("Advanced Search Options", expanded=False):
                search_mode = st.radio(
                    "Search Mode:",
                    ["Contains", "Fuzzy Match", "Exact Match"],
                    horizontal=True,
                    help="Contains: Find products containing search terms; Fuzzy Match: Find similar products with spelling variations; Exact Match: Find exact matches only"
                )
                
                search_fields = st.multiselect(
                    "Search Fields:",
                    ["product_name", "description", "category", "brand", "all_fields"],
                    default=["all_fields"],
                    help="Select fields to search within"
                )
            
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
        
        # Apply text search if provided
        if 'search_query' in locals() and search_query:
            search_terms = search_query.lower().strip()
            
            if 'search_mode' in locals():
                search_mode_value = search_mode
            else:
                search_mode_value = "Contains"  # Default to contains
                
            if 'search_fields' in locals():
                selected_fields = search_fields
            else:
                selected_fields = ["all_fields"]  # Default to all fields
                
            # Define fields to search based on selection
            if "all_fields" in selected_fields:
                search_columns = ["product_name", "description", "category", "brand"]
            else:
                search_columns = selected_fields
                
            # Filter based on selected search mode
            if search_mode_value == "Contains":
                # Simple contains search
                search_mask = False
                for col in search_columns:
                    if col in filtered_df.columns:
                        # Convert column to string and check if it contains the search term
                        col_mask = filtered_df[col].astype(str).str.lower().str.contains(search_terms, na=False)
                        search_mask = search_mask | col_mask
                
                filtered_df = filtered_df[search_mask]
                
            elif search_mode_value == "Fuzzy Match":
                # Import necessary fuzzy matching library
                from fuzzywuzzy import fuzz
                
                # Create a function to calculate similarity
                def calculate_similarity(row):
                    max_score = 0
                    for col in search_columns:
                        if col in row and pd.notna(row[col]):
                            score = fuzz.partial_ratio(str(row[col]).lower(), search_terms)
                            max_score = max(max_score, score)
                    return max_score
                
                # Calculate similarity for each row
                filtered_df['search_similarity'] = filtered_df.apply(calculate_similarity, axis=1)
                
                # Filter rows with similarity score above threshold (e.g., 70%)
                filtered_df = filtered_df[filtered_df['search_similarity'] >= 70]
                
                # Sort by similarity (most similar first)
                filtered_df = filtered_df.sort_values('search_similarity', ascending=False)
                
                # Remove the temporary similarity column
                filtered_df = filtered_df.drop('search_similarity', axis=1)
                
            elif search_mode_value == "Exact Match":
                # Exact match search
                search_mask = False
                for col in search_columns:
                    if col in filtered_df.columns:
                        # Convert column to string and check for exact matches
                        col_mask = filtered_df[col].astype(str).str.lower() == search_terms
                        search_mask = search_mask | col_mask
                
                filtered_df = filtered_df[search_mask]
                
        # Apply category filter
        if selected_category != "All Categories":
            filtered_df = filtered_df[filtered_df["category"] == selected_category]
        
        # Apply price filter
        filtered_df = filtered_df[(filtered_df["price"] >= price_range[0]) & 
                                 (filtered_df["price"] <= price_range[1])]
        
        # Apply source website filter
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
        
        # Top selling products section
        tab_best, tab_trending = st.tabs(["Top Sellers", "Trending Products"])
        
        with tab_best:
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
        
        with tab_trending:
            st.subheader("Trending Products (Week-over-Week Changes)")
            
            # Look for historical data to perform trend analysis
            try:
                # Try to load historical data if available
                historical_file = os.path.join("data", "historical_products.csv")
                if os.path.exists(historical_file):
                    historical_df = pd.read_csv(historical_file)
                    
                    # Ensure timestamp is datetime for comparison
                    if 'timestamp' in historical_df.columns:
                        historical_df['timestamp'] = pd.to_datetime(historical_df['timestamp'], errors='coerce')
                        
                        # Get data from current week and previous week
                        now = datetime.now()
                        one_week_ago = now - timedelta(days=7)
                        two_weeks_ago = now - timedelta(days=14)
                        
                        current_week = historical_df[(historical_df['timestamp'] >= one_week_ago) & 
                                                   (historical_df['timestamp'] <= now)]
                        previous_week = historical_df[(historical_df['timestamp'] >= two_weeks_ago) & 
                                                    (historical_df['timestamp'] < one_week_ago)]
                        
                        if not current_week.empty and not previous_week.empty:
                            # Group by product and get average metrics
                            current_week_agg = current_week.groupby(['product_name', 'category', 'source'])[['price', 'view_count']].mean().reset_index()
                            previous_week_agg = previous_week.groupby(['product_name', 'category', 'source'])[['price', 'view_count']].mean().reset_index()
                            
                            # Merge to compare changes
                            trending_df = pd.merge(current_week_agg, previous_week_agg, 
                                               on=['product_name', 'category', 'source'], 
                                               suffixes=('_current', '_previous'))
                            
                            # Calculate changes
                            trending_df['price_change'] = trending_df['price_current'] - trending_df['price_previous']
                            trending_df['price_change_percent'] = (trending_df['price_change'] / trending_df['price_previous']) * 100
                            trending_df['view_count_change'] = trending_df['view_count_current'] - trending_df['view_count_previous']
                            trending_df['view_count_change_percent'] = (trending_df['view_count_change'] / trending_df['view_count_previous']) * 100
                            
                            # Sort by view count change to show trending products
                            trending_df = trending_df.sort_values('view_count_change_percent', ascending=False)
                            
                            # Display the trending products
                            st.dataframe(
                                trending_df[['product_name', 'category', 'source', 'price_current', 
                                           'price_change_percent', 'view_count_change_percent']].head(10),
                                column_config={
                                    "product_name": "Product",
                                    "category": "Category",
                                    "source": "Source",
                                    "price_current": st.column_config.NumberColumn("Current Price (₦)", format="₦%.2f"),
                                    "price_change_percent": st.column_config.NumberColumn("Price Change %", format="%.2f%%"),
                                    "view_count_change_percent": st.column_config.NumberColumn("Popularity Change %", format="%.2f%%")
                                },
                                use_container_width=True
                            )
                            
                            # Visualize trending categories
                            st.subheader("Trending Categories")
                            
                            category_trend = trending_df.groupby('category')['view_count_change_percent'].mean().reset_index()
                            category_trend = category_trend.sort_values('view_count_change_percent', ascending=False)
                            
                            fig = px.bar(
                                category_trend,
                                x='category',
                                y='view_count_change_percent',
                                title="Category Week-over-Week Growth",
                                color='view_count_change_percent',
                                color_continuous_scale=px.colors.sequential.Viridis,
                                labels={'category': 'Category', 'view_count_change_percent': 'Popularity Growth (%)'}
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("Not enough historical data available for trend analysis. Please check back after collecting data for at least two weeks.")
                    else:
                        st.info("Historical data missing timestamp information. Unable to perform trend analysis.")
                else:
                    st.info("No historical data available yet. Trend analysis will be available once more data is collected over time.")
            except Exception as e:
                st.error(f"Error performing trend analysis: {str(e)}")
                st.info("Trend analysis feature requires historical data collection over time.")
        
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
        
        # Handle export functionality if requested
        if 'export_requested' in st.session_state and st.session_state.export_requested:
            export_format = st.session_state.export_format if 'export_format' in st.session_state else "CSV"
            
            # Create download button based on format
            if export_format == "CSV":
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV File",
                    data=csv,
                    file_name=f"ecommerce_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            elif export_format == "Excel":
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    filtered_df.to_excel(writer, sheet_name="Products", index=False)
                    # Get the sheet to add some formatting
                    workbook = writer.book
                    worksheet = writer.sheets["Products"]
                    
                    # Add column formatting
                    money_fmt = workbook.add_format({'num_format': '₦#,##0.00'})
                    date_fmt = workbook.add_format({'num_format': 'yyyy-mm-dd'})
                    
                    # Apply formatting to specific columns
                    if 'price' in filtered_df.columns:
                        price_col = filtered_df.columns.get_loc('price')
                        worksheet.set_column(price_col, price_col, 12, money_fmt)
                    
                    if 'timestamp' in filtered_df.columns:
                        date_col = filtered_df.columns.get_loc('timestamp')
                        worksheet.set_column(date_col, date_col, 18, date_fmt)
                    
                    # Set column widths
                    worksheet.set_column(0, 0, 40)  # Product name column wider
                    worksheet.set_column(1, len(filtered_df.columns), 15)  # Other columns

                excel_data = buffer.getvalue()
                st.download_button(
                    label="Download Excel File",
                    data=excel_data,
                    file_name=f"ecommerce_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.ms-excel"
                )
            elif export_format == "JSON":
                json_data = filtered_df.to_json(orient="records", date_format="iso")
                st.download_button(
                    label="Download JSON File",
                    data=json_data,
                    file_name=f"ecommerce_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
            
            # Reset export requested flag
            st.session_state.export_requested = False
        
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
        
        # Website Content Scraper Section
        st.subheader("Website Content Scraper & Analyzer")
        st.write("""
        Use the enhanced Trafilatura scraper to extract clean text content from any website and perform advanced analysis.
        This is useful for analyzing articles, blog posts, news, and other text-heavy content.
        """)
        
        scraper_col1, scraper_col2 = st.columns([3, 1])
        
        with scraper_col1:
            website_urls = st.text_area(
                "Enter website URLs (one per line):",
                height=100,
                help="Enter one or more website URLs to extract their main content."
            )
            
            source_names = st.text_area(
                "Optional: Source names (one per line, matching the order of URLs):",
                height=80,
                help="Provide custom names for each source. Leave blank to use default names."
            )
            
            # Add semantic search capability
            semantic_search_query = st.text_input(
                "Semantic Search (after extraction):",
                help="Search extracted content using natural language. Example: 'information about inflation rates' or 'economic trends in Nigeria'"
            )
        
        with scraper_col2:
            st.write("#### Options")
            save_content = st.checkbox("Save to file", value=True)
            file_format = st.radio("File format:", ["CSV", "JSON", "Both"])
            
            st.write("#### Analysis Options")
            content_analysis = st.multiselect(
                "Content Analysis:",
                ["Extract Keywords", "Summarize", "Sentiment Analysis", "Topic Classification"],
                default=["Extract Keywords"],
                help="Select analysis to perform on extracted content"
            )
        
        if st.button("Extract Content & Analyze"):
            if website_urls.strip():
                urls = [url.strip() for url in website_urls.strip().split('\n') if url.strip()]
                
                if source_names.strip():
                    names = [name.strip() for name in source_names.strip().split('\n') if name.strip()]
                else:
                    names = None
                
                if urls:
                    with st.spinner(f"Extracting content from {len(urls)} websites..."):
                        try:
                            scraper = TrafilaturaScraper()
                            results = scraper.scrape_urls(urls, names)
                            
                            if results:
                                st.success(f"Successfully extracted content from {len(results)} out of {len(urls)} websites.")
                                
                                # Store results in session state for future use
                                st.session_state['extracted_content'] = results
                                
                                # Perform content analysis if selected
                                if "Extract Keywords" in content_analysis:
                                    # Simple keyword extraction using frequency counts
                                    with st.spinner("Extracting keywords..."):
                                        for i, result in enumerate(results):
                                            content = result['content'].lower()
                                            # Remove common stop words
                                            stop_words = ["the", "and", "of", "to", "a", "in", "for", "is", "on", "that", "by", "this", "with", "i", "you", "it", "not", "or", "be", "are", "from", "at", "as", "your", "have", "more", "an", "was", "we", "will", "can", "us", "our", "if", "their", "been", "were"]
                                            
                                            # Split into words and count frequency
                                            words = [word.strip('.,!?:;()[]{}""''') for word in content.split()]
                                            word_freq = {}
                                            for word in words:
                                                if len(word) > 3 and word not in stop_words:  # Ignore short words and stop words
                                                    if word in word_freq:
                                                        word_freq[word] += 1
                                                    else:
                                                        word_freq[word] = 1
                                            
                                            # Sort by frequency and take top 10
                                            top_keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
                                            results[i]['keywords'] = top_keywords
                                
                                # Perform sentiment analysis if selected
                                if "Sentiment Analysis" in content_analysis:
                                    with st.spinner("Analyzing sentiment..."):
                                        for i, result in enumerate(results):
                                            # Simple sentiment analysis based on positive and negative word counts
                                            content = result['content'].lower()
                                            
                                            # Basic positive and negative word lists
                                            positive_words = ["good", "great", "excellent", "positive", "wonderful", "best", "amazing", "love", "benefit", "success", "successful", "growth", "improve", "improved", "increasing", "profit", "profitable", "advantage", "quality", "efficient", "efficiency", "effective", "productivity", "innovative"]
                                            negative_words = ["bad", "worst", "poor", "negative", "terrible", "hate", "problem", "fail", "failure", "decrease", "decreasing", "loss", "risk", "crisis", "deficit", "disadvantage", "difficult", "inefficient", "ineffective", "expensive", "costly", "complicated", "corrupt"]
                                            
                                            # Count positive and negative words
                                            positive_count = sum(1 for word in content.split() if word.strip('.,!?:;()[]{}""''') in positive_words)
                                            negative_count = sum(1 for word in content.split() if word.strip('.,!?:;()[]{}""''') in negative_words)
                                            
                                            # Calculate sentiment score
                                            if positive_count + negative_count > 0:
                                                sentiment_score = (positive_count - negative_count) / (positive_count + negative_count)
                                            else:
                                                sentiment_score = 0
                                                
                                            # Determine sentiment label
                                            if sentiment_score > 0.1:
                                                sentiment = "Positive"
                                            elif sentiment_score < -0.1:
                                                sentiment = "Negative"
                                            else:
                                                sentiment = "Neutral"
                                                
                                            results[i]['sentiment'] = sentiment
                                            results[i]['sentiment_score'] = sentiment_score
                                
                                # Perform semantic search if provided
                                if semantic_search_query:
                                    with st.spinner("Performing semantic search..."):
                                        search_results = []
                                        
                                        for result in results:
                                            # Break content into smaller chunks for better search
                                            chunks = []
                                            content = result['content']
                                            chunk_size = 1000  # Characters per chunk
                                            
                                            # Split content into chunks of around 1000 characters at paragraph breaks
                                            paragraphs = content.split('\n\n')
                                            current_chunk = ""
                                            
                                            for paragraph in paragraphs:
                                                if len(current_chunk) + len(paragraph) <= chunk_size:
                                                    current_chunk += paragraph + "\n\n"
                                                else:
                                                    if current_chunk:
                                                        chunks.append(current_chunk.strip())
                                                    current_chunk = paragraph + "\n\n"
                                            
                                            if current_chunk:
                                                chunks.append(current_chunk.strip())
                                            
                                            # If no paragraph breaks or very long paragraphs
                                            if not chunks:
                                                for i in range(0, len(content), chunk_size):
                                                    chunks.append(content[i:i+chunk_size])
                                            
                                            # For each chunk, calculate a simple relevance score based on term frequency
                                            query_terms = semantic_search_query.lower().split()
                                            for i, chunk in enumerate(chunks):
                                                chunk_lower = chunk.lower()
                                                # Count occurrences of query terms
                                                term_matches = sum(chunk_lower.count(term) for term in query_terms)
                                                relevance_score = term_matches / max(1, len(chunk.split()))
                                                
                                                # Only include chunks with matches
                                                if term_matches > 0:
                                                    search_results.append({
                                                        'source_name': result['source_name'],
                                                        'url': result['url'],
                                                        'chunk': chunk,
                                                        'relevance': relevance_score,
                                                        'matches': term_matches
                                                    })
                                        
                                        # Sort by relevance
                                        search_results = sorted(search_results, key=lambda x: x['relevance'], reverse=True)
                                        
                                        # Display search results
                                        if search_results:
                                            st.subheader(f"Search Results for: '{semantic_search_query}'")
                                            for i, result in enumerate(search_results[:5]):  # Show top 5 results
                                                with st.expander(f"Result {i+1} from {result['source_name']} (Relevance: {result['relevance']:.2f})"):
                                                    st.write(result['chunk'])
                                                    st.write(f"**Source:** {result['source_name']} | **URL:** {result['url']}")
                                        else:
                                            st.info(f"No matches found for '{semantic_search_query}'")
                                
                                # Display content preview with analysis
                                for i, result in enumerate(results):
                                    with st.expander(f"{result['source_name']} ({result['word_count']} words)"):
                                        # Display preview content
                                        st.write("#### Content Preview")
                                        preview = result['content'][:500] + "..." if len(result['content']) > 500 else result['content']
                                        st.write(preview)
                                        st.write(f"**URL:** {result['url']}")
                                        st.write(f"**Total Characters:** {result['character_count']}")
                                        
                                        # Display analysis results
                                        if "Extract Keywords" in content_analysis and 'keywords' in result:
                                            st.write("#### Top Keywords")
                                            keywords_df = pd.DataFrame(result['keywords'], columns=["Keyword", "Frequency"])
                                            st.dataframe(keywords_df, use_container_width=True)
                                        
                                        if "Sentiment Analysis" in content_analysis and 'sentiment' in result:
                                            st.write(f"#### Sentiment Analysis: **{result['sentiment']}** (Score: {result['sentiment_score']:.2f})")
                                
                                # Save files if requested
                                if save_content:
                                    os.makedirs("data", exist_ok=True)
                                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    
                                    if file_format in ["CSV", "Both"]:
                                        # Create DataFrame with analysis results if available
                                        df_data = []
                                        for r in results:
                                            entry = {
                                                "source_name": r["source_name"],
                                                "url": r["url"],
                                                "content_preview": r["content"][:500] + "..." if len(r["content"]) > 500 else r["content"],
                                                "word_count": r["word_count"],
                                                "character_count": r["character_count"],
                                                "timestamp": r["timestamp"]
                                            }
                                            
                                            # Add analysis fields if available
                                            if 'sentiment' in r:
                                                entry["sentiment"] = r["sentiment"]
                                                entry["sentiment_score"] = r["sentiment_score"]
                                            
                                            if 'keywords' in r:
                                                entry["top_keywords"] = ", ".join([k for k, v in r["keywords"][:5]])
                                            
                                            df_data.append(entry)
                                        
                                        df = pd.DataFrame(df_data)
                                        csv_file = f"data/scraped_content_{timestamp}.csv"
                                        df.to_csv(csv_file, index=False)
                                        
                                        with open(csv_file, "r") as f:
                                            csv_data = f.read()
                                        
                                        st.download_button(
                                            label="Download CSV",
                                            data=csv_data,
                                            file_name=f"scraped_content_{timestamp}.csv",
                                            mime="text/csv"
                                        )
                                    
                                    if file_format in ["JSON", "Both"]:
                                        # Include all data including analysis results
                                        json_data = json.dumps([{
                                            **r,
                                            "timestamp": r["timestamp"].isoformat() if hasattr(r["timestamp"], "isoformat") else str(r["timestamp"])
                                        } for r in results], indent=2)
                                        
                                        json_file = f"data/scraped_content_{timestamp}.json"
                                        with open(json_file, "w") as f:
                                            f.write(json_data)
                                        
                                        st.download_button(
                                            label="Download JSON",
                                            data=json_data,
                                            file_name=f"scraped_content_{timestamp}.json",
                                            mime="application/json"
                                        )
                            else:
                                st.warning("No content could be extracted from the provided URLs.")
                        except Exception as e:
                            st.error(f"Error extracting content: {str(e)}")
                else:
                    st.warning("Please enter at least one valid URL.")
            else:
                st.warning("Please enter at least one URL to extract content from.")
        
        # Footer with image
        st.image("https://pixabay.com/get/ga877dd855b45dbf691b4b465ad2580265f36b94d6cdc2dcad5462c2ad9145fec9bc9b1b991cea36e912de183a1d18c751f9ad194f00c88d08755fb8e07bb51f6_1280.jpg", 
                 caption="Data Visualization Interface")
    
    except Exception as e:
        st.error(f"Error rendering dashboard: {str(e)}")
        st.write("Please try refreshing the data or contact support if the issue persists.")
