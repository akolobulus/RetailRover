"""
Geographic insights module for Nigerian e-commerce analytics.
This module provides location-based analysis of e-commerce data.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("GeoInsights")

# Nigerian states and major cities
NIGERIAN_STATES = [
    'Abia', 'Adamawa', 'Akwa Ibom', 'Anambra', 'Bauchi', 'Bayelsa', 'Benue', 'Borno',
    'Cross River', 'Delta', 'Ebonyi', 'Edo', 'Ekiti', 'Enugu', 'FCT', 'Gombe', 'Imo',
    'Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Kogi', 'Kwara', 'Lagos', 'Nasarawa',
    'Niger', 'Ogun', 'Ondo', 'Osun', 'Oyo', 'Plateau', 'Rivers', 'Sokoto', 'Taraba',
    'Yobe', 'Zamfara'
]

# Major cities mapped to their states
MAJOR_CITIES = {
    'Lagos': 'Lagos',
    'Ikeja': 'Lagos',
    'Lekki': 'Lagos',
    'Victoria Island': 'Lagos',
    'Ikoyi': 'Lagos',
    'Yaba': 'Lagos',
    'Surulere': 'Lagos',
    'Ajah': 'Lagos',
    'Kano': 'Kano',
    'Ibadan': 'Oyo',
    'Abuja': 'FCT',
    'Port Harcourt': 'Rivers',
    'Benin City': 'Edo',
    'Kaduna': 'Kaduna',
    'Enugu': 'Enugu',
    'Aba': 'Abia',
    'Onitsha': 'Anambra',
    'Warri': 'Delta',
    'Maiduguri': 'Borno',
    'Zaria': 'Kaduna',
    'Jos': 'Plateau',
    'Ilorin': 'Kwara',
    'Abeokuta': 'Ogun',
    'Calabar': 'Cross River',
    'Akure': 'Ondo',
    'Bauchi': 'Bauchi',
    'Asaba': 'Delta',
    'Osogbo': 'Osun',
    'Sokoto': 'Sokoto',
    'Uyo': 'Akwa Ibom',
    'Makurdi': 'Benue',
    'Owerri': 'Imo',
    'Yola': 'Adamawa',
    'Gombe': 'Gombe',
    'Umuahia': 'Abia',
    'Ado Ekiti': 'Ekiti',
    'Minna': 'Niger',
    'Lafia': 'Nasarawa',
    'Dutse': 'Jigawa',
    'Ikot Ekpene': 'Akwa Ibom',
    'Ogbomosho': 'Oyo',
    'Ilesha': 'Osun',
    'Ife': 'Osun',
    'Abakaliki': 'Ebonyi',
    'Birnin Kebbi': 'Kebbi',
    'Lokoja': 'Kogi',
    'Damaturu': 'Yobe',
    'Jalingo': 'Taraba',
    'Gusau': 'Zamfara'
}

# Region definitions
REGIONS = {
    'North Central': ['Benue', 'FCT', 'Kogi', 'Kwara', 'Nasarawa', 'Niger', 'Plateau'],
    'North East': ['Adamawa', 'Bauchi', 'Borno', 'Gombe', 'Taraba', 'Yobe'],
    'North West': ['Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Sokoto', 'Zamfara'],
    'South East': ['Abia', 'Anambra', 'Ebonyi', 'Enugu', 'Imo'],
    'South South': ['Akwa Ibom', 'Bayelsa', 'Cross River', 'Delta', 'Edo', 'Rivers'],
    'South West': ['Ekiti', 'Lagos', 'Ogun', 'Ondo', 'Osun', 'Oyo']
}

# State to region mapping
STATE_TO_REGION = {}
for region, states in REGIONS.items():
    for state in states:
        STATE_TO_REGION[state] = region

class GeoInsights:
    """
    Geographic insights analysis for Nigerian e-commerce.
    Provides location-based analytics and mapping capabilities.
    """
    
    def __init__(self, order_data=None):
        """
        Initialize the geographic insights engine.
        
        Args:
            order_data (DataFrame, optional): Order data with location information
        """
        self.order_data = order_data
        logger.info("Geographic insights engine initialized")
        
    def load_order_data(self, order_df):
        """
        Load order data for geographic analysis.
        
        Args:
            order_df (DataFrame): Order data with location information
        """
        self.order_data = order_df
        logger.info(f"Loaded {len(order_df)} orders for geographic analysis")
        
    def normalize_location(self, location_text):
        """
        Normalize location text to standard state/region format.
        
        Args:
            location_text (str): Raw location text
            
        Returns:
            dict: Normalized location data
        """
        if not location_text or not isinstance(location_text, str):
            return {'city': None, 'state': None, 'region': None}
            
        # Normalize text
        location_text = location_text.strip().title()
        
        # Try to identify city
        city = None
        for known_city in MAJOR_CITIES:
            if known_city in location_text:
                city = known_city
                break
                
        # Get state from city or direct match
        state = None
        if city and city in MAJOR_CITIES:
            state = MAJOR_CITIES[city]
        else:
            for known_state in NIGERIAN_STATES:
                if known_state in location_text:
                    state = known_state
                    break
        
        # Get region from state
        region = STATE_TO_REGION.get(state) if state else None
        
        return {
            'city': city,
            'state': state,
            'region': region
        }
        
    def enrich_location_data(self, df, location_column='location'):
        """
        Enrich dataset with normalized location data.
        
        Args:
            df (DataFrame): DataFrame with location data
            location_column (str): Column name containing location information
            
        Returns:
            DataFrame: Enriched DataFrame with location details
        """
        if not isinstance(df, pd.DataFrame) or location_column not in df.columns:
            logger.warning(f"DataFrame missing required column: {location_column}")
            return df
            
        # Create a copy to avoid modifying the original
        enriched_df = df.copy()
        
        # Add location columns if not present
        if 'city' not in enriched_df.columns:
            enriched_df['city'] = None
        if 'state' not in enriched_df.columns:
            enriched_df['state'] = None
        if 'region' not in enriched_df.columns:
            enriched_df['region'] = None
            
        # Normalize locations
        location_data = enriched_df[location_column].apply(self.normalize_location)
        
        # Extract normalized location components
        enriched_df['city'] = location_data.apply(lambda x: x.get('city'))
        enriched_df['state'] = location_data.apply(lambda x: x.get('state'))
        enriched_df['region'] = location_data.apply(lambda x: x.get('region'))
        
        logger.info(f"Enriched {len(enriched_df)} records with location data")
        return enriched_df
        
    def get_regional_distribution(self, df=None, value_column=None):
        """
        Get distribution of orders or sales by region.
        
        Args:
            df (DataFrame, optional): Order data (uses stored data if None)
            value_column (str, optional): Column to sum for values (uses count if None)
            
        Returns:
            DataFrame: Distribution by region
        """
        if df is None:
            df = self.order_data
            
        if df is None or not isinstance(df, pd.DataFrame):
            logger.warning("No data available for regional distribution analysis")
            return None
            
        # Ensure region data is available
        if 'region' not in df.columns:
            logger.warning("DataFrame missing required 'region' column")
            if 'location' in df.columns:
                df = self.enrich_location_data(df)
            else:
                return None
                
        # Group by region
        if value_column and value_column in df.columns:
            # Sum values by region
            regional_data = df.groupby('region')[value_column].sum().reset_index()
            regional_data.columns = ['region', 'value']
        else:
            # Count by region
            regional_data = df.groupby('region').size().reset_index()
            regional_data.columns = ['region', 'value']
            
        # Sort by value
        regional_data = regional_data.sort_values('value', ascending=False)
        
        # Calculate percentages
        total = regional_data['value'].sum()
        if total > 0:
            regional_data['percentage'] = (regional_data['value'] / total) * 100
            
        return regional_data
        
    def get_state_distribution(self, df=None, value_column=None):
        """
        Get distribution of orders or sales by state.
        
        Args:
            df (DataFrame, optional): Order data (uses stored data if None)
            value_column (str, optional): Column to sum for values (uses count if None)
            
        Returns:
            DataFrame: Distribution by state
        """
        if df is None:
            df = self.order_data
            
        if df is None or not isinstance(df, pd.DataFrame):
            logger.warning("No data available for state distribution analysis")
            return None
            
        # Ensure state data is available
        if 'state' not in df.columns:
            logger.warning("DataFrame missing required 'state' column")
            if 'location' in df.columns:
                df = self.enrich_location_data(df)
            else:
                return None
                
        # Group by state
        if value_column and value_column in df.columns:
            # Sum values by state
            state_data = df.groupby('state')[value_column].sum().reset_index()
            state_data.columns = ['state', 'value']
        else:
            # Count by state
            state_data = df.groupby('state').size().reset_index()
            state_data.columns = ['state', 'value']
            
        # Sort by value
        state_data = state_data.sort_values('value', ascending=False)
        
        # Calculate percentages
        total = state_data['value'].sum()
        if total > 0:
            state_data['percentage'] = (state_data['value'] / total) * 100
            
        return state_data
        
    def get_top_cities(self, df=None, value_column=None, top_n=10):
        """
        Get top cities by orders or sales.
        
        Args:
            df (DataFrame, optional): Order data (uses stored data if None)
            value_column (str, optional): Column to sum for values (uses count if None)
            top_n (int): Number of top cities to return
            
        Returns:
            DataFrame: Top cities by value
        """
        if df is None:
            df = self.order_data
            
        if df is None or not isinstance(df, pd.DataFrame):
            logger.warning("No data available for city analysis")
            return None
            
        # Ensure city data is available
        if 'city' not in df.columns:
            logger.warning("DataFrame missing required 'city' column")
            if 'location' in df.columns:
                df = self.enrich_location_data(df)
            else:
                return None
                
        # Remove rows with null cities
        df = df[df['city'].notna()]
        
        # Group by city
        if value_column and value_column in df.columns:
            # Sum values by city
            city_data = df.groupby('city')[value_column].sum().reset_index()
            city_data.columns = ['city', 'value']
        else:
            # Count by city
            city_data = df.groupby('city').size().reset_index()
            city_data.columns = ['city', 'value']
            
        # Sort by value and get top N
        city_data = city_data.sort_values('value', ascending=False).head(top_n)
        
        # Add state information
        city_data['state'] = city_data['city'].map(MAJOR_CITIES)
        
        return city_data
        
    def get_category_by_region(self, df=None, category_column='category'):
        """
        Analyze category popularity by region.
        
        Args:
            df (DataFrame, optional): Order data (uses stored data if None)
            category_column (str): Column containing category information
            
        Returns:
            dict: Category popularity by region
        """
        if df is None:
            df = self.order_data
            
        if df is None or not isinstance(df, pd.DataFrame):
            logger.warning("No data available for category analysis")
            return {}
            
        # Ensure required columns are available
        if category_column not in df.columns:
            logger.warning(f"DataFrame missing required column: {category_column}")
            return {}
            
        if 'region' not in df.columns:
            logger.warning("DataFrame missing required 'region' column")
            if 'location' in df.columns:
                df = self.enrich_location_data(df)
            else:
                return {}
                
        # Group by region and category
        category_region = df.groupby(['region', category_column]).size().reset_index()
        category_region.columns = ['region', 'category', 'count']
        
        # Find top category for each region
        results = {}
        
        for region in category_region['region'].unique():
            if pd.isna(region):
                continue
                
            region_data = category_region[category_region['region'] == region]
            
            if len(region_data) == 0:
                continue
                
            # Sort by count and get top categories
            top_categories = region_data.sort_values('count', ascending=False)
            
            # Store in results
            results[region] = top_categories[['category', 'count']].to_dict('records')
            
        return results
        
    def get_delivery_times_by_region(self, df=None, order_date_col='order_date', 
                                   delivery_date_col='delivery_date'):
        """
        Analyze delivery times by region.
        
        Args:
            df (DataFrame, optional): Order data (uses stored data if None)
            order_date_col (str): Column containing order date
            delivery_date_col (str): Column containing delivery date
            
        Returns:
            DataFrame: Average delivery times by region
        """
        if df is None:
            df = self.order_data
            
        if df is None or not isinstance(df, pd.DataFrame):
            logger.warning("No data available for delivery time analysis")
            return None
            
        # Ensure required columns are available
        required_cols = [order_date_col, delivery_date_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.warning(f"DataFrame missing required columns: {missing_cols}")
            return None
            
        if 'region' not in df.columns:
            logger.warning("DataFrame missing required 'region' column")
            if 'location' in df.columns:
                df = self.enrich_location_data(df)
            else:
                return None
                
        # Create a copy to avoid modifying the original
        analysis_df = df.copy()
        
        # Ensure dates are datetime type
        for col in [order_date_col, delivery_date_col]:
            if analysis_df[col].dtype != 'datetime64[ns]':
                analysis_df[col] = pd.to_datetime(analysis_df[col], errors='coerce')
                
        # Calculate delivery time in days
        analysis_df['delivery_days'] = (analysis_df[delivery_date_col] - 
                                      analysis_df[order_date_col]).dt.total_seconds() / (24 * 3600)
        
        # Remove invalid delivery times (negative or extremely high)
        analysis_df = analysis_df[(analysis_df['delivery_days'] >= 0) & 
                                (analysis_df['delivery_days'] <= 30)]
        
        # Group by region
        region_delivery = analysis_df.groupby('region')['delivery_days'].agg(
            ['mean', 'median', 'std', 'min', 'max', 'count']
        ).reset_index()
        
        # Sort by average delivery time
        region_delivery = region_delivery.sort_values('mean')
        
        return region_delivery
    
    def generate_choropleth_data(self, metric='order_count'):
        """
        Generate data for a choropleth map of Nigeria.
        
        Args:
            metric (str): Metric to visualize ('order_count', 'sales', 'delivery_time')
            
        Returns:
            dict: Data for choropleth map
        """
        # Example data structure for choropleth map
        if self.order_data is None:
            return {
                'type': 'choropleth',
                'locations': NIGERIAN_STATES,
                'z': [0] * len(NIGERIAN_STATES),
                'text': NIGERIAN_STATES,
                'colorscale': 'Viridis',
                'marker': {'line': {'color': 'rgb(180,180,180)', 'width': 0.5}},
                'colorbar': {'title': metric.replace('_', ' ').title()}
            }
            
        # Get appropriate distribution data
        if metric == 'delivery_time':
            distribution = self.get_delivery_times_by_region()
            if distribution is not None:
                # Convert to dictionary mapping state to delivery time
                state_values = {}
                for _, row in distribution.iterrows():
                    region = row['region']
                    value = row['mean']
                    
                    # Assign to all states in the region
                    for state in REGIONS.get(region, []):
                        state_values[state] = value
            else:
                return None
        else:
            if metric == 'sales' and 'order_value' in self.order_data.columns:
                value_column = 'order_value'
            else:
                value_column = None
                
            distribution = self.get_state_distribution(value_column=value_column)
            if distribution is not None:
                # Convert to dictionary mapping state to value
                state_values = {row['state']: row['value'] 
                           for _, row in distribution.iterrows() if not pd.isna(row['state'])}
            else:
                return None
                
        # Create arrays for choropleth
        locations = []
        values = []
        text = []
        
        for state in NIGERIAN_STATES:
            locations.append(state)
            value = state_values.get(state, 0)
            values.append(value)
            text.append(f"{state}: {value:.1f}")
            
        return {
            'type': 'choropleth',
            'locations': locations,
            'z': values,
            'text': text,
            'colorscale': 'Viridis',
            'marker': {'line': {'color': 'rgb(180,180,180)', 'width': 0.5}},
            'colorbar': {'title': metric.replace('_', ' ').title()}
        }