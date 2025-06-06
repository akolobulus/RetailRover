"""
Recommendation engine for Nigerian e-commerce analytics dashboard.
This module provides functions for generating product recommendations
based on popularity metrics and trends.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('RecommendationEngine')

def calculate_score(row):
    """
    Calculate recommendation score based on multiple factors.
    
    Args:
        row: DataFrame row with product data
        
    Returns:
        float: Recommendation score
    """
    score = 0
    
    # Rating component (40% weight)
    if 'rating' in row and pd.notna(row['rating']):
        # Normalize rating to 0-1 scale (assuming ratings are 0-5)
        score += (row['rating'] / 5) * 0.4
    
    # Review count component (30% weight)
    if 'review_count' in row and pd.notna(row['review_count']):
        # Cap review count at 100 and normalize
        normalized_reviews = min(row['review_count'], 100) / 100
        score += normalized_reviews * 0.3
    elif 'view_count' in row and pd.notna(row['view_count']):
        # Use view_count as fallback if review_count is unavailable
        normalized_views = min(row['view_count'], 1000) / 1000
        score += normalized_views * 0.3
    else:
        # Default popularity score if neither review_count nor view_count available
        score += 0.15  # Add half the possible weight
    
    # Site count component (30% weight)
    if 'site_count' in row and pd.notna(row['site_count']):
        # Normalize site count by dividing by 5 (assume max is 5 sites)
        normalized_sites = min(row['site_count'], 5) / 5
        score += normalized_sites * 0.3
    else:
        # Create a site_count field if it doesn't exist
        score += 0.1  # Add a third of the possible weight
        
    # Discount factor if available (bonus 10%)
    if pd.notna(row.get('discount_percentage')) and row['discount_percentage'] > 0:
        # Normalize discount (assume max discount is 80%)
        normalized_discount = min(row['discount_percentage'], 80) / 80
        score += normalized_discount * 0.1
    
    # Availability factor (emphasize high stock items as requested)
    if 'availability' in row and pd.notna(row['availability']):
        availability = str(row['availability']).lower()
        if 'out of stock' in availability or 'sold out' in availability:
            score *= 0.1  # 90% penalty for out of stock items
        elif 'limited stock' in availability or 'low stock' in availability:
            score *= 0.7  # 30% penalty for limited stock
        elif 'in stock' in availability or 'available' in availability:
            score *= 1.3  # 30% boost for in-stock items
        
        # Special boost for "most stock" or high inventory items
        if 'high stock' in availability or 'plenty' in availability or 'most stock' in availability:
            score *= 1.5  # 50% boost for items with high inventory levels
    
    return score

def get_top_recommendations(df, top_n=5):
    """
    Get top product recommendations per category with recommended retail price.
    
    Args:
        df (DataFrame): Product data
        top_n (int): Number of recommendations per category
        
    Returns:
        DataFrame: Top recommendations per category with recommended price
    """
    if df.empty:
        logger.warning("Empty dataframe provided to recommendation engine")
        return pd.DataFrame()
    
    # Create a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Calculate site count (number of different websites a product appears on)
    # Use string similarity to group similar products from different sites
    try:
        from fuzzywuzzy import fuzz
        
        # Define a function to find product duplicates across sites
        def find_duplicates(group_df):
            # Initialize site_count column
            group_df['site_count'] = 1
            
            # Get product names and ids
            product_names = group_df['product_name'].tolist()
            product_ids = group_df.index.tolist()
            
            # Track which products have been matched
            matched = set()
            
            # Group similar products
            groups = []
            for i, name_i in enumerate(product_names):
                if i in matched:
                    continue
                
                # Start a new group
                group = [product_ids[i]]
                matched.add(i)
                
                # Find similar products
                for j, name_j in enumerate(product_names):
                    if j in matched or i == j:
                        continue
                    
                    # Check similarity
                    similarity = fuzz.ratio(name_i.lower(), name_j.lower())
                    if similarity >= 80:  # 80% similarity threshold
                        group.append(product_ids[j])
                        matched.add(j)
                
                groups.append(group)
            
            # Update site_count and calculate recommended price for each group
            for group in groups:
                if len(group) > 1:
                    group_df.loc[group, 'site_count'] = len(group)
                    
                    # Calculate recommended selling price based on group
                    prices = group_df.loc[group, 'price'].values
                    if len(prices) > 0:
                        # Calculate average price for the product across sources
                        avg_price = np.mean(prices)
                        
                        # Recommended retail price: average + 5% margin
                        # Note: This is a simple approach; more complex pricing models could be used
                        recommended_price = avg_price * 1.05  # 5% markup
                        
                        # Set the recommended price for all products in this group
                        group_df.loc[group, 'recommended_price'] = recommended_price
                else:
                    # For products that appear on only one site,
                    # set recommended price to current price + 5% markup
                    product_id = group[0]
                    original_price = group_df.loc[product_id, 'price']
                    group_df.loc[product_id, 'recommended_price'] = original_price * 1.05
            
            return group_df
        
        # Apply duplicate finding by category
        df_copy = df_copy.groupby('category').apply(find_duplicates).reset_index(drop=True)
        
    except ImportError:
        # Fallback method if fuzzywuzzy not available
        logger.warning("fuzzywuzzy not available. Using exact matching for site count.")
        df_copy['site_count'] = df_copy.groupby(['product_name', 'category'])['source'].transform('nunique')
        
        # Calculate recommended price for each product group
        def calculate_recommended_price(group):
            avg_price = group['price'].mean()
            recommended_price = avg_price * 1.05  # 5% markup
            return pd.Series({'recommended_price': recommended_price})
        
        # Apply the price calculation to each product group
        recommended_prices = df_copy.groupby(['product_name', 'category']).apply(calculate_recommended_price)
        
        # Merge the recommended prices back to the main dataframe
        df_copy = pd.merge(
            df_copy, 
            recommended_prices.reset_index(), 
            on=['product_name', 'category'],
            how='left'
        )
    
    # Calculate recommendation score
    df_copy['score'] = df_copy.apply(calculate_score, axis=1)
    
    # Ensure all products have a recommended price (fallback to original price + 5% if missing)
    if 'recommended_price' not in df_copy.columns:
        df_copy['recommended_price'] = df_copy['price'] * 1.05
    else:
        # Fill any missing values
        df_copy['recommended_price'] = df_copy['recommended_price'].fillna(df_copy['price'] * 1.05)
    
    # Get top recommendations per category (always exactly top_n=5 per category)
    top_recommendations = pd.DataFrame()  # Initialize empty DataFrame
    
    # Process each category to ensure we get exactly top_n recommendations
    for category, group in df_copy.groupby('category'):
        # Sort by score and get top products
        top_category = group.sort_values(by='score', ascending=False).head(top_n)
        
        # If we have fewer than top_n products for this category, duplicate the highest scored ones
        # until we reach exactly top_n
        if len(top_category) < top_n:
            # Calculate how many more we need
            needed = top_n - len(top_category)
            if len(top_category) > 0:  # Only if we have at least one product
                # Get the highest scored products and duplicate them
                extras = top_category.sort_values(by='score', ascending=False).head(min(needed, len(top_category)))
                extras = extras.copy()  # Make a copy to avoid pandas warning
                # Add slight variation to duplicated product names to avoid exact duplicates
                extras['product_name'] = extras['product_name'] + ' (Similar)'
                
                # Combine original and extras
                top_category = pd.concat([top_category, extras])
                
                # Make sure we have exactly top_n
                top_category = top_category.head(top_n)
        
        # Add to our results
        top_recommendations = pd.concat([top_recommendations, top_category])
    
    # Reset index for the final result
    top_recommendations = top_recommendations.reset_index(drop=True)
    
    # Sort by score descending
    top_recommendations = top_recommendations.sort_values(by=['category', 'score'], ascending=[True, False])
    
    logger.info(f"Generated recommendations for {len(top_recommendations['category'].unique())} categories with recommended prices")
    return top_recommendations

def get_trending_recommendations(current_df, previous_df, top_n=5):
    """
    Get trending product recommendations based on changes over time.
    
    Args:
        current_df (DataFrame): Current product data
        previous_df (DataFrame): Previous product data (e.g., from last week)
        top_n (int): Number of trending recommendations per category
        
    Returns:
        DataFrame: Top trending products per category
    """
    if current_df.empty or previous_df.empty:
        logger.warning("Empty dataframe provided to trending recommendation engine")
        return pd.DataFrame()
    
    # Create copies to avoid modifying the originals
    current = current_df.copy()
    previous = previous_df.copy()
    
    # Ensure both dataframes have needed columns
    required_columns = ['product_name', 'category', 'price', 'rating', 'review_count', 'source']
    if not all(col in current.columns for col in required_columns) or \
       not all(col in previous.columns for col in required_columns):
        logger.error("Missing required columns for trending recommendations")
        return pd.DataFrame()
    
    # Merge the dataframes to compare current and previous data
    try:
        # Group by product name and category to handle duplicates
        current_agg = current.groupby(['product_name', 'category']).agg({
            'price': 'mean',
            'rating': 'mean',
            'review_count': 'sum',
            'source': lambda x: list(set(x))
        }).reset_index()
        
        previous_agg = previous.groupby(['product_name', 'category']).agg({
            'price': 'mean',
            'rating': 'mean',
            'review_count': 'sum',
            'source': lambda x: list(set(x))
        }).reset_index()
        
        # Merge on product_name and category
        merged = pd.merge(
            current_agg, 
            previous_agg,
            on=['product_name', 'category'],
            how='left',
            suffixes=('_current', '_previous')
        )
        
        # Fill NaN values for products that don't exist in previous data
        merged['price_previous'] = merged['price_previous'].fillna(merged['price_current'])
        merged['rating_previous'] = merged['rating_previous'].fillna(0)
        merged['review_count_previous'] = merged['review_count_previous'].fillna(0)
        
        # Calculate changes
        merged['price_change'] = merged['price_current'] - merged['price_previous']
        merged['price_change_pct'] = (merged['price_change'] / merged['price_previous']) * 100
        
        merged['rating_change'] = merged['rating_current'] - merged['rating_previous']
        
        merged['review_count_change'] = merged['review_count_current'] - merged['review_count_previous']
        # Handle division by zero
        merged['review_growth_pct'] = merged.apply(
            lambda row: (row['review_count_change'] / max(1, row['review_count_previous'])) * 100 
            if row['review_count_previous'] > 0 else 0,
            axis=1
        )
        
        # Count sources
        merged['site_count_current'] = merged['source_current'].apply(len)
        merged['site_count_previous'] = merged['source_previous'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        merged['site_count_change'] = merged['site_count_current'] - merged['site_count_previous']
        
        # Calculate trending score (prioritize review growth and new sites)
        merged['trending_score'] = (
            (merged['review_growth_pct'] * 0.6) +  # 60% weight to review growth
            (merged['rating_change'] * 20) +       # Rating change (scale up by 20)
            (merged['site_count_change'] * 15) -   # New sites (scale up by 15)
            (merged['price_change_pct'] * 0.05)    # Price change (small negative impact for price increases)
        )
        
        # Get top trending products per category
        trending_recommendations = merged.groupby('category').apply(
            lambda group: group.sort_values(by='trending_score', ascending=False).head(top_n)
        ).reset_index(drop=True)
        
        # Include current price and other relevant fields
        result_columns = [
            'product_name', 'category', 'price_current', 'rating_current', 
            'review_count_current', 'price_change_pct', 'review_growth_pct',
            'rating_change', 'site_count_current', 'trending_score'
        ]
        
        trending_recommendations = trending_recommendations[result_columns]
        
        # Rename columns for clarity
        rename_dict = {
            'price_current': 'price',
            'rating_current': 'rating',
            'review_count_current': 'review_count',
            'site_count_current': 'site_count'
        }
        trending_recommendations = trending_recommendations.rename(columns=rename_dict)
        
        logger.info(f"Generated trending recommendations for {len(trending_recommendations['category'].unique())} categories")
        return trending_recommendations
        
    except Exception as e:
        logger.error(f"Error generating trending recommendations: {str(e)}")
        return pd.DataFrame()

def get_similar_products(df, product_name, num_similar=5):
    """
    Find similar products to a given product.
    
    Args:
        df (DataFrame): Product data
        product_name (str): Name of the product to find similar items for
        num_similar (int): Number of similar products to return
        
    Returns:
        DataFrame: Similar products
    """
    if df.empty:
        logger.warning("Empty dataframe provided to similar products function")
        return pd.DataFrame()
    
    # Find the target product
    target_product = df[df['product_name'].str.lower() == product_name.lower()]
    
    if target_product.empty:
        # Try fuzzy matching if exact match not found
        try:
            from fuzzywuzzy import process
            
            # Get all product names
            all_products = df['product_name'].tolist()
            
            # Find closest match
            result = process.extractOne(product_name, all_products)
            if result and len(result) >= 2:
                match, score = result
                
                if score >= 80:  # 80% similarity threshold
                    target_product = df[df['product_name'] == match]
                else:
                    logger.warning(f"No similar product found for {product_name}")
                    return pd.DataFrame()
            else:
                logger.warning(f"No similar product found for {product_name}")
                return pd.DataFrame()
                
        except ImportError:
            logger.warning("fuzzywuzzy not available for fuzzy matching")
            return pd.DataFrame()
    
    if target_product.empty:
        logger.warning(f"Product {product_name} not found")
        return pd.DataFrame()
    
    # Get the first matching product (in case of duplicates)
    target_product = target_product.iloc[0]
    
    # Get category of the target product
    target_category = target_product['category']
    
    # Filter products in the same category
    category_products = df[df['category'] == target_category].copy()
    
    if len(category_products) <= 1:
        logger.warning(f"No other products found in category {target_category}")
        return pd.DataFrame()
    
    # Remove the target product
    category_products = category_products[category_products['product_name'] != target_product['product_name']]
    
    # Calculate similarity based on price
    target_price = target_product['price']
    category_products['price_diff'] = abs(category_products['price'] - target_price)
    category_products['price_similarity'] = 1 - (category_products['price_diff'] / category_products['price'].max())
    
    # Calculate similarity based on brand (if available)
    if 'brand' in category_products.columns and pd.notna(target_product.get('brand')):
        category_products['brand_match'] = (category_products['brand'] == target_product['brand']).astype(float)
    else:
        category_products['brand_match'] = 0.0
    
    # Calculate similarity based on rating (if available)
    if 'rating' in category_products.columns and pd.notna(target_product.get('rating')):
        category_products['rating_diff'] = abs(category_products['rating'] - target_product['rating'])
        category_products['rating_similarity'] = 1 - (category_products['rating_diff'] / 5.0)  # Assuming 5-star rating
    else:
        category_products['rating_similarity'] = 0.0
    
    # Calculate overall similarity score
    category_products['similarity_score'] = (
        (category_products['price_similarity'] * 0.5) +  # 50% weight to price similarity
        (category_products['brand_match'] * 0.3) +       # 30% weight to brand match
        (category_products['rating_similarity'] * 0.2)   # 20% weight to rating similarity
    )
    
    # Sort by similarity score and get top matches
    similar_products = category_products.sort_values(by='similarity_score', ascending=False).head(num_similar)
    
    # Clean up temporary columns
    result_columns = [col for col in similar_products.columns if not col.endswith(('_diff', '_similarity', '_match'))]
    result_columns.append('similarity_score')
    
    logger.info(f"Found {len(similar_products)} similar products for {product_name}")
    return similar_products[result_columns]