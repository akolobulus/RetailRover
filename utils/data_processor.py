import pandas as pd
import numpy as np
from datetime import datetime
import re
import os
import json
import logging
from fuzzywuzzy import fuzz
from collections import defaultdict

class DataProcessor:
    """
    Process and clean data from e-commerce websites.
    """
    
    def __init__(self):
        """
        Initialize the data processor.
        """
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Define data directories
        self.data_dir = "data"
        self.processed_file = os.path.join(self.data_dir, "processed_products.csv")
        self.historical_file = os.path.join(self.data_dir, "historical_products.csv")
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Define category mappings
        self.category_mappings = {
            # Beverages
            r"(tea|coffee|cocoa|hot\s+chocolate|milo|bournvita|ovaltine)": "beverages",
            r"(water|mineral\s+water|bottled\s+water|table\s+water)": "beverages",
            r"(energy\s+drink|milk|chocolate\s+drink)": "beverages",
            
            # Soft Drinks
            r"(soda|cola|pepsi|coca[\s-]*cola|fanta|sprite|7up|seven\s*up|coke)": "soft-drinks",
            r"(soft\s+drink|carbonated)": "soft-drinks",
            r"(mirinda|mountain\s+dew|teem|schweppes)": "soft-drinks",
            
            # Detergents
            r"(detergent|soap|washing\s+powder)": "detergents",
            r"(bleach|stain\s+remover|fabric\s+cleaner)": "detergents",
            r"(laundry|cleaning\s+agent|dishwashing)": "detergents",
            r"(omo|ariel|sunlight|surf)": "detergents",
            
            # Snacks
            r"(biscuit|cookie|wafer|chips|crisps)": "snacks",
            r"(snack|chocolate|candy|sweet|gum)": "snacks",
            r"(popcorn|pringles|digestive)": "snacks",
            
            # Personal Care
            r"(soap|body\s+wash|shower\s+gel)": "personal-care",
            r"(toothpaste|toothbrush|dental|floss)": "personal-care",
            r"(shampoo|conditioner|hair\s+care)": "personal-care",
            r"(deodorant|antiperspirant|body\s+spray)": "personal-care",
            r"(lotion|cream|moisturizer|sunscreen)": "personal-care",
            
            # Food
            r"(rice|beans|pasta|noodles|spaghetti)": "food",
            r"(flour|sugar|salt|seasoning|spice)": "food",
            r"(oil|cooking\s+oil|vegetable\s+oil|palm\s+oil)": "food",
            r"(bread|cereal|breakfast)": "food",
            r"(canned|tinned|sardine|tuna|tomato\s+paste)": "food",
            
            # Default
            r".*": "other"
        }
        
        # Units normalization patterns
        self.unit_patterns = {
            'volume': {
                'ml': r'(\d+\.?\d*)\s*ml|milliliter|millilitre',
                'l': r'(\d+\.?\d*)\s*l|liter|litre|ltr',
                'cl': r'(\d+\.?\d*)\s*cl|centiliter|centilitre',
            },
            'weight': {
                'g': r'(\d+\.?\d*)\s*g|gram|gm',
                'kg': r'(\d+\.?\d*)\s*kg|kilo|kilogram',
            },
            'quantity': {
                'pack': r'(\d+)\s*pack|pk',
                'piece': r'(\d+)\s*pc|piece|pcs',
            }
        }
    
    def categorize_product(self, product_name, original_category=None):
        """
        Assign a standardized category to a product based on its name.
        
        Args:
            product_name (str): Name of the product
            original_category (str, optional): Original category from the source
            
        Returns:
            str: Standardized category
        """
        if not product_name:
            return "other"
        
        product_name = product_name.lower()
        
        # Try to match product name against category patterns
        for pattern, category in self.category_mappings.items():
            if re.search(pattern, product_name, re.IGNORECASE):
                return category
        
        # If no match and original category is provided, try to map that
        if original_category:
            original_category = original_category.lower()
            for pattern, category in self.category_mappings.items():
                if re.search(pattern, original_category, re.IGNORECASE):
                    return category
        
        return "other"
    
    def normalize_price(self, price):
        """
        Normalize price values.
        
        Args:
            price: Original price value
            
        Returns:
            float: Normalized price
        """
        if pd.isna(price) or price == 0:
            return 0.0
        
        try:
            return float(price)
        except (ValueError, TypeError):
            # If conversion fails, return 0
            return 0.0
    
    def compute_sales_rank(self, row):
        """
        Compute a sales rank proxy based on available metrics.
        
        Args:
            row: DataFrame row with product data
            
        Returns:
            float: Sales rank score (higher is better)
        """
        score = 0
        
        # Check bestseller flag
        if 'is_bestseller' in row and row['is_bestseller']:
            score += 100
        
        # Check if featured
        if 'is_featured' in row and row['is_featured']:
            score += 50
        
        # Check if it has a high discount
        if 'discount_percent' in row and not pd.isna(row['discount_percent']):
            score += min(row['discount_percent'] * 0.5, 25)  # Cap at 25 points
        
        # Check rating
        if 'rating' in row and not pd.isna(row['rating']):
            score += row['rating'] * 5  # Up to 25 points for 5-star rating
        
        # Random component to avoid ties
        score += np.random.uniform(0, 10)
        
        return score
    
    def normalize_units(self, product_name):
        """
        Extract and normalize units from product name.
        
        Args:
            product_name (str): Name of the product
            
        Returns:
            dict: Normalized unit information
        """
        if not product_name or not isinstance(product_name, str):
            return {'unit_type': '', 'unit': '', 'value': 0}
            
        result = {'unit_type': '', 'unit': '', 'value': 0}
        
        # Try to find a unit match
        for unit_type, patterns in self.unit_patterns.items():
            for unit, pattern in patterns.items():
                match = re.search(pattern, product_name, re.IGNORECASE)
                if match and match.group(1):
                    try:
                        value = float(match.group(1))
                        result = {
                            'unit_type': unit_type,
                            'unit': unit,
                            'value': value
                        }
                        return result
                    except (ValueError, IndexError):
                        continue
        
        return result
        
    def deduplicate_products(self, df):
        """
        Deduplicate products using fuzzy matching to identify similar products.
        
        Args:
            df (DataFrame): DataFrame with product data
            
        Returns:
            DataFrame: Deduplicated product data
        """
        if df is None or df.empty:
            return df
            
        self.logger.info(f"Starting deduplication of {len(df)} products")
        
        # Group by category to speed up processing
        grouped = df.groupby('category')
        deduplicated_dfs = []
        
        for category, group_df in grouped:
            self.logger.info(f"Deduplicating {len(group_df)} products in {category} category")
            
            # Only deduplicate if we have multiple products
            if len(group_df) <= 1:
                deduplicated_dfs.append(group_df)
                continue
                
            # Initialize duplicate groups
            product_groups = []
            processed_indices = set()
            
            # For each product
            for idx1, row1 in group_df.iterrows():
                if idx1 in processed_indices:
                    continue
                    
                current_group = [idx1]
                processed_indices.add(idx1)
                
                # Compare with all other products
                for idx2, row2 in group_df.iterrows():
                    if idx2 in processed_indices or idx1 == idx2:
                        continue
                        
                    # Calculate similarity
                    name1 = str(row1['product_name']).lower()
                    name2 = str(row2['product_name']).lower()
                    
                    # Use fuzzy string matching
                    similarity = fuzz.ratio(name1, name2)
                    
                    # If similarity is high, consider as duplicate
                    if similarity > 80:  # Threshold for similarity
                        current_group.append(idx2)
                        processed_indices.add(idx2)
                
                if len(current_group) > 0:
                    product_groups.append(current_group)
            
            # Process each group
            deduplicated_rows = []
            
            for group in product_groups:
                if len(group) == 1:
                    # No duplicates
                    deduplicated_rows.append(group_df.loc[group[0]])
                else:
                    # Multiple similar products, keep the one with the highest sales rank
                    group_items = group_df.loc[group].sort_values('sales_rank', ascending=False)
                    deduplicated_rows.append(group_items.iloc[0])
            
            # Create a new DataFrame from deduplicated rows
            if deduplicated_rows:
                dedup_df = pd.DataFrame(deduplicated_rows)
                deduplicated_dfs.append(dedup_df)
        
        # Combine all deduplicated DataFrames
        if deduplicated_dfs:
            result = pd.concat(deduplicated_dfs, ignore_index=True)
            self.logger.info(f"Deduplication complete. Reduced from {len(df)} to {len(result)} products")
            return result
        
        return df
    
    def process_data(self, products):
        """
        Process and clean the scraped product data.
        
        Args:
            products (list): List of product dictionaries
            
        Returns:
            DataFrame: Processed data
        """
        if not products:
            self.logger.warning("No products to process")
            return pd.DataFrame()
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(products)
            
            # Process product names
            if 'product_name' in df.columns:
                df['product_name'] = df['product_name'].astype(str).str.strip()
            
            # Normalize prices
            if 'price' in df.columns:
                df['price'] = df['price'].apply(self.normalize_price)
            
            # Standardize categories
            if 'category' in df.columns:
                df['orig_category'] = df['category']
                df['category'] = df.apply(
                    lambda row: self.categorize_product(row['product_name'], row['category']), 
                    axis=1
                )
            
            # Normalize units where possible
            if 'product_name' in df.columns:
                unit_info = df['product_name'].apply(self.normalize_units)
                
                # Extract unit information
                if not unit_info.empty:
                    df['unit_type'] = unit_info.apply(lambda x: x.get('unit_type', ''))
                    df['unit'] = unit_info.apply(lambda x: x.get('unit', ''))
                    df['unit_value'] = unit_info.apply(lambda x: x.get('value', 0))
            
            # Add sales_rank proxy
            df['sales_rank'] = df.apply(self.compute_sales_rank, axis=1)
            
            # Add timestamp if not present
            if 'timestamp' not in df.columns:
                df['timestamp'] = datetime.now()
            
            # Add view_count proxy
            if 'view_count' not in df.columns:
                df['view_count'] = np.random.randint(10, 1000, size=len(df))
            
            # Handle missing values
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('')
                else:
                    df[col] = df[col].fillna(0)
            
            # Deduplicate products
            df = self.deduplicate_products(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing product data: {str(e)}")
            return pd.DataFrame()
    
    def save_data(self, df):
        """
        Save processed data to CSV and update historical data.
        
        Args:
            df (DataFrame): Processed data
            
        Returns:
            bool: Success status
        """
        if df is None or df.empty:
            self.logger.warning("No data to save")
            return False
        
        try:
            # Save current data to CSV
            df.to_csv(self.processed_file, index=False)
            self.logger.info(f"Data saved to {self.processed_file}")
            
            # Update historical data
            self.update_historical_data(df)
            
            # Save sample to JSON for debugging
            sample_file = os.path.join(self.data_dir, "sample_products.json")
            sample = df.head(10).to_dict('records')
            
            with open(sample_file, 'w') as f:
                json.dump(sample, f, default=str, indent=2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving data: {str(e)}")
            return False
    
    def update_historical_data(self, new_data):
        """
        Update the historical data file with new data.
        Maintains a record of price changes and product availability over time.
        
        Args:
            new_data (DataFrame): New data to add to historical record
            
        Returns:
            bool: Success status
        """
        try:
            # Add current timestamp if not present
            if 'timestamp' not in new_data.columns:
                new_data['timestamp'] = datetime.now()
                
            # Ensure timestamp is datetime
            if new_data['timestamp'].dtype == 'object':
                new_data['timestamp'] = pd.to_datetime(new_data['timestamp'], errors='coerce')
            
            # Load existing historical data if available
            if os.path.exists(self.historical_file):
                historical_df = pd.read_csv(self.historical_file)
                
                # Ensure timestamp is datetime
                if 'timestamp' in historical_df.columns and historical_df['timestamp'].dtype == 'object':
                    historical_df['timestamp'] = pd.to_datetime(historical_df['timestamp'], errors='coerce')
                
                # Append new data
                combined_df = pd.concat([historical_df, new_data], ignore_index=True)
                
                # Sort by timestamp
                combined_df = combined_df.sort_values('timestamp', ascending=False)
                
                # Limit historical data to a reasonable size (e.g., last 90 days)
                cutoff_date = datetime.now() - pd.Timedelta(days=90)
                combined_df = combined_df[combined_df['timestamp'] >= cutoff_date]
                
                # Save updated historical data
                combined_df.to_csv(self.historical_file, index=False)
                self.logger.info(f"Historical data updated with {len(new_data)} new records. Total: {len(combined_df)} records")
            else:
                # Create new historical file
                new_data.to_csv(self.historical_file, index=False)
                self.logger.info(f"New historical data file created with {len(new_data)} records")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating historical data: {str(e)}")
            return False
    
    def merge_data(self, new_data):
        """
        Merge new data with existing data.
        
        Args:
            new_data (DataFrame): New data to merge
            
        Returns:
            DataFrame: Merged data
        """
        if new_data is None or new_data.empty:
            self.logger.warning("No new data to merge")
            return self.load_existing_data()
        
        try:
            # Load existing data
            existing_data = self.load_existing_data()
            
            if existing_data is None or existing_data.empty:
                return new_data
            
            # Concatenate and deduplicate
            merged = pd.concat([existing_data, new_data], ignore_index=True)
            
            # Convert timestamp to datetime if it's a string
            if merged['timestamp'].dtype == 'object':
                merged['timestamp'] = pd.to_datetime(merged['timestamp'], errors='coerce')
            
            # Sort by timestamp (most recent first)
            merged = merged.sort_values('timestamp', ascending=False)
            
            # Remove duplicates, keeping the most recent version
            if 'product_name' in merged.columns and 'source' in merged.columns:
                merged = merged.drop_duplicates(subset=['product_name', 'source'], keep='first')
            
            return merged
            
        except Exception as e:
            self.logger.error(f"Error merging data: {str(e)}")
            return new_data
    
    def load_existing_data(self):
        """
        Load existing processed data.
        
        Returns:
            DataFrame: Existing data or empty DataFrame
        """
        try:
            if os.path.exists(self.processed_file):
                return pd.read_csv(self.processed_file)
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error loading existing data: {str(e)}")
            return pd.DataFrame()
