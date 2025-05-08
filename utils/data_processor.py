import pandas as pd
import numpy as np
from datetime import datetime
import re
import os
import json
import logging

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
            
            # Add sales_rank proxy
            df['sales_rank'] = df.apply(self.compute_sales_rank, axis=1)
            
            # Add timestamp if not present
            if 'timestamp' not in df.columns:
                df['timestamp'] = datetime.now()
            
            # Add view_count proxy
            df['view_count'] = np.random.randint(10, 1000, size=len(df))
            
            # Handle missing values
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].fillna('')
                else:
                    df[col] = df[col].fillna(0)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing product data: {str(e)}")
            return pd.DataFrame()
    
    def save_data(self, df):
        """
        Save processed data to CSV.
        
        Args:
            df (DataFrame): Processed data
            
        Returns:
            bool: Success status
        """
        if df is None or df.empty:
            self.logger.warning("No data to save")
            return False
        
        try:
            # Save to CSV
            df.to_csv(self.processed_file, index=False)
            self.logger.info(f"Data saved to {self.processed_file}")
            
            # Save sample to JSON for debugging
            sample_file = os.path.join(self.data_dir, "sample_products.json")
            sample = df.head(10).to_dict('records')
            
            with open(sample_file, 'w') as f:
                json.dump(sample, f, default=str, indent=2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving data: {str(e)}")
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
