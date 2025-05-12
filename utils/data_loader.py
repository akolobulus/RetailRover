import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import logging

class DataLoader:
    """
    Load and prepare data for the dashboard.
    """
    
    def __init__(self):
        """
        Initialize the data loader.
        """
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Define data file paths
        self.data_dir = "data"
        self.processed_file = os.path.join(self.data_dir, "processed_products.csv")
    
    def load_data(self):
        """
        Load processed data for the dashboard.
        
        Returns:
            DataFrame: Processed data or sample data if file not found
        """
        try:
            # Check if processed data file exists
            if os.path.exists(self.processed_file):
                df = pd.read_csv(self.processed_file)
                
                # Convert timestamp to datetime
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                
                self.logger.info(f"Loaded {len(df)} products from {self.processed_file}")
                return df
            else:
                self.logger.warning(f"Data file {self.processed_file} not found. Generating sample data.")
                return self.generate_sample_data()
        
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            return self.generate_sample_data()
    
    def generate_sample_data(self):
        """
        Generate sample data for initial dashboard view.
        
        Returns:
            DataFrame: Sample product data
        """
        # Categories
        categories = ['beverages', 'soft-drinks', 'detergents', 'snacks', 'personal-care', 'food']
        
        # Get all e-commerce sources from configuration
        try:
            from config.sources import get_all_sources
            all_source_configs = get_all_sources()
            sources = [source['name'] for source in all_source_configs]
            self.logger.info(f"Using {len(sources)} sources from configuration")
        except Exception as e:
            self.logger.warning(f"Failed to load sources from config: {str(e)}. Using defaults.")
            # Fallback to basic sources
            sources = ['Jumia', 'Konga', 'Jiji', 'PayPorte', 'Slot', 'Temu', 'Fouani', 'AjeboMarket', 'FoodCo', 
                      'ZikelCosmetics', 'BedmateFurniture', 'LaternaBooks', 'OneHealthNG', 'Autochek', 'Kusnap', 'NBS']
        
        # Sample product names by category
        product_templates = {
            'beverages': [
                "Lipton Yellow Label Tea {size}g",
                "Nescafé Classic Coffee {size}g",
                "Milo Energy Drink {size}g",
                "Bournvita Chocolate Drink {size}g",
                "Ovaltine Malted Drink {size}g",
                "Peak Milk Powder {size}g",
                "Dano Milk Powder {size}g",
                "Cowbell Milk Powder {size}g",
                "Nestlé Pure Life Water {size}L"
            ],
            'soft-drinks': [
                "Coca-Cola {size}L",
                "Pepsi {size}L",
                "Fanta Orange {size}L",
                "Sprite {size}L",
                "7UP {size}L",
                "Mirinda {size}L",
                "Schweppes Tonic Water {size}L",
                "Mountain Dew {size}L",
                "Teem Bitter Lemon {size}L"
            ],
            'detergents': [
                "Omo Multi-Active Detergent {size}kg",
                "Ariel Detergent Powder {size}kg",
                "Sunlight Detergent {size}kg",
                "Surf Excel Detergent {size}kg",
                "Hypo Bleach {size}L",
                "Morning Fresh Dishwashing Liquid {size}L",
                "Harpic Toilet Cleaner {size}L",
                "Dettol Surface Cleaner {size}L"
            ],
            'snacks': [
                "Digestive Biscuits {size}g",
                "Pure Bliss Wafers {size}g",
                "McVitie's Digestive {size}g",
                "Pringles {flavor} {size}g",
                "Cadbury Chocolate {size}g",
                "Tom Tom Sweet {size}g",
                "Orbit Chewing Gum {size}g",
                "Buttermint Sweets {size}g"
            ],
            'personal-care': [
                "Dettol Soap {size}g",
                "Lux Soap {size}g",
                "Close-Up Toothpaste {size}g",
                "Oral-B Toothbrush",
                "Head & Shoulders Shampoo {size}ml",
                "Nivea Body Lotion {size}ml",
                "Rexona Deodorant {size}ml",
                "Dove Soap {size}g"
            ],
            'food': [
                "Golden Penny Rice {size}kg",
                "Indomie Noodles {flavor} {size}g",
                "Honeywell Semolina {size}kg",
                "Golden Penny Spaghetti {size}g",
                "Kings Oil Vegetable Oil {size}L",
                "Knorr Seasoning Cubes {size}g",
                "Gino Tomato Paste {size}g",
                "Titus Sardines {size}g"
            ]
        }
        
        # Generate sample data
        num_products = 300
        data = []
        
        for _ in range(num_products):
            # Select random category
            category = random.choice(categories)
            
            # Select random product template for the category
            template = random.choice(product_templates[category])
            
            # Random size
            size = random.choice([50, 100, 200, 250, 500, 1000, 2000])
            
            # Flavor for applicable products
            flavor = random.choice(["Original", "Classic", "Regular", "Extra", "Special"])
            
            # Format product name
            product_name = template.format(size=size, flavor=flavor)
            
            # Random price (in Naira)
            if category in ['beverages', 'soft-drinks']:
                price = random.uniform(500, 5000)
            elif category == 'detergents':
                price = random.uniform(1000, 8000)
            elif category == 'personal-care':
                price = random.uniform(300, 3000)
            else:
                price = random.uniform(200, 10000)
            
            # Random source
            source = random.choice(sources)
            
            # Random timestamp within the last 30 days
            timestamp = datetime.now() - timedelta(days=random.randint(0, 30))
            
            # Random discount
            discount_percent = random.choice([0, 0, 0, 5, 10, 15, 20, 25, 30])
            
            # Random bestseller flag
            is_bestseller = random.choice([True, False, False, False, False])
            
            # Random rating
            rating = round(random.uniform(3, 5), 1) if random.random() > 0.3 else 0
            
            # Random view count
            view_count = random.randint(10, 1000)
            
            # Random sales rank
            sales_rank = random.uniform(1, 100)
            
            # Create product entry
            product = {
                "product_name": product_name,
                "price": price,
                "category": category,
                "source": source,
                "timestamp": timestamp,
                "discount_percent": discount_percent,
                "is_bestseller": is_bestseller,
                "rating": rating,
                "view_count": view_count,
                "sales_rank": sales_rank,
                "url": f"https://www.{source.lower()}.com.ng/products/{category}/{random.randint(100000, 999999)}"
            }
            
            data.append(product)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Create sample NBS data
        self.create_sample_nbs_data()
        
        return df
    
    def create_sample_nbs_data(self):
        """
        Create sample NBS data file.
        """
        try:
            # Economic indicators data
            indicators = [
                {"indicator": "Inflation Rate", "value": 22.41, "period": "May 2023", "data_type": "inflation"},
                {"indicator": "Food Inflation", "value": 24.82, "period": "May 2023", "data_type": "inflation"},
                {"indicator": "Core Inflation", "value": 20.06, "period": "May 2023", "data_type": "inflation"},
                {"indicator": "GDP Growth Rate", "value": 2.31, "period": "Q1 2023", "data_type": "gdp"},
                {"indicator": "Consumer Price Index", "value": 444.34, "period": "May 2023", "data_type": "cpi"},
                {"indicator": "Food Price Index", "value": 492.15, "period": "May 2023", "data_type": "cpi"},
                {"indicator": "Average Household Expenditure on Food", "value": 18000, "period": "2022", "data_type": "household"},
                {"indicator": "Average Household Expenditure on Non-Food", "value": 22000, "period": "2022", "data_type": "household"},
                {"indicator": "Import Value (₦ Billion)", "value": 4875.93, "period": "Q1 2023", "data_type": "trade"},
                {"indicator": "Export Value (₦ Billion)", "value": 6217.62, "period": "Q1 2023", "data_type": "trade"},
                {"indicator": "Household Consumption Growth", "value": 3.54, "period": "Q1 2023", "data_type": "household"},
                {"indicator": "Food and Non-Alcoholic Beverages Inflation", "value": 25.84, "period": "May 2023", "data_type": "inflation"},
                {"indicator": "Alcoholic Beverages Inflation", "value": 19.38, "period": "May 2023", "data_type": "inflation"},
                {"indicator": "Clothing and Footwear Inflation", "value": 18.62, "period": "May 2023", "data_type": "inflation"},
                {"indicator": "Housing, Water, Electricity, Gas Inflation", "value": 16.67, "period": "May 2023", "data_type": "inflation"}
            ]
            
            # Add source and timestamp
            for indicator in indicators:
                indicator["source"] = "NBS"
                indicator["timestamp"] = datetime.now()
            
            # Convert to DataFrame and save
            nbs_df = pd.DataFrame(indicators)
            nbs_file = os.path.join(self.data_dir, "nbs_data.csv")
            
            # Ensure data directory exists
            os.makedirs(self.data_dir, exist_ok=True)
            
            nbs_df.to_csv(nbs_file, index=False)
            self.logger.info(f"Sample NBS data saved to {nbs_file}")
            
        except Exception as e:
            self.logger.error(f"Error creating sample NBS data: {str(e)}")
