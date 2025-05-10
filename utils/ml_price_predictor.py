"""
Machine learning-based price prediction module for Nigerian e-commerce analytics.
This module provides price trend prediction and anomaly detection capabilities.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PricePredictor")

class PricePredictor:
    """
    Price prediction model for Nigerian e-commerce products.
    Uses historical data to predict future price trends.
    """
    
    def __init__(self, historical_data=None):
        """
        Initialize the price predictor.
        
        Args:
            historical_data (DataFrame, optional): Historical product data
        """
        self.historical_data = historical_data
        self.is_trained = False
        self.model_weights = {}
        logger.info("Price Predictor initialized")
    
    def train(self, df=None):
        """
        Train the price prediction model using historical data.
        
        Args:
            df (DataFrame, optional): Override the stored historical data
            
        Returns:
            bool: True if training was successful
        """
        if df is not None:
            self.historical_data = df
            
        if self.historical_data is None or len(self.historical_data) < 10:
            logger.warning("Insufficient data for price prediction training")
            return False
            
        try:
            # Prepare data by grouping products and calculating trends
            grouped_data = self._prepare_data()
            
            # Train a simple moving average model with trend detection
            # For each product category, calculate weight factors
            for category in grouped_data['category'].unique():
                category_data = grouped_data[grouped_data['category'] == category]
                
                # Simple weighted trend factors
                self.model_weights[category] = {
                    'trend_factor': self._calculate_trend_factor(category_data),
                    'seasonality': self._detect_seasonality(category_data),
                    'category_volatility': self._calculate_volatility(category_data)
                }
            
            self.is_trained = True
            logger.info(f"Price prediction model trained for {len(self.model_weights)} categories")
            return True
            
        except Exception as e:
            logger.error(f"Error training price prediction model: {str(e)}")
            return False
    
    def _prepare_data(self):
        """
        Prepare data for training by organizing and cleaning.
        
        Returns:
            DataFrame: Prepared data
        """
        if 'timestamp' not in self.historical_data.columns:
            # Add timestamp if not present
            self.historical_data['timestamp'] = datetime.now()
        
        # Ensure timestamp is datetime
        self.historical_data['timestamp'] = pd.to_datetime(self.historical_data['timestamp'])
        
        # Group by product and timestamp to get price trends
        df = self.historical_data.copy()
        
        # Add day of week and month features
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['month'] = df['timestamp'].dt.month
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        return df
    
    def _calculate_trend_factor(self, category_data):
        """
        Calculate the price trend factor for a category.
        
        Args:
            category_data (DataFrame): Data for a specific category
            
        Returns:
            float: Trend factor indicating price direction
        """
        # Simple implementation using recent vs older price averages
        if len(category_data) < 5:
            return 0.0
            
        # Split data into recent and older
        sorted_data = category_data.sort_values('timestamp')
        split_point = len(sorted_data) // 2
        
        older_data = sorted_data.iloc[:split_point]
        recent_data = sorted_data.iloc[split_point:]
        
        # Calculate average prices
        older_avg = older_data['price'].mean()
        recent_avg = recent_data['price'].mean()
        
        # Calculate trend factor (-1 to 1 range)
        if older_avg == 0:
            return 0
        
        trend = (recent_avg - older_avg) / older_avg
        capped_trend = max(min(trend, 1.0), -1.0)
        
        return capped_trend
    
    def _detect_seasonality(self, category_data):
        """
        Detect seasonal patterns in price data.
        
        Args:
            category_data (DataFrame): Data for a specific category
            
        Returns:
            dict: Seasonality factors by month
        """
        if 'month' not in category_data.columns or len(category_data) < 12:
            return {m: 1.0 for m in range(1, 13)}
        
        # Group by month and calculate average price relative to overall average
        monthly_avg = category_data.groupby('month')['price'].mean()
        overall_avg = category_data['price'].mean()
        
        # Calculate seasonality factors
        seasonality = {}
        for month in range(1, 13):
            if month in monthly_avg.index:
                seasonality[month] = monthly_avg[month] / overall_avg
            else:
                seasonality[month] = 1.0
                
        return seasonality
    
    def _calculate_volatility(self, category_data):
        """
        Calculate price volatility for a category.
        
        Args:
            category_data (DataFrame): Data for a specific category
            
        Returns:
            float: Volatility score (0-1)
        """
        if len(category_data) < 3:
            return 0.5
            
        # Calculate coefficient of variation as volatility measure
        mean_price = category_data['price'].mean()
        std_price = category_data['price'].std()
        
        if mean_price == 0:
            return 0.5
            
        # Normalize to 0-1 range with sigmoid
        cv = std_price / mean_price
        volatility = 1 / (1 + np.exp(-cv * 5))
        
        return volatility
    
    def predict_prices(self, products_df, days_ahead=30):
        """
        Predict future prices for products.
        
        Args:
            products_df (DataFrame): Current product data
            days_ahead (int): Number of days to predict ahead
            
        Returns:
            DataFrame: Products with predicted future prices
        """
        if not self.is_trained:
            logger.warning("Model not trained yet. Training now with available data.")
            self.train(products_df)
            
        if not self.is_trained:
            # Still not trained successfully
            logger.error("Cannot make predictions: insufficient data for training")
            return products_df
        
        # Copy dataframe to avoid modifying original
        result_df = products_df.copy()
        
        # Add prediction columns
        result_df['predicted_price'] = result_df['price']
        result_df['price_trend'] = 'Stable'
        result_df['confidence'] = 0.5
        
        # Current date and future date
        now = datetime.now()
        future_date = now + timedelta(days=days_ahead)
        future_month = future_date.month
        
        # Apply predictions to each product
        for idx, product in result_df.iterrows():
            category = product.get('category')
            
            if category not in self.model_weights:
                # Use average of all categories if this category not in model
                avg_trend = np.mean([w['trend_factor'] for w in self.model_weights.values()])
                seasonality = 1.0
                volatility = 0.5
            else:
                # Get category-specific factors
                model = self.model_weights[category]
                avg_trend = model['trend_factor']
                seasonality = model['seasonality'].get(future_month, 1.0)
                volatility = model['category_volatility']
            
            # Calculate price prediction with trend and seasonality
            current_price = product['price']
            
            # Base change from trend and seasonality
            price_change = avg_trend * (days_ahead/30) * seasonality
            
            # Add randomness based on volatility
            if volatility > 0:
                random_factor = np.random.normal(0, volatility * 0.1)
                price_change += random_factor
            
            # Apply price change
            predicted_price = current_price * (1 + price_change)
            
            # Ensure reasonable price (non-negative)
            predicted_price = max(predicted_price, current_price * 0.5)
            
            # Update prediction data
            result_df.at[idx, 'predicted_price'] = round(predicted_price, 2)
            
            # Set trend direction
            if price_change > 0.05:
                result_df.at[idx, 'price_trend'] = 'Rising'
            elif price_change < -0.05:
                result_df.at[idx, 'price_trend'] = 'Falling'
            else:
                result_df.at[idx, 'price_trend'] = 'Stable'
                
            # Set confidence based on data quality
            confidence = max(0.3, min(0.9, 0.7 - (volatility * 0.5) + (len(self.historical_data) / 1000)))
            result_df.at[idx, 'confidence'] = round(confidence, 2)
        
        logger.info(f"Generated price predictions for {len(result_df)} products")
        return result_df
    
    def detect_price_anomalies(self, products_df, threshold=0.3):
        """
        Detect anomalous prices that don't follow the expected patterns.
        
        Args:
            products_df (DataFrame): Product data to check
            threshold (float): Threshold for anomaly detection (0-1)
            
        Returns:
            DataFrame: Products with anomaly flags
        """
        if not self.is_trained or self.historical_data is None:
            logger.warning("Cannot detect anomalies without training data")
            return products_df
            
        # Copy dataframe to avoid modifying original
        result_df = products_df.copy()
        result_df['price_anomaly'] = False
        result_df['anomaly_score'] = 0.0
        
        # Group historical data by category and product for reference prices
        grouped_hist = self.historical_data.groupby(['category', 'product_name'])['price'].agg(['mean', 'std'])
        
        for idx, product in result_df.iterrows():
            category = product.get('category')
            product_name = product.get('product_name')
            current_price = product.get('price')
            
            # Skip if price is missing
            if pd.isna(current_price):
                continue
                
            try:
                # Get historical stats for this product or similar products
                if (category, product_name) in grouped_hist.index:
                    # Direct match
                    hist_mean = grouped_hist.loc[(category, product_name), 'mean']
                    hist_std = max(grouped_hist.loc[(category, product_name), 'std'], 0.01 * hist_mean)
                elif category in self.model_weights:
                    # Use category average
                    cat_products = grouped_hist[grouped_hist.index.get_level_values('category') == category]
                    hist_mean = cat_products['mean'].mean()
                    hist_std = max(cat_products['std'].mean(), 0.05 * hist_mean)
                else:
                    # Skip if no reference data
                    continue
                
                # Calculate Z-score for anomaly detection
                z_score = abs(current_price - hist_mean) / hist_std
                anomaly_score = 1 / (1 + np.exp(-(z_score - 4)))  # Sigmoid centered at z=4
                
                # Flag if anomaly score exceeds threshold
                is_anomaly = anomaly_score > threshold
                
                # Update results
                result_df.at[idx, 'price_anomaly'] = is_anomaly
                result_df.at[idx, 'anomaly_score'] = round(anomaly_score, 3)
                
            except Exception as e:
                logger.warning(f"Error detecting anomaly for product {product_name}: {str(e)}")
                continue
                
        # Count anomalies found
        anomaly_count = result_df['price_anomaly'].sum()
        logger.info(f"Detected {anomaly_count} price anomalies out of {len(result_df)} products")
        
        return result_df