"""
Customer review analysis module for Nigerian e-commerce analytics.
This module provides sentiment analysis and key aspect extraction for product reviews.
"""

import re
import pandas as pd
import numpy as np
from collections import Counter
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ReviewAnalyzer")

class ReviewAnalyzer:
    """
    Analyze customer reviews for sentiment and key aspects.
    Provides insights into customer opinions about products.
    """
    
    def __init__(self):
        """Initialize the review analyzer with sentiment dictionaries"""
        # Positive and negative sentiment word dictionaries
        self.positive_words = {
            'good', 'great', 'excellent', 'amazing', 'wonderful', 'best', 'perfect',
            'love', 'awesome', 'fantastic', 'quality', 'recommend', 'satisfied',
            'happy', 'pleased', 'impressive', 'outstanding', 'super', 'nice', 'worth',
            'beautiful', 'comfortable', 'easy', 'reliable', 'durable', 'affordable',
            'value', 'fast', 'genuine', 'authentic', 'efficient', 'helpful'
        }
        
        self.negative_words = {
            'bad', 'poor', 'terrible', 'horrible', 'awful', 'worst', 'disappointed',
            'disappointing', 'defective', 'broken', 'cheap', 'expensive', 'waste',
            'slow', 'difficult', 'hard', 'uncomfortable', 'useless', 'overpriced',
            'fake', 'counterfeit', 'unhappy', 'regret', 'problem', 'issue', 'faulty',
            'damaged', 'late', 'delay', 'malfunction', 'fail', 'failure', 'complaint',
            'return', 'refund', 'scam', 'unreliable', 'avoid'
        }
        
        # Word importance weightings - some words convey stronger sentiment
        self.word_weights = {
            'excellent': 1.5, 'amazing': 1.5, 'outstanding': 1.5, 'perfect': 1.5,
            'terrible': 1.5, 'horrible': 1.5, 'awful': 1.5, 'best': 1.5, 'worst': 1.5,
            'love': 1.3, 'hate': 1.3, 'fantastic': 1.3, 'disappointed': 1.3,
            'counterfeit': 1.8, 'fake': 1.5, 'scam': 1.8, 'waste': 1.3,
            'recommend': 1.4, 'avoid': 1.4, 'genuine': 1.3, 'authentic': 1.3
        }
        
        # Aspect categories to track with associated keywords
        self.aspect_keywords = {
            'quality': ['quality', 'build', 'material', 'durable', 'sturdy', 'flimsy',
                       'cheap', 'craftsmanship', 'made', 'construction', 'solid'],
            'price': ['price', 'expensive', 'affordable', 'worth', 'value', 'cost',
                     'cheap', 'overpriced', 'bargain', 'budget', 'money'],
            'shipping': ['shipping', 'delivery', 'arrive', 'arrived', 'package', 'late',
                       'damage', 'courier', 'box', 'tracking', 'time', 'quick', 'fast'],
            'customer_service': ['service', 'support', 'help', 'response', 'responsive',
                               'contact', 'staff', 'communication', 'seller', 'agent'],
            'authenticity': ['original', 'authentic', 'genuine', 'fake', 'counterfeit',
                           'real', 'legit', 'knockoff', 'copy', 'imitation'],
            'functionality': ['work', 'function', 'feature', 'broken', 'defective',
                            'operation', 'perform', 'working', 'failed', 'correctly'],
            'appearance': ['look', 'design', 'color', 'style', 'beautiful', 'attractive',
                         'appearance', 'sleek', 'elegant', 'aesthetic', 'nice'],
            'usability': ['easy', 'difficult', 'user-friendly', 'complicated', 'simple',
                        'intuitive', 'confusing', 'convenient', 'usable', 'efficient']
        }
        
        # Common negating words that invert sentiment
        self.negation_words = {
            'not', 'no', 'never', "don't", "doesn't", "didn't", "isn't", "aren't",
            "wasn't", "weren't", "haven't", "hasn't", "hadn't", "won't", "wouldn't",
            "can't", "couldn't", "shouldn't", "without"
        }
        
        # Nigerian market specific aspects
        self.ng_market_aspects = {
            'power_compatibility': ['power', 'voltage', 'generator', 'electricity', 'outage', 
                                  'current', 'charging', 'charge', 'battery', 'adapter', 'nepa'],
            'warranty': ['warranty', 'guarantee', 'return', 'refund', 'replacement', 'repair', 'fix'],
            'location_relevance': ['nigeria', 'lagos', 'abuja', 'local', 'import', 'customs', 'duty']
        }
        
        self.aspect_keywords.update(self.ng_market_aspects)
        
        logger.info("Review analyzer initialized")
    
    def analyze_reviews(self, reviews_df):
        """
        Analyze a set of product reviews and extract sentiment and aspects.
        
        Args:
            reviews_df (DataFrame): DataFrame with product reviews 
                                   (must have 'review_text' column)
                                   
        Returns:
            DataFrame: Original reviews with added sentiment and aspect analysis
        """
        if reviews_df is None or len(reviews_df) == 0:
            logger.warning("No reviews to analyze")
            return None
            
        if 'review_text' not in reviews_df.columns:
            logger.error("DataFrame missing required 'review_text' column")
            return reviews_df
            
        # Create a copy to avoid modifying the original
        results_df = reviews_df.copy()
        
        # Apply analysis to each review
        results_df['sentiment_score'] = results_df['review_text'].apply(self.analyze_sentiment)
        results_df['sentiment'] = results_df['sentiment_score'].apply(self._score_to_sentiment)
        results_df['main_aspects'] = results_df['review_text'].apply(self.extract_aspects)
        
        # Extract the primary aspects of each review
        results_df['primary_aspect'] = results_df['main_aspects'].apply(
            lambda aspects: aspects[0][0] if aspects and len(aspects) > 0 else None
        )
        
        # Extract the sentiment for the primary aspect
        results_df['primary_aspect_sentiment'] = results_df.apply(
            lambda row: self._aspect_sentiment(row['review_text'], row['primary_aspect']), axis=1
        )
        
        logger.info(f"Analyzed {len(results_df)} reviews")
        return results_df
    
    def analyze_sentiment(self, text):
        """
        Analyze the sentiment of a single review text.
        
        Args:
            text (str): Review text
            
        Returns:
            float: Sentiment score from -1.0 (very negative) to 1.0 (very positive)
        """
        if not text or not isinstance(text, str):
            return 0.0
            
        # Preprocess text
        processed_text = self._preprocess_text(text)
        words = processed_text.split()
        
        if not words:
            return 0.0
            
        # Check for sentiment words
        pos_score = 0
        neg_score = 0
        
        # Track negation state (flips sentiment when True)
        negated = False
        negation_window = 0  # Words left in negation window
        
        for i, word in enumerate(words):
            # Reset negation window counter if needed
            if negation_window > 0:
                negation_window -= 1
            elif negated:
                # End of negation window
                negated = False
            
            # Check for negation words
            if word in self.negation_words:
                negated = True
                negation_window = 3  # Apply negation to the next 3 words
                continue
            
            # Check sentiment and apply negation if needed
            sentiment_value = 0
            
            # Get word importance weight (default to 1.0)
            word_weight = self.word_weights.get(word, 1.0)
            
            if word in self.positive_words:
                sentiment_value = word_weight
            elif word in self.negative_words:
                sentiment_value = -word_weight
                
            # Apply negation if in a negation window
            if negated:
                sentiment_value = -sentiment_value
                
            # Add to the appropriate score
            if sentiment_value > 0:
                pos_score += sentiment_value
            elif sentiment_value < 0:
                neg_score -= sentiment_value  # Note: neg_score is positive
                
        # Calculate final score (-1 to 1 range)
        total_sentiment_words = float(pos_score + neg_score)
        
        if total_sentiment_words == 0:
            return 0.0
            
        # Normalize to [-1, 1] range
        sentiment_score = (pos_score - neg_score) / total_sentiment_words
        
        # Apply tanh to smooth the curve
        return np.tanh(sentiment_score)
    
    def _score_to_sentiment(self, score):
        """
        Convert a numerical sentiment score to a categorical label.
        
        Args:
            score (float): Sentiment score from -1.0 to 1.0
            
        Returns:
            str: Sentiment category
        """
        if score >= 0.6:
            return "Very Positive"
        elif score >= 0.2:
            return "Positive"
        elif score > -0.2:
            return "Neutral"
        elif score > -0.6:
            return "Negative"
        else:
            return "Very Negative"
    
    def extract_aspects(self, text, top_n=3):
        """
        Extract the main aspects discussed in the review.
        
        Args:
            text (str): Review text
            top_n (int): Number of top aspects to return
            
        Returns:
            list: List of (aspect, relevance_score) tuples
        """
        if not text or not isinstance(text, str):
            return []
            
        # Preprocess text
        processed_text = self._preprocess_text(text)
        words = processed_text.split()
        
        if not words:
            return []
            
        # Count aspect occurrences
        aspect_counts = {aspect: 0 for aspect in self.aspect_keywords}
        
        for aspect, keywords in self.aspect_keywords.items():
            # Count keyword occurrences for this aspect
            for word in words:
                if word in keywords:
                    aspect_counts[aspect] += 1
        
        # Sort aspects by occurrence count
        sorted_aspects = sorted(
            [(aspect, count) for aspect, count in aspect_counts.items() if count > 0],
            key=lambda x: x[1],
            reverse=True
        )
        
        # Return top N aspects
        return sorted_aspects[:top_n]
    
    def _aspect_sentiment(self, text, aspect):
        """
        Extract sentiment for a specific aspect in a review.
        
        Args:
            text (str): Review text
            aspect (str): Aspect to analyze
            
        Returns:
            float: Aspect-specific sentiment score
        """
        if not text or not isinstance(text, str) or not aspect:
            return 0.0
            
        if aspect not in self.aspect_keywords:
            return 0.0
            
        # Get keywords for this aspect
        aspect_words = self.aspect_keywords[aspect]
        
        # Preprocess text
        processed_text = self._preprocess_text(text)
        words = processed_text.split()
        
        # Find instances of aspect words
        aspect_positions = []
        for i, word in enumerate(words):
            if word in aspect_words:
                aspect_positions.append(i)
                
        if not aspect_positions:
            return 0.0
            
        # Check sentiment in context windows around aspect words
        window_size = 5  # Words to check on either side
        pos_score = 0
        neg_score = 0
        
        for pos in aspect_positions:
            # Define window boundaries
            window_start = max(0, pos - window_size)
            window_end = min(len(words), pos + window_size + 1)
            
            # Get words in window
            window_words = words[window_start:window_end]
            
            # Track negation for this window
            negated = False
            
            for i, word in enumerate(window_words):
                # Check for negation words
                if word in self.negation_words:
                    negated = not negated
                    continue
                
                # Get word importance weight (default to 1.0)
                word_weight = self.word_weights.get(word, 1.0)
                
                # Check sentiment 
                if word in self.positive_words:
                    pos_score += word_weight if not negated else 0
                    neg_score += 0 if not negated else word_weight
                elif word in self.negative_words:
                    pos_score += 0 if not negated else word_weight
                    neg_score += word_weight if not negated else 0
        
        # Calculate final score for this aspect
        total_sentiment_words = float(pos_score + neg_score)
        
        if total_sentiment_words == 0:
            return 0.0
            
        # Normalize to [-1, 1] range
        sentiment_score = (pos_score - neg_score) / total_sentiment_words
        
        # Apply tanh to smooth the curve
        return np.tanh(sentiment_score)
    
    def summarize_product_reviews(self, reviews_df):
        """
        Generate a summary of all reviews for a product.
        
        Args:
            reviews_df (DataFrame): DataFrame with analyzed reviews
            
        Returns:
            dict: Summary statistics for product reviews
        """
        if reviews_df is None or len(reviews_df) == 0:
            return {
                'review_count': 0,
                'avg_sentiment': 0,
                'sentiment_distribution': {},
                'top_aspects': [],
                'aspect_sentiments': {}
            }
            
        if 'sentiment_score' not in reviews_df.columns:
            # Analyze reviews if not already done
            reviews_df = self.analyze_reviews(reviews_df)
            
        # Basic statistics
        review_count = len(reviews_df)
        avg_sentiment = reviews_df['sentiment_score'].mean()
        
        # Sentiment distribution
        sentiment_distribution = reviews_df['sentiment'].value_counts().to_dict()
        
        # Get top aspects across all reviews
        all_aspects = []
        for aspects in reviews_df['main_aspects']:
            if aspects:
                all_aspects.extend([aspect for aspect, _ in aspects])
                
        aspect_counter = Counter(all_aspects)
        top_aspects = aspect_counter.most_common(5)
        
        # Calculate sentiment by aspect
        aspect_sentiments = {}
        for aspect in set(all_aspects):
            # Get reviews that mention this aspect
            aspect_reviews = reviews_df[reviews_df['main_aspects'].apply(
                lambda aspects: any(a == aspect for a, _ in aspects if aspects)
            )]
            
            if len(aspect_reviews) > 0:
                aspect_sentiments[aspect] = {
                    'count': len(aspect_reviews),
                    'avg_sentiment': aspect_reviews['sentiment_score'].mean(),
                    'positive_pct': (aspect_reviews['sentiment_score'] > 0.2).mean() * 100,
                    'negative_pct': (aspect_reviews['sentiment_score'] < -0.2).mean() * 100
                }
        
        return {
            'review_count': review_count,
            'avg_sentiment': avg_sentiment,
            'sentiment_distribution': sentiment_distribution,
            'top_aspects': top_aspects,
            'aspect_sentiments': aspect_sentiments
        }
    
    def _preprocess_text(self, text):
        """
        Preprocess review text for analysis.
        
        Args:
            text (str): Raw review text
            
        Returns:
            str: Preprocessed text
        """
        if not isinstance(text, str):
            return ""
            
        # Convert to lowercase
        text = text.lower()
        
        # Remove punctuation except for negation apostrophes
        text = re.sub(r"[^\w\s']", " ", text)
        
        # Normalize negation contractions
        text = text.replace("n't", " not")
        
        # Replace multiple spaces with single space
        text = re.sub(r"\s+", " ", text)
        
        return text.strip()
    
    def generate_word_cloud_data(self, reviews_df):
        """
        Generate word frequency data for word clouds.
        
        Args:
            reviews_df (DataFrame): DataFrame with reviews
            
        Returns:
            dict: Word frequencies for positive and negative reviews
        """
        if 'sentiment_score' not in reviews_df.columns or 'review_text' not in reviews_df.columns:
            reviews_df = self.analyze_reviews(reviews_df)
            
        if reviews_df is None or len(reviews_df) == 0:
            return {'positive': {}, 'negative': {}}
            
        # Split into positive and negative reviews
        positive_reviews = reviews_df[reviews_df['sentiment_score'] > 0.2]
        negative_reviews = reviews_df[reviews_df['sentiment_score'] < -0.2]
        
        # Get word frequencies for positive reviews
        positive_words = []
        for text in positive_reviews['review_text']:
            if isinstance(text, str):
                words = self._preprocess_text(text).split()
                positive_words.extend(words)
                
        # Get word frequencies for negative reviews
        negative_words = []
        for text in negative_reviews['review_text']:
            if isinstance(text, str):
                words = self._preprocess_text(text).split()
                negative_words.extend(words)
                
        # Count word frequencies
        positive_counter = Counter(positive_words)
        negative_counter = Counter(negative_words)
        
        # Remove common stopwords
        stopwords = {'the', 'and', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'is', 'are',
                   'was', 'were', 'be', 'been', 'being', 'this', 'that', 'these', 'those', 'it', 'its',
                   'i', 'my', 'me', 'mine', 'you', 'your', 'yours', 'they', 'their', 'them', 'have', 'has',
                   'had', 'do', 'does', 'did', 'will', 'would', 'shall', 'should', 'can', 'could', 'may',
                   'might', 'must', 'but', 'so', 'or', 'as', 'if', 'than', 'by', 'from'}
                   
        for word in stopwords:
            positive_counter.pop(word, None)
            negative_counter.pop(word, None)
            
        # Get top word frequencies (limit to top 50)
        positive_freq = dict(positive_counter.most_common(50))
        negative_freq = dict(negative_counter.most_common(50))
        
        return {
            'positive': positive_freq,
            'negative': negative_freq
        }