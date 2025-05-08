"""
E-commerce scraper template that provides a standard structure for creating new e-commerce scrapers.
New scrapers can inherit from this template and override specific selectors.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from bs4 import BeautifulSoup

from scrapers.base_scraper import BaseScraper

class EcommerceScraper(BaseScraper):
    """
    Base template for all e-commerce scrapers.
    Concrete scrapers should inherit from this class and override
    the site-specific attributes and methods.
    """
    
    def __init__(self):
        """
        Initialize the scraper with default values.
        Override these values in concrete scraper implementations.
        """
        super().__init__()
        
        # Site-specific attributes to override in concrete classes
        self.site_name = "GenericEcommerce"
        self.base_url = "https://example.com"
        self.categories = {
            "electronics": "/electronics",
            "fashion": "/fashion",
            "home": "/home-and-kitchen",
            "groceries": "/groceries",
            "health": "/health-and-beauty",
        }
        
        # CSS selectors (override these in concrete classes)
        self.selectors = {
            "product_container": ".product-item",
            "product_name": ".product-name",
            "product_price": ".price",
            "product_old_price": ".old-price",
            "product_discount": ".discount",
            "product_rating": ".rating",
            "product_review_count": ".review-count",
            "product_image": ".product-image img",
            "product_url": ".product-name a",
            "product_brand": ".brand",
            "product_seller": ".seller",
            "product_availability": ".availability",
            "product_category": ".breadcrumb .category",
            "next_page": ".pagination .next a",
        }
        
        # Configure logger
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def build_category_url(self, category_key, page=1):
        """
        Build the URL for a specific category and page.
        
        Args:
            category_key (str): Key for the category
            page (int): Page number
            
        Returns:
            str: Complete category URL
        """
        if category_key not in self.categories:
            self.logger.warning(f"Unknown category: {category_key}")
            return None
        
        category_path = self.categories[category_key]
        
        # Different sites handle pagination differently
        # This is a generic approach that might need overriding
        if "?" in category_path:
            return f"{self.base_url}{category_path}&page={page}"
        else:
            return f"{self.base_url}{category_path}?page={page}"
    
    def extract_product_info(self, product_element):
        """
        Extract product information from a product element.
        
        Args:
            product_element: BeautifulSoup element containing product data
            
        Returns:
            dict: Product information
        """
        product_name = self.extract_text(product_element, self.selectors["product_name"])
        product_url_elem = product_element.select_one(self.selectors["product_url"])
        product_url = product_url_elem.get("href") if product_url_elem else ""
        
        # Make relative URLs absolute
        if product_url and not product_url.startswith(("http://", "https://")):
            product_url = self.base_url + (product_url if product_url.startswith("/") else "/" + product_url)
        
        # Extract price and convert to float
        price_text = self.extract_text(product_element, self.selectors["product_price"])
        price = self.extract_price(price_text)
        
        # Extract old price if available
        old_price_text = self.extract_text(product_element, self.selectors["product_old_price"])
        old_price = self.extract_price(old_price_text) if old_price_text else None
        
        # Extract discount percentage
        discount_text = self.extract_text(product_element, self.selectors["product_discount"])
        discount = None
        if discount_text:
            import re
            discount_match = re.search(r'\d+', discount_text)
            if discount_match:
                discount = int(discount_match.group())
        
        # Extract rating and review count
        rating_text = self.extract_text(product_element, self.selectors["product_rating"])
        rating = None
        if rating_text:
            import re
            rating_match = re.search(r'\d+(\.\d+)?', rating_text)
            if rating_match:
                rating = float(rating_match.group())
        
        review_count_text = self.extract_text(product_element, self.selectors["product_review_count"])
        review_count = None
        if review_count_text:
            import re
            review_match = re.search(r'\d+', review_count_text)
            if review_match:
                review_count = int(review_match.group())
        
        # Extract image URL
        image_elem = product_element.select_one(self.selectors["product_image"])
        image_url = image_elem.get("src") if image_elem else None
        # Fallback to data-src if src is not available (lazy loading)
        if not image_url and image_elem:
            image_url = image_elem.get("data-src")
        
        # Extract brand, seller, and availability
        brand = self.extract_text(product_element, self.selectors["product_brand"])
        seller = self.extract_text(product_element, self.selectors["product_seller"])
        availability = self.extract_text(product_element, self.selectors["product_availability"])
        
        # Create product data dictionary
        product_data = {
            "product_name": product_name,
            "price": price,
            "old_price": old_price,
            "discount_percentage": discount,
            "category": self.extract_text(product_element, self.selectors["product_category"]),
            "rating": rating,
            "review_count": review_count,
            "product_url": product_url,
            "image_url": image_url,
            "brand": brand if brand else None,
            "seller": seller if seller else None,
            "availability": availability if availability else "In Stock",  # Default assumption
            "source": self.site_name,
            "timestamp": datetime.now(),
        }
        
        return product_data
    
    def scrape_category(self, category_key, page_limit=3):
        """
        Scrape products from a specific category.
        
        Args:
            category_key (str): Key for the category to scrape
            page_limit (int): Maximum number of pages to scrape
            
        Returns:
            list: List of product dictionaries
        """
        category_results = []
        
        for page in range(1, page_limit + 1):
            category_url = self.build_category_url(category_key, page)
            if not category_url:
                continue
                
            self.logger.info(f"Scraping {self.site_name} - {category_key}, page {page}: {category_url}")
            
            # Get page content
            soup = self.get_page(category_url)
            if not soup:
                self.logger.warning(f"Failed to fetch page {page} for {category_key}")
                break
            
            # Find all product elements
            product_elements = soup.select(self.selectors["product_container"])
            if not product_elements:
                self.logger.warning(f"No products found on page {page} for {category_key}")
                break
            
            # Extract product data
            for product_element in product_elements:
                try:
                    product_data = self.extract_product_info(product_element)
                    if product_data and product_data["product_name"]:
                        category_results.append(product_data)
                except Exception as e:
                    self.logger.error(f"Error extracting product data: {str(e)}")
            
            # Check if there's a next page
            next_page = soup.select_one(self.selectors["next_page"])
            if not next_page:
                self.logger.info(f"No more pages for {category_key}")
                break
        
        self.logger.info(f"Scraped {len(category_results)} products from {self.site_name} - {category_key}")
        return category_results
    
    def scrape_data(self):
        """
        Scrape product data from all categories.
        
        Returns:
            list: List of product dictionaries
        """
        all_results = []
        
        # Scrape each category
        for category_key in self.categories:
            try:
                category_results = self.scrape_category(category_key)
                all_results.extend(category_results)
            except Exception as e:
                self.logger.error(f"Error scraping {category_key}: {str(e)}")
        
        self.logger.info(f"Total products scraped from {self.site_name}: {len(all_results)}")
        return all_results