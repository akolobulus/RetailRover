import time
import random
import pandas as pd
from datetime import datetime
from scrapers.base_scraper import BaseScraper
from typing import List, Dict, Any, Optional, Union

class PayPorteScraper(BaseScraper):
    """
    Scraper implementation for PayPorte Nigeria e-commerce website.
    """
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://payporte.com"
        
        # Categories to scrape
        self.categories = {
            "beverages": "/food-drinks/beverages",
            "soft-drinks": "/food-drinks/soft-drinks",
            "household": "/home-kitchen",
            "toiletries": "/beauty-health/bath-body",
            "food": "/food-drinks/food-cupboard"
        }
    
    def scrape_category(self, category_name, category_url, page_limit=3):
        """
        Scrape products from a specific category.
        
        Args:
            category_name (str): Name of the category
            category_url (str): URL path for the category
            page_limit (int): Maximum number of pages to scrape
            
        Returns:
            list: List of product dictionaries
        """
        products = []
        full_url = self.base_url + category_url
        
        for page in range(1, page_limit + 1):
            try:
                url = f"{full_url}?page={page}"
                soup = self.get_page(url)
                
                if not soup:
                    self.logger.warning(f"Failed to get page {page} for category {category_name}")
                    continue
                
                # Find product containers
                product_cards = soup.select(".product-item, .product-card")
                
                if not product_cards:
                    self.logger.info(f"No products found on page {page} for category {category_name}")
                    break
                
                for card in product_cards:
                    try:
                        # Extract product details
                        product_name = self.extract_text(card, ".product-title, .product-name")
                        price_text = self.extract_text(card, ".price, .product-price")
                        price = self.extract_price(price_text)
                        
                        # Extract original price if available (for discount calculation)
                        original_price_text = self.extract_text(card, ".old-price, .regular-price")
                        original_price = self.extract_price(original_price_text)
                        
                        # Calculate discount
                        discount_percent = 0
                        if original_price > 0 and price > 0 and original_price > price:
                            discount_percent = round(((original_price - price) / original_price) * 100)
                        
                        # Extract product URL
                        product_url = ""
                        url_elem = card.select_one("a.product-link, a.product-url")
                        if url_elem and url_elem.has_attr("href"):
                            href = url_elem["href"]
                            if isinstance(href, str) and not href.startswith("http"):
                                product_url = self.base_url + href
                            else:
                                product_url = href
                        
                        # Check if product is on sale or new
                        is_on_sale = False
                        is_new = False
                        labels = card.select(".product-label, .product-badge")
                        for label in labels:
                            label_text = label.get_text(strip=True).lower()
                            if "sale" in label_text or "discount" in label_text:
                                is_on_sale = True
                            if "new" in label_text:
                                is_new = True
                        
                        # Add product to results
                        products.append({
                            "product_name": product_name,
                            "price": price,
                            "original_price": original_price,
                            "category": category_name,
                            "source": "PayPorte",
                            "url": product_url,
                            "discount_percent": discount_percent,
                            "is_on_sale": is_on_sale,
                            "is_new": is_new,
                            "timestamp": datetime.now()
                        })
                    except Exception as e:
                        self.logger.error(f"Error extracting product data: {str(e)}")
                
                # Random delay between page requests
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                self.logger.error(f"Error scraping category {category_name}, page {page}: {str(e)}")
        
        return products
    
    def scrape_data(self) -> List[Dict[str, Any]]:
        """
        Scrape product data from all categories.
        
        Returns:
            list: List of product dictionaries
        """
        all_products = []
        
        for category_name, category_url in self.categories.items():
            try:
                self.logger.info(f"Scraping category: {category_name}")
                products = self.scrape_category(category_name, category_url)
                all_products.extend(products)
                self.logger.info(f"Found {len(products)} products in category {category_name}")
                
                # Short delay between categories
                time.sleep(random.uniform(3, 7))
                
            except Exception as e:
                self.logger.error(f"Error scraping category {category_name}: {str(e)}")
        
        self.logger.info(f"Total products scraped from PayPorte: {len(all_products)}")
        return all_products