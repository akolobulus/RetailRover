import time
import random
import pandas as pd
from datetime import datetime
from scrapers.base_scraper import BaseScraper
from typing import List, Dict, Any, Optional, Union

class TemuScraper(BaseScraper):
    """
    Scraper implementation for Temu Nigeria e-commerce website.
    """
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.temu.com/ng"
        
        # Categories to scrape
        self.categories = {
            "beverages": "/category/beverage-1299-0-74-0-65-0.html",
            "soft-drinks": "/category/beverage-1299-0-74-0-65-0-5-0.html",
            "detergents": "/category/household-cleaning-1303-0.html", 
            "snacks": "/category/food-1297-0.html",
            "personal-care": "/category/personal-care-1263-0.html"
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
                product_cards = soup.select(".product-item")
                
                if not product_cards:
                    self.logger.info(f"No products found on page {page} for category {category_name}")
                    break
                
                for card in product_cards:
                    try:
                        # Extract product details
                        product_name = self.extract_text(card, ".product-title")
                        price_text = self.extract_text(card, ".product-price")
                        price = self.extract_price(price_text)
                        
                        # Extract product URL
                        product_url = ""
                        url_elem = card.select_one("a.product-link")
                        if url_elem and url_elem.has_attr("href"):
                            href = url_elem["href"]
                            if isinstance(href, str) and not href.startswith("http"):
                                product_url = self.base_url + href
                            else:
                                product_url = href
                        
                        # Extract rating if available
                        rating = 0
                        rating_elem = card.select_one(".rating-score")
                        if rating_elem:
                            try:
                                rating = float(rating_elem.get_text(strip=True))
                            except ValueError:
                                rating = 0
                        
                        # Get discount information if available
                        discount_percent = 0
                        discount_elem = card.select_one(".discount-tag")
                        if discount_elem:
                            discount_text = discount_elem.get_text(strip=True)
                            try:
                                discount_percent = int(discount_text.replace("%", "").replace("-", ""))
                            except ValueError:
                                discount_percent = 0
                        
                        # Check for bestseller badge
                        is_bestseller = False
                        badges = card.select(".product-badge")
                        for badge in badges:
                            if "bestseller" in badge.get_text(strip=True).lower():
                                is_bestseller = True
                                break
                        
                        # Add product to results
                        products.append({
                            "product_name": product_name,
                            "price": price,
                            "category": category_name,
                            "source": "Temu",
                            "url": product_url,
                            "rating": rating,
                            "discount_percent": discount_percent,
                            "is_bestseller": is_bestseller,
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
        
        self.logger.info(f"Total products scraped from Temu: {len(all_products)}")
        return all_products