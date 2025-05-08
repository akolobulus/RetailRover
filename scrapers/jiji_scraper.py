import time
import random
import pandas as pd
from datetime import datetime
from scrapers.base_scraper import BaseScraper

class JijiScraper(BaseScraper):
    """
    Scraper implementation for Jiji Nigeria e-commerce website.
    """
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://jiji.ng"
        
        # Categories to scrape
        self.categories = {
            "food": "/food",
            "cleaning": "/cleaning-products",
            "health-beauty": "/health-beauty",
            "beverages": "/food/beverages",
            "household-items": "/home-garden/household-items"
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
                product_cards = soup.select(".b-list-advert__item")
                
                if not product_cards:
                    self.logger.info(f"No products found on page {page} for category {category_name}")
                    break
                
                for card in product_cards:
                    try:
                        # Extract product details
                        product_name = self.extract_text(card, ".qa-advert-title")
                        price_text = self.extract_text(card, ".qa-advert-price")
                        price = self.extract_price(price_text)
                        
                        # Extract product URL
                        product_url = ""
                        url_elem = card.select_one("a.b-advert-link")
                        if url_elem and url_elem.has_attr("href"):
                            product_url = url_elem["href"]
                            if not product_url.startswith("http"):
                                product_url = self.base_url + product_url
                        
                        # Extract posted time (as proxy for freshness)
                        posted_time = self.extract_text(card, ".b-list-advert__item-date")
                        
                        # Check if product is featured or urgent
                        is_featured = False
                        is_urgent = False
                        badges = card.select(".b-list-advert__item-badge")
                        for badge in badges:
                            badge_text = badge.get_text(strip=True).lower()
                            if "featured" in badge_text or "premium" in badge_text:
                                is_featured = True
                            if "urgent" in badge_text:
                                is_urgent = True
                        
                        # Add product to results
                        products.append({
                            "product_name": product_name,
                            "price": price,
                            "category": category_name,
                            "source": "Jiji",
                            "url": product_url,
                            "posted_time": posted_time,
                            "is_featured": is_featured,
                            "is_urgent": is_urgent,
                            "timestamp": datetime.now()
                        })
                    except Exception as e:
                        self.logger.error(f"Error extracting product data: {str(e)}")
                
                # Random delay between page requests
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                self.logger.error(f"Error scraping category {category_name}, page {page}: {str(e)}")
        
        return products
    
    def scrape_data(self):
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
        
        self.logger.info(f"Total products scraped from Jiji: {len(all_products)}")
        return all_products
