import time
import random
import pandas as pd
from datetime import datetime
from scrapers.base_scraper import BaseScraper

class JumiaScraper(BaseScraper):
    """
    Scraper implementation for Jumia Nigeria e-commerce website.
    """
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.jumia.com.ng"
        
        # Categories to scrape
        self.categories = {
            "beverages": "/groceries/beverages/",
            "soft-drinks": "/groceries/soft-drinks/",
            "detergents": "/home-office/laundry-cleaning/",
            "snacks": "/groceries/snacks/",
            "toiletries": "/health-beauty/bath-body/"
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
                product_cards = soup.select("article.prd")
                
                if not product_cards:
                    self.logger.info(f"No products found on page {page} for category {category_name}")
                    break
                
                for card in product_cards:
                    try:
                        # Extract product details
                        product_name = self.extract_text(card, "h3.name")
                        price_text = self.extract_text(card, ".prc")
                        price = self.extract_price(price_text)
                        
                        # Extract product URL for additional details
                        product_url = ""
                        url_elem = card.select_one("a.core")
                        if url_elem and url_elem.has_attr("href"):
                            product_url = self.base_url + url_elem["href"]
                        
                        # Extract rating if available
                        rating = 0
                        rating_elem = card.select_one(".stars._s")
                        if rating_elem and rating_elem.has_attr("data-val"):
                            try:
                                rating = float(rating_elem["data-val"])
                            except ValueError:
                                rating = 0
                        
                        # Get discount information if available
                        discount_percent = 0
                        discount_elem = card.select_one(".bdg._dsct")
                        if discount_elem:
                            discount_text = discount_elem.get_text(strip=True)
                            try:
                                discount_percent = int(discount_text.replace("%", "").replace("-", ""))
                            except ValueError:
                                discount_percent = 0
                        
                        # Check if product is a bestseller
                        is_bestseller = False
                        tag_elems = card.select(".bdg")
                        for tag in tag_elems:
                            if "bestseller" in tag.get_text(strip=True).lower():
                                is_bestseller = True
                                break
                        
                        # Add product to results
                        products.append({
                            "product_name": product_name,
                            "price": price,
                            "category": category_name,
                            "source": "Jumia",
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
        
        self.logger.info(f"Total products scraped from Jumia: {len(all_products)}")
        return all_products
