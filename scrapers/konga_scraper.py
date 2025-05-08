import time
import random
import pandas as pd
from datetime import datetime
from scrapers.base_scraper import BaseScraper

class KongaScraper(BaseScraper):
    """
    Scraper implementation for Konga e-commerce website.
    """
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.konga.com"
        
        # Categories to scrape
        self.categories = {
            "beverages": "/groceries/beverages-5091",
            "soft-drinks": "/groceries/beverages-5091/soft-drinks-9265",
            "detergents": "/konga-home/laundry-cleaning-4856/detergents-9289",
            "food-cupboard": "/groceries/food-cupboard-5089",
            "personal-care": "/health-beauty/personal-care-20661"
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
                product_cards = soup.select(".product-card")
                
                if not product_cards:
                    self.logger.info(f"No products found on page {page} for category {category_name}")
                    break
                
                for card in product_cards:
                    try:
                        # Extract product details
                        product_name = self.extract_text(card, "h3")
                        price_text = self.extract_text(card, ".product-card__price")
                        price = self.extract_price(price_text)
                        
                        # Extract product URL
                        product_url = ""
                        url_elem = card.select_one("a")
                        if url_elem and url_elem.has_attr("href"):
                            product_url = url_elem["href"]
                            if not product_url.startswith("http"):
                                product_url = self.base_url + product_url
                        
                        # Determine if product is a bestseller or has a flash sale
                        is_bestseller = False
                        is_flash_sale = False
                        badges = card.select(".product-card__badge")
                        for badge in badges:
                            badge_text = badge.get_text(strip=True).lower()
                            if "best" in badge_text or "top" in badge_text:
                                is_bestseller = True
                            if "sale" in badge_text or "flash" in badge_text:
                                is_flash_sale = True
                        
                        # Extract discount information
                        discount_percent = 0
                        old_price_elem = card.select_one(".product-card__old-price")
                        if old_price_elem:
                            old_price_text = old_price_elem.get_text(strip=True)
                            old_price = self.extract_price(old_price_text)
                            if old_price > 0 and price > 0:
                                discount_percent = round(((old_price - price) / old_price) * 100)
                        
                        # Add product to results
                        products.append({
                            "product_name": product_name,
                            "price": price,
                            "category": category_name,
                            "source": "Konga",
                            "url": product_url,
                            "is_bestseller": is_bestseller,
                            "is_flash_sale": is_flash_sale,
                            "discount_percent": discount_percent,
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
        
        self.logger.info(f"Total products scraped from Konga: {len(all_products)}")
        return all_products
