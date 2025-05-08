import time
import random
import pandas as pd
from datetime import datetime
import asyncio
import aiohttp
from scrapers.async_base_scraper import AsyncBaseScraper
from typing import List, Dict, Any, Optional, Union

class AsyncJumiaScraper(AsyncBaseScraper):
    """
    Asynchronous scraper implementation for Jumia Nigeria e-commerce website.
    """
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.jumia.com.ng"
        self.categories = {
            "beverages": "/beverages/",
            "soft-drinks": "/soft-drinks/",
            "detergents": "/laundry-cleaning/",
            "snacks": "/snacks-sweets/",
            "personal-care": "/health-beauty/",
            "food": "/groceries/"
        }
    
    async def scrape_category(self, category_name, category_url, session, page_limit=3):
        """
        Asynchronously scrape products from a specific category.
        
        Args:
            category_name (str): Name of the category
            category_url (str): URL path for the category
            session (aiohttp.ClientSession): The aiohttp session
            page_limit (int): Maximum number of pages to scrape
            
        Returns:
            list: List of product dictionaries
        """
        products = []
        
        # Generate URLs for all pages we want to scrape
        page_urls = [f"{self.base_url}{category_url}?page={page}" for page in range(1, page_limit + 1)]
        self.logger.info(f"Preparing to scrape {len(page_urls)} pages for {category_name}")
        
        # Limit concurrent requests to avoid blocking
        semaphore = asyncio.Semaphore(3)
        
        async def scrape_page(url):
            async with semaphore:
                # Add random delay between requests to avoid rate limiting
                await asyncio.sleep(random.uniform(1, 3))
                
                soup = await self.get_page(url, session)
                if not soup:
                    return []
                
                page_products = []
                product_cards = soup.select('article.prd')
                
                for card in product_cards:
                    try:
                        # Extract product details
                        name_elem = card.select_one('h3.name')
                        product_name = name_elem.text.strip() if name_elem else ""
                        
                        # Extract price
                        price_elem = card.select_one('div.prc')
                        price_text = price_elem.text.strip() if price_elem else "0"
                        price = self.extract_price(price_text)
                        
                        # Extract discount
                        discount_elem = card.select_one('div.bdg._dsct')
                        discount_text = discount_elem.text.strip() if discount_elem else "0%"
                        discount_percent = int(discount_text.replace('%', '').replace('-', '')) if "%" in discount_text else 0
                        
                        # Extract rating
                        rating_elem = card.select_one('div.stars._s')
                        rating = float(rating_elem.get('data-stars', 0)) if rating_elem else 0
                        
                        # Check if bestseller
                        bestseller_elem = card.select_one('div.bdg._bst')
                        is_bestseller = bestseller_elem is not None
                        
                        # Get product URL
                        url_elem = card.select_one('a.core')
                        product_url = self.base_url + url_elem.get('href') if url_elem else ""
                        
                        # Extract product size/volume from the name for unit normalization
                        normalized_units = self.normalize_units(product_name)
                        
                        product = {
                            "product_name": product_name,
                            "price": price,
                            "discount_percent": discount_percent,
                            "rating": rating,
                            "is_bestseller": is_bestseller,
                            "category": category_name,
                            "source": "Jumia",
                            "url": product_url,
                            "timestamp": datetime.now(),
                            "volume_value": normalized_units["value"],
                            "volume_unit": normalized_units["unit"]
                        }
                        
                        page_products.append(product)
                        
                    except Exception as e:
                        self.logger.error(f"Error parsing product card: {str(e)}")
                
                return page_products
        
        # Create tasks for all pages
        tasks = [scrape_page(url) for url in page_urls]
        results = await asyncio.gather(*tasks)
        
        # Flatten the list of lists
        for page_products in results:
            products.extend(page_products)
        
        self.logger.info(f"Scraped {len(products)} products from {category_name}")
        return products
    
    async def scrape_data_async(self):
        """
        Asynchronously scrape product data from all categories.
        
        Returns:
            list: List of product dictionaries
        """
        all_products = []
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                tasks = []
                
                # Create a task for each category
                for category_name, category_url in self.categories.items():
                    tasks.append(self.scrape_category(category_name, category_url, session))
                
                # Run all category scraping tasks concurrently
                results = await asyncio.gather(*tasks)
                
                # Combine products from all categories
                for products in results:
                    all_products.extend(products)
                
            self.logger.info(f"Successfully scraped {len(all_products)} products from Jumia")
            
        except Exception as e:
            self.logger.error(f"Error during Jumia scraping: {str(e)}")
        
        return all_products