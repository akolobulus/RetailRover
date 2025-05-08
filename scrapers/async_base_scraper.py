import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from abc import ABC, abstractmethod
import time
import random
import logging
import asyncio
from typing import List, Dict, Any, Optional, Union

class AsyncBaseScraper(ABC):
    """
    Abstract base class for asynchronous scraper implementations.
    Defines the common interface and helper methods for web scraping using asyncio.
    """
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
        }
        # Session will be created when needed, not in __init__
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def get_page(self, url, session, params=None, max_retries=3, retry_delay=2):
        """
        Asynchronously fetch a web page with retry logic.
        
        Args:
            url (str): The URL to fetch
            session (aiohttp.ClientSession): The aiohttp session
            params (dict, optional): Query parameters to include
            max_retries (int): Maximum number of retry attempts
            retry_delay (int): Base delay between retries in seconds
            
        Returns:
            BeautifulSoup: Parsed HTML content or None if failed
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.logger.info(f"Fetching {url}")
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        html = await response.text()
                        return BeautifulSoup(html, 'html.parser')
                    elif response.status == 403 or response.status == 429:
                        self.logger.warning(f"Rate limited (status {response.status}). Retrying after delay.")
                        retry_count += 1
                        # Exponential backoff with jitter
                        sleep_time = retry_delay * (2 ** retry_count) + random.uniform(0, 1)
                        await asyncio.sleep(sleep_time)
                    else:
                        self.logger.error(f"Failed to fetch {url}. Status code: {response.status}")
                        return None
            except Exception as e:
                self.logger.error(f"Request error for {url}: {str(e)}")
                retry_count += 1
                await asyncio.sleep(retry_delay)
        
        self.logger.error(f"Max retries reached for {url}")
        return None
    
    def extract_text(self, element, selector, default=""):
        """
        Safely extract text from an HTML element.
        
        Args:
            element: BeautifulSoup element to search within
            selector (str): CSS selector to find the target element
            default: Default value to return if element not found
            
        Returns:
            str: Extracted text or default value
        """
        try:
            found = element.select_one(selector)
            if found:
                return found.get_text(strip=True)
            return default
        except Exception as e:
            self.logger.warning(f"Error extracting text with selector '{selector}': {str(e)}")
            return default
    
    def extract_price(self, text):
        """
        Extract and normalize price from text.
        
        Args:
            text (str): Text containing price information
            
        Returns:
            float: Normalized price value
        """
        if not text:
            return 0.0
            
        # Remove currency symbols and commas
        import re
        price_text = re.sub(r'[^\d.]', '', text.replace(',', ''))
        
        try:
            return float(price_text)
        except ValueError:
            self.logger.warning(f"Could not convert price text to float: {text}")
            return 0.0
    
    def normalize_units(self, text):
        """
        Normalize product units to standard formats.
        
        Args:
            text (str): Text containing unit information (e.g., "500ml", "0.5L")
            
        Returns:
            dict: Dictionary with value and standardized unit
        """
        if not text:
            return {"value": 0, "unit": ""}
        
        # Extract numbers and units
        import re
        value_match = re.search(r'(\d+\.?\d*)', text)
        unit_match = re.search(r'([a-zA-Z]+)', text)
        
        if not value_match:
            return {"value": 0, "unit": ""}
        
        value = float(value_match.group(1))
        unit = unit_match.group(1).lower() if unit_match else ""
        
        # Convert units to standard format
        # Volume units to milliliters
        if unit in ['l', 'ltr', 'litre', 'liter', 'liters', 'litres']:
            return {"value": value * 1000, "unit": "ml"}
        elif unit in ['cl', 'centiliter', 'centilitre']:
            return {"value": value * 10, "unit": "ml"}
        elif unit in ['ml', 'milliliter', 'millilitre']:
            return {"value": value, "unit": "ml"}
        
        # Weight units to grams
        elif unit in ['kg', 'kilo', 'kilos', 'kilogram', 'kilograms']:
            return {"value": value * 1000, "unit": "g"}
        elif unit in ['g', 'gram', 'grams', 'gm']:
            return {"value": value, "unit": "g"}
        elif unit in ['mg', 'milligram', 'milligrams']:
            return {"value": value / 1000, "unit": "g"}
        
        return {"value": value, "unit": unit}
    
    async def scrape_urls(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        Asynchronously scrape multiple URLs.
        
        Args:
            urls (list): List of URLs to scrape
            
        Returns:
            list: List of BeautifulSoup objects for each URL
        """
        results = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [self.get_page(url, session) for url in urls]
            soups = await asyncio.gather(*tasks)
            
            for url, soup in zip(urls, soups):
                if soup is not None:
                    results.append({"url": url, "soup": soup})
        
        return results
    
    @abstractmethod
    async def scrape_data_async(self) -> List[Dict[str, Any]]:
        """
        Main method to asynchronously scrape data from a website.
        Must be implemented by all concrete scraper classes.
        
        Returns:
            list: List of product dictionaries
        """
        pass
    
    def scrape_data(self) -> List[Dict[str, Any]]:
        """
        Synchronous wrapper around the async scrape_data_async method.
        This makes the class compatible with existing code.
        
        Returns:
            list: List of product dictionaries
        """
        return asyncio.run(self.scrape_data_async())