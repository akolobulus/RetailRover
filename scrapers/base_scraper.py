import requests
from bs4 import BeautifulSoup
import pandas as pd
from abc import ABC, abstractmethod
import time
import random
import logging
from typing import List, Dict, Any, Optional, Union

class BaseScraper(ABC):
    """
    Abstract base class for all scraper implementations.
    Defines the common interface and helper methods for web scraping.
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
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_page(self, url, params=None, max_retries=3, retry_delay=2):
        """
        Fetch a web page with retry logic.
        
        Args:
            url (str): The URL to fetch
            params (dict, optional): Query parameters to include
            max_retries (int): Maximum number of retry attempts
            retry_delay (int): Base delay between retries in seconds
            
        Returns:
            BeautifulSoup: Parsed HTML content
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.logger.info(f"Fetching {url}")
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return BeautifulSoup(response.text, 'html.parser')
                elif response.status_code == 403 or response.status_code == 429:
                    self.logger.warning(f"Rate limited (status {response.status_code}). Retrying after delay.")
                    retry_count += 1
                    # Exponential backoff with jitter
                    sleep_time = retry_delay * (2 ** retry_count) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    self.logger.error(f"Failed to fetch {url}. Status code: {response.status_code}")
                    return None
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request error for {url}: {str(e)}")
                retry_count += 1
                time.sleep(retry_delay)
        
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
    
    @abstractmethod
    def scrape_data(self) -> List[Dict[str, Any]]:
        """
        Main method to scrape data from a website.
        Must be implemented by all concrete scraper classes.
        
        Returns:
            list: List of product dictionaries
        """
        pass
