import logging
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from datetime import datetime
import trafilatura
from scrapers.base_scraper import BaseScraper


class TrafilaturaScraper(BaseScraper):
    """
    A specialized scraper that uses Trafilatura to extract clean text content
    from websites. This is useful for article content, blog posts, news, and
    other text-heavy content that would be hard to extract with standard HTML parsing.
    """
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize list to store scraped content
        self.scraped_content = []
        
    def get_website_text_content(self, url: str) -> str:
        """
        Extract the main text content from a website using Trafilatura.
        
        Args:
            url (str): The URL of the website to scrape
            
        Returns:
            str: The extracted text content
        """
        try:
            self.logger.info(f"Fetching content from {url}")
            downloaded = trafilatura.fetch_url(url)
            
            if not downloaded:
                self.logger.warning(f"Failed to download content from {url}")
                return ""
            
            text = trafilatura.extract(downloaded)
            
            if not text:
                self.logger.warning(f"No text content extracted from {url}")
                return ""
                
            self.logger.info(f"Successfully extracted content from {url} ({len(text)} characters)")
            return text
            
        except Exception as e:
            self.logger.error(f"Error extracting content from {url}: {str(e)}")
            return ""
    
    def scrape_urls(self, urls: List[str], source_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Scrape text content from multiple URLs.
        
        Args:
            urls (list): List of URLs to scrape
            source_names (list, optional): Names of the sources corresponding to URLs
            
        Returns:
            list: List of dictionaries containing scraped content
        """
        results = []
        
        # If source names not provided, use URLs as names
        if not source_names:
            source_names = urls
        
        # Ensure we have matching number of names and urls
        if len(source_names) != len(urls):
            self.logger.warning("Mismatch between number of URLs and source names. Using URLs as source names.")
            source_names = urls
        
        for i, url in enumerate(urls):
            try:
                content = self.get_website_text_content(url)
                
                if content:
                    # Add the content to results
                    results.append({
                        "source": source_names[i],
                        "url": url,
                        "content": content,
                        "content_length": len(content),
                        "extraction_time": datetime.now(),
                        "success": True
                    })
                else:
                    # Add failed extraction to results
                    results.append({
                        "source": source_names[i],
                        "url": url,
                        "content": "",
                        "content_length": 0,
                        "extraction_time": datetime.now(),
                        "success": False
                    })
            except Exception as e:
                self.logger.error(f"Error processing {url}: {str(e)}")
                
                # Add error information to results
                results.append({
                    "source": source_names[i],
                    "url": url,
                    "content": "",
                    "content_length": 0,
                    "extraction_time": datetime.now(),
                    "success": False,
                    "error": str(e)
                })
        
        # Store results
        self.scraped_content = results
        return results
    
    def save_to_csv(self, filepath: str = "data/scraped_content.csv") -> bool:
        """
        Save the scraped content to a CSV file.
        
        Args:
            filepath (str): Path to save the CSV file
            
        Returns:
            bool: Success status
        """
        try:
            if not self.scraped_content:
                self.logger.warning("No content to save")
                return False
            
            df = pd.DataFrame(self.scraped_content)
            df.to_csv(filepath, index=False)
            self.logger.info(f"Scraped content saved to {filepath}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving scraped content: {str(e)}")
            return False
    
    def scrape_data(self) -> List[Dict[str, Any]]:
        """
        Implementation of the abstract method from BaseScraper.
        In this case, it returns any previously scraped content.
        
        Returns:
            list: List of dictionaries containing scraped content
        """
        if not self.scraped_content:
            self.logger.warning("No content has been scraped yet. Call scrape_urls() first.")
        
        return self.scraped_content