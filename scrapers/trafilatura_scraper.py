"""
Trafilatura-based web scraper for extracting clean text content from websites.
"""

import trafilatura
from typing import List, Dict, Any, Optional
import pandas as pd
import logging
import os
import json
from datetime import datetime
from .base_scraper import BaseScraper

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TrafilaturaScraper')

class TrafilaturaScraper(BaseScraper):
    """
    A specialized scraper that uses Trafilatura to extract clean text content
    from websites. This is useful for article content, blog posts, news, and
    other text-heavy content that would be hard to extract with standard HTML parsing.
    """
    
    def __init__(self):
        """
        Initialize the Trafilatura scraper.
        """
        super().__init__()
        self.base_url = ""  # Not tied to a single base URL
        self.name = "Trafilatura Content Scraper"
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
            logger.info(f"Extracting content from: {url}")
            downloaded = trafilatura.fetch_url(url)
            
            if downloaded is None:
                logger.warning(f"Failed to download content from: {url}")
                return ""
                
            text = trafilatura.extract(downloaded)
            
            if text is None:
                logger.warning(f"Failed to extract text from: {url}")
                return ""
                
            logger.info(f"Successfully extracted {len(text)} characters from: {url}")
            return text
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
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
        if source_names is None:
            source_names = [f"Source {i+1}" for i in range(len(urls))]
        
        # Ensure we have same number of names as URLs
        if len(source_names) != len(urls):
            source_names = source_names[:len(urls)]
            # If still not enough, pad with generic names
            if len(source_names) < len(urls):
                source_names.extend([f"Source {i+1+len(source_names)}" for i in range(len(urls) - len(source_names))])
        
        for url, source_name in zip(urls, source_names):
            content = self.get_website_text_content(url)
            
            if content:
                result = {
                    "source_name": source_name,
                    "url": url,
                    "content": content,
                    "timestamp": datetime.now(),
                    "word_count": len(content.split()),
                    "character_count": len(content)
                }
                results.append(result)
                self.scraped_content.append(result)
        
        logger.info(f"Scraped content from {len(results)} out of {len(urls)} URLs")
        return results
    
    def save_to_csv(self, filepath: str = "data/scraped_content.csv") -> bool:
        """
        Save the scraped content to a CSV file.
        
        Args:
            filepath (str): Path to save the CSV file
            
        Returns:
            bool: Success status
        """
        if not self.scraped_content:
            logger.warning("No content to save")
            return False
            
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # Create DataFrame and save
            df = pd.DataFrame(self.scraped_content)
            
            # Truncate content to avoid enormous CSV files
            df["content_preview"] = df["content"].apply(lambda x: x[:500] + "..." if len(x) > 500 else x)
            
            # Save with minimal content for preview purposes
            preview_df = df.drop(columns=["content"])
            preview_df.to_csv(filepath, index=False)
            
            # Also save full content as JSON for each source
            for i, row in df.iterrows():
                source_filename = f"data/content_{row['source_name'].replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d')}.json"
                with open(source_filename, 'w', encoding='utf-8') as f:
                    json.dump({
                        "source_name": row["source_name"],
                        "url": row["url"],
                        "content": row["content"],
                        "timestamp": row["timestamp"].isoformat(),
                        "word_count": row["word_count"],
                        "character_count": row["character_count"]
                    }, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved content preview to {filepath} and full content as JSON files")
            return True
        except Exception as e:
            logger.error(f"Error saving content to {filepath}: {str(e)}")
            return False
    
    def scrape_data(self) -> List[Dict[str, Any]]:
        """
        Implementation of the abstract method from BaseScraper.
        In this case, it returns any previously scraped content.
        
        Returns:
            list: List of dictionaries containing scraped content
        """
        return self.scraped_content