"""
Scraper factory module responsible for dynamically selecting and instantiating scrapers
based on configuration.
"""
import importlib
import logging
from typing import Dict, List, Any, Optional

from scrapers.base_scraper import BaseScraper
from scrapers.async_base_scraper import AsyncBaseScraper
from config.sources import get_all_sources, get_sources_by_category, get_source_by_name

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ScraperFactory')

def get_scraper_by_name(name: str) -> Optional[BaseScraper]:
    """
    Get a scraper instance by name.
    
    Args:
        name (str): Name of the scraper class to instantiate
        
    Returns:
        BaseScraper: Instantiated scraper or None if not found
    """
    try:
        source_config = get_source_by_name(name)
        if not source_config:
            logger.warning(f"No source configuration found for {name}")
            return None
            
        scraper_class_name = source_config["scraper"]
        
        try:
            # Try to import specific scraper implementation
            module = importlib.import_module(f"scrapers.{scraper_class_name.lower()}")
            scraper_class = getattr(module, scraper_class_name)
            
            # Instantiate and return
            scraper = scraper_class()
            logger.info(f"Successfully loaded dedicated scraper for {name}")
            return scraper
            
        except (ImportError, AttributeError) as e:
            logger.warning(f"Specific scraper for {name} not found: {str(e)}")
            logger.info(f"Using generic template scraper for {name}")
            
            # Import generic scraper template as fallback
            try:
                # Try async template first
                from scrapers.async_ecommerce_scraper_template import AsyncEcommerceScraper
                
                # Create a class on the fly with the correct name
                scraper_class = type(scraper_class_name, (AsyncEcommerceScraper,), {
                    "source_name": name,
                    "base_url": source_config["url"],
                })
                return scraper_class()
                
            except ImportError:
                # Fall back to regular template
                from scrapers.ecommerce_scraper_template import EcommerceScraper
                
                # Create a class on the fly with the correct name
                scraper_class = type(scraper_class_name, (EcommerceScraper,), {
                    "source_name": name,
                    "base_url": source_config["url"],
                })
                return scraper_class()
        
    except Exception as e:
        logger.error(f"Failed to load scraper for {name}: {str(e)}")
        return None

def get_all_scrapers() -> List[BaseScraper]:
    """
    Get all available scrapers as instances.
    
    Returns:
        list: List of instantiated scrapers
    """
    scrapers = []
    
    for source in get_all_sources():
        scraper = get_scraper_by_name(source["name"])
        if scraper:
            scrapers.append(scraper)
    
    return scrapers

def get_scrapers_by_category(category: str) -> List[BaseScraper]:
    """
    Get scrapers for a specific category.
    
    Args:
        category (str): Category name
        
    Returns:
        list: Scrapers for the category
    """
    scrapers = []
    
    for source in get_sources_by_category(category):
        scraper = get_scraper_by_name(source["name"])
        if scraper:
            scrapers.append(scraper)
    
    return scrapers

def scrape_all(async_mode: bool = True) -> List[Dict[str, Any]]:
    """
    Run all available scrapers and combine their results.
    
    Args:
        async_mode (bool): Whether to use asynchronous scraping if available
        
    Returns:
        list: Combined results from all scrapers
    """
    scrapers = get_all_scrapers()
    all_results = []
    
    if async_mode:
        # Separate async and regular scrapers
        async_scrapers = [s for s in scrapers if isinstance(s, AsyncBaseScraper)]
        regular_scrapers = [s for s in scrapers if not isinstance(s, AsyncBaseScraper)]
        
        # Run async scrapers
        import asyncio
        for scraper in async_scrapers:
            logger.info(f"Running async scraper: {scraper.__class__.__name__}")
            results = scraper.scrape_data()  # This calls the sync wrapper
            all_results.extend(results)
            
        # Run regular scrapers
        for scraper in regular_scrapers:
            logger.info(f"Running regular scraper: {scraper.__class__.__name__}")
            results = scraper.scrape_data()
            all_results.extend(results)
    else:
        # Run all in regular mode
        for scraper in scrapers:
            logger.info(f"Running scraper: {scraper.__class__.__name__}")
            results = scraper.scrape_data()
            all_results.extend(results)
    
    return all_results

def scrape_by_category(category: str, async_mode: bool = True) -> List[Dict[str, Any]]:
    """
    Run scrapers for a specific category and combine their results.
    
    Args:
        category (str): Category name
        async_mode (bool): Whether to use asynchronous scraping if available
        
    Returns:
        list: Combined results from category scrapers
    """
    scrapers = get_scrapers_by_category(category)
    category_results = []
    
    if async_mode:
        # Separate async and regular scrapers
        async_scrapers = [s for s in scrapers if isinstance(s, AsyncBaseScraper)]
        regular_scrapers = [s for s in scrapers if not isinstance(s, AsyncBaseScraper)]
        
        # Run async scrapers
        import asyncio
        for scraper in async_scrapers:
            logger.info(f"Running async scraper for {category}: {scraper.__class__.__name__}")
            results = scraper.scrape_data()  # This calls the sync wrapper
            category_results.extend(results)
            
        # Run regular scrapers
        for scraper in regular_scrapers:
            logger.info(f"Running regular scraper for {category}: {scraper.__class__.__name__}")
            results = scraper.scrape_data()
            category_results.extend(results)
    else:
        # Run all in regular mode
        for scraper in scrapers:
            logger.info(f"Running scraper for {category}: {scraper.__class__.__name__}")
            results = scraper.scrape_data()
            category_results.extend(results)
    
    return category_results