"""
Configuration file for all data sources by category.
This allows for centralized management of scraping targets.
"""

# Nigerian e-commerce websites organized by category
E_COMMERCE_SOURCES = {
    "General E-commerce": [
        {"name": "Jumia", "url": "https://www.jumia.com.ng", "scraper": "JumiaScraper"},
        {"name": "Konga", "url": "https://www.konga.com", "scraper": "KongaScraper"},
        {"name": "Jiji", "url": "https://www.jiji.ng", "scraper": "JijiScraper"},
        {"name": "PayPorte", "url": "https://www.payporte.com", "scraper": "PayPorteScraper"},
        {"name": "Kara", "url": "https://kara.com.ng", "scraper": "KaraScraper"},
        {"name": "Slot", "url": "https://www.slot.ng", "scraper": "SlotScraper"},
        {"name": "AjeboMarket", "url": "https://www.ajebomarket.com", "scraper": "AjeboMarketScraper"},
        {"name": "Obeezi", "url": "https://www.obeezi.com", "scraper": "ObeeziScraper"},
        {"name": "ChrisVicMall", "url": "https://www.chrisvicmall.com", "scraper": "ChrisVicMallScraper"},
        {"name": "SuperMart", "url": "https://www.supermart.ng", "scraper": "SuperMartScraper"},
    ],
    
    "Electronics": [
        {"name": "Fouani", "url": "https://www.fouani.com", "scraper": "FouaniScraper"},
        {"name": "3CHub", "url": "https://www.3chub.com", "scraper": "ThreeCHubScraper"},
        {"name": "ParktelOnline", "url": "https://www.parktelonline.com", "scraper": "ParktelOnlineScraper"},
        {"name": "JustFones", "url": "https://www.justfones.ng", "scraper": "JustFonesScraper"},
        {"name": "PointekOnline", "url": "https://www.pointekonline.com", "scraper": "PointekOnlineScraper"},
    ],
    
    "Groceries & Food": [
        {"name": "FoodCo", "url": "https://foodco.ng", "scraper": "FoodCoScraper"},
        {"name": "PricePally", "url": "https://www.pricepally.com", "scraper": "PricePallyScraper"},
        {"name": "SuperMart", "url": "https://www.supermart.ng", "scraper": "SuperMartScraper"},
        {"name": "MyChopChop", "url": "https://www.mychopchop.ng", "scraper": "MyChopChopScraper"},
        {"name": "Ofadaa", "url": "https://www.ofadaa.com", "scraper": "OfadaaScraper"},
    ],
    
    "Fashion & Beauty": [
        {"name": "ZikelCosmetics", "url": "https://www.zikelcosmetics.com", "scraper": "ZikelCosmeticsScraper"},
        {"name": "RuffnTumbleKids", "url": "https://www.ruffntumblekids.com", "scraper": "RuffnTumbleKidsScraper"},
        {"name": "TheLadyMaker", "url": "https://www.theladymaker.com", "scraper": "TheLadyMakerScraper"},
        {"name": "PoloAvenue", "url": "https://www.poloavenue.com", "scraper": "PoloAvenueScraper"},
        {"name": "Shopaholic", "url": "https://shopaholic.com.ng", "scraper": "ShopaholicScraper"},
    ],
    
    "Home & Furniture": [
        {"name": "BedmateFurniture", "url": "https://www.bedmatefurniture.com.ng", "scraper": "BedmateFurnitureScraper"},
        {"name": "LifemateNigeria", "url": "https://lifematenigeria.com", "scraper": "LifemateNigeriaScraper"},
        {"name": "AvenueMall", "url": "https://avenuemall.com.ng", "scraper": "AvenueMallScraper"},
        {"name": "DreamsFurnitures", "url": "https://www.dreamsfurnitures.com", "scraper": "DreamsFurnituresScraper"},
        {"name": "ZRTales", "url": "https://www.zrtales.com", "scraper": "ZRTalesScraper"},
    ],
    
    "Books & Stationery": [
        {"name": "LaternaBooks", "url": "https://www.laternabooks.com", "scraper": "LaternaBooksScraperScraper"},
        {"name": "CSSBookshops", "url": "https://cssbookshops.com", "scraper": "CSSBookshopsScraper"},
        {"name": "BookvilleWorld", "url": "https://bookvilleworld.com", "scraper": "BookvilleWorldScraper"},
    ],
    
    "Health & Pharmacy": [
        {"name": "OneHealthNG", "url": "https://www.onehealthng.com", "scraper": "OneHealthNGScraper"},
        {"name": "MyMedicines", "url": "https://mymedicines.ng", "scraper": "MyMedicinesScraper"},
        {"name": "HealthPlusNigeria", "url": "https://www.healthplusnigeria.com", "scraper": "HealthPlusNigeriaScraper"},
        {"name": "MedPlusNig", "url": "https://www.medplusnig.com", "scraper": "MedPlusNigScraper"},
    ],
    
    "Automotive": [
        {"name": "GZ-Supplies", "url": "https://www.gz-supplies.com", "scraper": "GZSuppliesScraper"},
        {"name": "Autochek", "url": "https://www.autochek.africa/ng", "scraper": "AutochekScraper"},
        {"name": "JijiCars", "url": "https://jiji.ng/cars", "scraper": "JijiCarsScraper"},
    ],
    
    "Wholesale & B2B": [
        {"name": "Kusnap", "url": "https://www.kusnap.com", "scraper": "KusnapScraper"},
        {"name": "VConnect", "url": "https://www.vconnect.com", "scraper": "VConnectScraper"},
        {"name": "Tofa", "url": "https://www.tofa.com.ng", "scraper": "TofaScraper"},
    ],
    
    "Official & Data": [
        {"name": "NBS", "url": "https://www.nigerianstat.gov.ng", "scraper": "NBSScraper"},
        {"name": "SMEdenMarket", "url": "https://smedenmarket.ng", "scraper": "SMEdenMarketScraper"},
        {"name": "NEPC", "url": "https://www.nepc.gov.ng", "scraper": "NEPCScraper"},
        {"name": "Nairametrics", "url": "https://nairametrics.com", "scraper": "NairametricsScraper"},
        {"name": "Proshare", "url": "https://www.proshareng.com", "scraper": "ProshareScraper"},
        {"name": "BusinessDay", "url": "https://businessday.ng", "scraper": "BusinessDayScraper"},
        {"name": "TechpointMarketplace", "url": "https://marketplace.techpoint.africa", "scraper": "TechpointMarketplaceScraper"},
    ],
}

# URLs for NBS data
NBS_DATA_SOURCES = {
    "inflation": "https://nigerianstat.gov.ng/elibrary/read/1245",
    "cpi": "https://nigerianstat.gov.ng/elibrary/read/1244",
    "gdp": "https://nigerianstat.gov.ng/elibrary/read/1243",
    "household": "https://nigerianstat.gov.ng/elibrary/read/1242",
    "trade": "https://nigerianstat.gov.ng/elibrary/read/1241",
    "poverty": "https://nigerianstat.gov.ng/elibrary/read/1240",
    "employment": "https://nigerianstat.gov.ng/elibrary/read/1239",
    "ecommerce": "https://nigerianstat.gov.ng/elibrary/read/1238",
}

# Helper functions for accessing source configuration
def get_all_sources():
    """
    Get all e-commerce sources as a flat list
    
    Returns:
        list: All source configs
    """
    all_sources = []
    for category, sources in E_COMMERCE_SOURCES.items():
        all_sources.extend(sources)
    return all_sources

def get_sources_by_category(category):
    """
    Get sources for a specific category
    
    Args:
        category (str): Category name
        
    Returns:
        list: Sources for the category
    """
    return E_COMMERCE_SOURCES.get(category, [])

def get_source_by_name(name):
    """
    Get source config by name
    
    Args:
        name (str): Source name
        
    Returns:
        dict: Source config or None if not found
    """
    for source in get_all_sources():
        if source["name"] == name:
            return source
    return None