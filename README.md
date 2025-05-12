# RetailRover NG: Nigerian E-commerce Analytics Dashboard

*Where data meets retail instinct*

A comprehensive data analytics pipeline and visualization dashboard that scrapes Nigerian e-commerce websites to identify fast-selling products across categories. The system provides actionable market insights through an interactive Streamlit dashboard with retail price recommendations.

![Dashboard Screenshot](https://cdn.pixabay.com/photo/2019/10/16/09/09/ecommerce-4554313_1280.jpg)

## Project Overview

This dashboard provides a comprehensive view of the Nigerian e-commerce market by:

1. **Data Collection**: Scraping product data from 50 Nigerian retail websites across various categories
2. **Data Processing**: Cleaning, deduplicating, and normalizing product information
3. **Analytics**: Identifying trends, popular products, and market opportunities
4. **Recommendations**: Suggesting high-potential products based on ratings, reviews, and cross-site popularity
5. **Visualization**: Presenting insights through interactive charts and tables

## Data Sources

The system collects data from multiple types of Nigerian e-commerce and retail websites:

### General E-commerce
- Jumia Nigeria
- Konga
- Jiji
- PayPorte
- Kara
- Slot
- AjeboMarket
- Obeezi
- ChrisVicMall
- SuperMart

### Industry-Specific Retailers
- **Electronics**: Fouani, 3CHub, ParktelOnline, JustFones, PointekOnline
- **Groceries & Food**: FoodCo, PricePally, SuperMart, MyChopChop, Ofadaa
- **Fashion & Beauty**: ZikelCosmetics, RuffnTumbleKids, TheLadyMaker, PoloAvenue, Shopaholic
- **Home & Furniture**: BedmateFurniture, LifemateNigeria, AvenueMall, DreamsFurnitures, ZRTales
- **Books & Stationery**: LaternaBooks, CSSBookshops, BookvilleWorld
- **Health & Pharmacy**: OneHealthNG, MyMedicines, HealthPlusNigeria, MedPlusNig
- **Automotive**: GZ-Supplies, Autochek, JijiCars
- **Wholesale & B2B**: Kusnap, VConnect, Tofa

### Official & Data Sources
- National Bureau of Statistics (NBS)
- SMEdenMarket
- NEPC
- Nairametrics
- Proshare
- BusinessDay
- Techpoint Marketplace

## Technical Architecture

### Scraping Framework

The system uses a modular, extensible scraping architecture with both synchronous and asynchronous capabilities:

- **Base Scraper Class**: Common interface for all scrapers with shared utility methods
- **Async Base Scraper**: Asynchronous version using `aiohttp` for parallel requests
- **Scraper Factory**: Dynamically loads and manages all scraper implementations
- **E-commerce Scraper Templates**: Templated classes that make adding new sites quick and easy

### Data Processing

Data processing is handled by a dedicated pipeline that:

1. Normalizes prices and units
2. Categorizes products consistently across different sites
3. Deduplicates similar products using fuzzy matching
4. Creates standardized data structures for analytics
5. Maintains historical data for trend analysis

### Recommendation Engine

The recommendation system uses several metrics to identify promising products:

- **Rating**: Product quality rating (40% weight)
- **Review Count**: Number of customer reviews (30% weight)
- **Site Count**: Appearances across multiple e-commerce sites (30% weight)
- **Discount Factor**: Bonus for products on sale (10% bonus)
- **Availability**: Penalty for out-of-stock items (50% reduction)

Products are ranked within their categories to provide targeted recommendations. The system also calculates a recommended retail price for each product based on a 5% markup from the average market price across all sources.

For each category, the dashboard shows the top 5 recommended products with:
- Current market price 
- Recommended retail price
- Product rating and source
- Cross-site popularity metrics

### Dashboard

Built with Streamlit, RetailRover NG dashboard offers:

- Sleek green-themed interface with both light and dark mode support
- Real-time data collection and processing
- Advanced filtering by category, price, source, and time period
- Interactive visualizations using Plotly
- Text search with multiple modes (contains, fuzzy match, exact match)
- Product recommendations with suggested retail prices (5% markup)
- Export capabilities in CSV, Excel, and JSON formats
- Website content scraping for market reports and news analysis

## Installation & Usage

### Prerequisites
- Python 3.10+
- Required packages (via pyproject.toml)

### Setup and Run (Replit-specific)
1. Fork this Replit project
2. The environment is already configured for you
3. Click the "Run" button to start the Streamlit server
4. Access the dashboard at the provided URL

### Local Setup (non-Replit)
1. Clone the repository
2. Install dependencies: `pip install -e .`
3. Run the dashboard: `streamlit run app.py --server.port 5000`

## Future Improvements

- **Machine Learning Integration**:
  - Price prediction models
  - Product categorization using NLP
  - Customer sentiment analysis from reviews

- **Real-time API Integration**:
  - Connect with e-commerce APIs for real-time inventory 
  - Payment gateway integration for market analysis
  - Logistics/delivery service data for geographic insights

- **Enhanced Analytics**:
  - Seasonality analysis for product demand
  - Geographic distribution of product popularity
  - Competitor pricing analysis and alerting

- **Data Enhancements**:
  - Image-based product matching
  - Customer review semantic analysis
  - Mobile marketplace integration

## License

This project is licensed under the MIT License.

## Acknowledgments

- Data from Nigerian e-commerce platforms
- National Bureau of Statistics for economic indicators
- Streamlit for the interactive dashboard capabilities