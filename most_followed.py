#!/usr/bin/env python3
"""
Most Followed Uranium Stocks Fetcher - Process 1
Fetches stock data for uranium stocks across regions
"""

import threading
import time
import yfinance as yf
from datetime import datetime, timedelta
import os
from insert_function import insert_most_followed_stock
from database_config import get_curser

server_config = os.getenv("server_config")

# Custom ticker mappings for Yahoo Finance
custom_mappings = {
    'GLATF': 'GLATF',
    'UEC': 'UEC',
    'PALAF': 'PALAF',
    'GVXXF': 'GVXXF',
    'URAX': 'URAX',
    '1164.HK': '1164.HK',
    'EU': 'EU',
    'NATKY': 'NATKY',
    'SPUT': 'SPUT.TO',
    'PDN': 'PDN.AX',
    'ABA.V': 'ABA.V',
    'ANLDF': 'ANLDF',
    'LI.V': 'LI.V',
    'MAW.V': 'MAW.V',
    'MWSNF': 'MWSNF',
    'URG': 'URG',
    'FMC.V': 'FMC.V',
    'DMX.V': 'DMX.V',
    'SASK.V': 'SASK.V',
    'THB.AX': 'THB.AX',
    'AEE.AX': 'AEE.AX',
    'BKY.AX': 'BKY.AX',
    'LAM.AX': 'LAM.AX',
    'DYL.AX': 'DYL.AX',
    'DEV.AX': 'DEV.AX',
    'BOE.AX': 'BOE.AX',
    'BMN.AX': 'BMN.AX',
    'EL8.AX': 'EL8.AX',
    'PDN.AX': 'PDN.AX',
}

# Exchange suffix mappings for Yahoo Finance
exchange_mappings = {
    'TSXV': '.V',
    'TSX.V': '.V',
    'TSX': '.TO',
    'NYSE': '',
    'NYSE Arca': '',
    'LSE': '.L',
    'LONDON': '.L',
    'ASX': '.AX',
    'CNE': '.CN',
    'BRUSSELS': '.BR',
    'TOKYO': '.T',
    'HKEX': '.HK',
    'OTC': ''
}

# Column 1: Most Watched Uranium Stocks
most_watched = [
    {"Name": "Global Atomic Corporation", "Country": "Canada", "Ticker": "GLATF", "tv_ticker": "GLATF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Uranium Energy Corporation", "Country": "United States", "Ticker": "UEC", "tv_ticker": "UEC", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Paladin Energy Limited", "Country": "Australia", "Ticker": "PALAF", "tv_ticker": "PALAF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "GoviEx Uranium Inc", "Country": "Canada", "Ticker": "GVXXF", "tv_ticker": "GVXXF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Defiance Daily Target 2X Long Uranium", "Country": "United States", "Ticker": "URAX", "tv_ticker": "URAX", "Stock exchange": "NYSE Arca", "stock_exchange_tv": "AMEX"},
    {"Name": "CGN Mining Company Limited", "Country": "China", "Ticker": "1164.HK", "tv_ticker": "1164.HK", "Stock exchange": "HKEX", "stock_exchange_tv": "HKEX"},
    {"Name": "enCore Energy Corporation", "Country": "United States", "Ticker": "EU", "tv_ticker": "EU", "Stock exchange": "NASDAQ", "stock_exchange_tv": "NASDAQ"},
    {"Name": "Kazatomprom", "Country": "Kazakhstan", "Ticker": "NATKY", "tv_ticker": "NATKY", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Sprott Physical Uranium Trust", "Country": "Canada", "Ticker": "SPUT", "tv_ticker": "SPUT", "Stock exchange": "TSX", "stock_exchange_tv": "TSX"},
    {"Name": "Paladin Energy", "Country": "Australia", "Ticker": "PDN", "tv_ticker": "PDN", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
]

# Column 2: North American Uranium Leaders
north_american_leaders = [
    {"Name": "GoviEx Uranium Inc", "Country": "Canada", "Ticker": "GVXXF", "tv_ticker": "GVXXF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Abasca Resources Inc.", "Country": "Canada", "Ticker": "ABA.V", "tv_ticker": "ABA.V", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Anfield Energy Inc.", "Country": "Canada", "Ticker": "ANLDF", "tv_ticker": "ANLDF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "American Lithium Corp.", "Country": "Canada", "Ticker": "LI.V", "tv_ticker": "LI.V", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Mawson Gold Ltd.", "Country": "Canada", "Ticker": "MAW.V", "tv_ticker": "MAW.V", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "Mawson Gold Ltd.", "Country": "Canada", "Ticker": "MWSNF", "tv_ticker": "MWSNF", "Stock exchange": "OTC", "stock_exchange_tv": "OTC"},
    {"Name": "Ur-Energy", "Country": "Canada", "Ticker": "URG", "tv_ticker": "URG", "Stock exchange": "NYSE", "stock_exchange_tv": "NYSE"},
    {"Name": "Forum Energy Metals Corp.", "Country": "Canada", "Ticker": "FMC.V", "tv_ticker": "FMC.V", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "District Metals Corp.", "Country": "Canada", "Ticker": "DMX.V", "tv_ticker": "DMX.V", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
    {"Name": "ATHA Energy Corp.", "Country": "Canada", "Ticker": "SASK.V", "tv_ticker": "SASK.V", "Stock exchange": "TSX.V", "stock_exchange_tv": "TSXV"},
]

# Column 3: ASX Uranium Market Leaders
global_market_leaders = [
    {"Name": "Lotus Resources Limited", "Country": "Australia", "Ticker": "THB.AX", "tv_ticker": "THB.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Aura Energy Limited", "Country": "Australia", "Ticker": "AEE.AX", "tv_ticker": "AEE.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Berkeley Energia Ltd.", "Country": "Australia", "Ticker": "BKY.AX", "tv_ticker": "BKY.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Laramide Resources", "Country": "Australia", "Ticker": "LAM.AX", "tv_ticker": "LAM.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Deep Yellow Limited", "Country": "Australia", "Ticker": "DYL.AX", "tv_ticker": "DYL.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "DevEx Resources Ltd.", "Country": "Australia", "Ticker": "DEV.AX", "tv_ticker": "DEV.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Boss Energy", "Country": "Australia", "Ticker": "BOE.AX", "tv_ticker": "BOE.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Bannerman Energy", "Country": "Australia", "Ticker": "BMN.AX", "tv_ticker": "BMN.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Elevate Uranium", "Country": "Australia", "Ticker": "EL8.AX", "tv_ticker": "EL8.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
    {"Name": "Paladin Energy", "Country": "Australia", "Ticker": "PDN.AX", "tv_ticker": "PDN.AX", "Stock exchange": "ASX", "stock_exchange_tv": "ASX"},
]

# Combine all stocks for processing
most_followed_stocks = most_watched + north_american_leaders + global_market_leaders


def get_yahoo_ticker(ticker, exchange):
    """
    Get the correct Yahoo Finance ticker using custom mappings and exchange suffixes.
    """
    # First check if there's a custom mapping
    if ticker in custom_mappings:
        return custom_mappings[ticker]
    
    # If no custom mapping, use exchange mapping
    base_ticker = ticker.split('.')[0]  # Remove any existing suffix
    
    # Map exchange to suffix
    suffix = exchange_mappings.get(exchange, '')
    
    return f"{base_ticker}{suffix}"


def process_stock_data(cursor, connection, stockdata):
    """
    Inserts or updates a single stock data into the database using the upsert function.
    """
    print("DOne 3")
    try:
        insert_most_followed_stock(
            cursor=cursor,
            connection=connection,
            name=stockdata.get("name"),
            ticker=stockdata.get("ticker"),
            open_price=stockdata.get("open_price"),
            close_price=stockdata.get("close_price"),
            intraday_percentage=stockdata.get("intraday_percentage"),
            current_price=stockdata.get("current_price"),
            intraday_change=stockdata.get("intraday_change"),
            seven_day_change=stockdata.get("seven_day_change"),
            seven_day_percentage=stockdata.get("seven_day_percentage"),
            volume=stockdata.get("volume"),
            country=stockdata.get("country"),
            stock_exchange=stockdata.get("stock_exchange"),
            stock_type=stockdata.get("stocks_type"),
        )
    except Exception as e:
        print(f"Error processing stock data for {stockdata.get('ticker')}: {e}")


def calculate_percentage_change(start, end):
    """Calculate percentage change between two values."""
    if start and end and start != 0:
        return ((end - start) / start) * 100
    return None

stock_data = []

def get_stock_data_from_yfinance(ticker):
    """Get stock data using yfinance library."""
    try:
        # Let yfinance handle the session - don't pass custom session
        data = yf.Ticker(ticker)
        
        # Get current market data
        current_info = data.info
        
        # Get historical data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)  # 7-day range
        hist = data.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"No historical data available for {ticker}")
            return None
        
        # Extract data
        open_price = hist['Open'].iloc[-1] if 'Open' in hist.columns and len(hist['Open']) > 0 else None
        close_price = hist['Close'].iloc[-1] if 'Close' in hist.columns and len(hist['Close']) > 0 else None
        current_price = close_price  # Use close price as current price
        
        first_close = hist['Close'].iloc[0] if 'Close' in hist.columns and len(hist['Close']) > 0 else None
        volume = hist['Volume'].iloc[-1] if 'Volume' in hist.columns and len(hist['Volume']) > 0 else None
        
        # Calculate changes
        intraday_change = current_price - open_price if open_price and current_price else None
        intraday_percentage = calculate_percentage_change(open_price, current_price)
        seven_day_change = current_price - first_close if first_close and current_price else None
        seven_day_percentage = calculate_percentage_change(first_close, current_price)
        
        return {
            "price": current_price,
            "open_price": open_price,
            "close_price": close_price,
            "intraday_change": intraday_change,
            "intraday_percentage": intraday_percentage,
            "seven_day_change": seven_day_change,
            "seven_day_percentage": seven_day_percentage,
            "volume": volume
        }
        
    except Exception as e:
        print(f"Error getting data for {ticker}: {e}")
        return None


def process_stock_category(cursor, connection, stocks, category_name):
    global stock_data
    for stock in stocks:
        ticker = stock["Ticker"]
        exchange = stock["Stock exchange"]
        
        # Get the correct Yahoo Finance ticker
        yahoo_ticker = get_yahoo_ticker(ticker, exchange)
        
        try:
            # Get stock data from yfinance using the mapped ticker
            stock_info_data = get_stock_data_from_yfinance(yahoo_ticker)
            
            if stock_info_data:
                stock_info = {
                    "name": stock["Name"],
                    "ticker": stock["Ticker"],
                    "open_price": round(stock_info_data["open_price"], 2) if stock_info_data["open_price"] else None,
                    "close_price": stock_info_data["close_price"] if stock_info_data["close_price"] else None,
                    "current_price": round(stock_info_data["price"], 2) if stock_info_data["price"] else None,
                    "intraday_change": round(stock_info_data["intraday_change"], 2) if stock_info_data["intraday_change"] else None,
                    "intraday_percentage": round(stock_info_data["intraday_percentage"], 2) if stock_info_data["intraday_percentage"] else None,
                    "seven_day_change": round(stock_info_data["seven_day_change"], 2) if stock_info_data["seven_day_change"] else None,
                    "seven_day_percentage": round(stock_info_data["seven_day_percentage"], 2) if stock_info_data["seven_day_percentage"] else None,
                    "volume": stock_info_data["volume"],
                    "country": stock["Country"],
                    "stock_exchange": stock["Stock exchange"],
                    "stocks_type": category_name
                }

                process_stock_data(cursor, connection, stock_info)
                stock_data.append(stock_info)
            else:
                print(f"No data available for {yahoo_ticker} (original: {ticker})")
        except Exception as e:
            print(f"Error processing stock {stock['Name']} ({yahoo_ticker}): {str(e)}")

def get_most_followed_data():
    # Get database connection
    connection, cursor = get_curser()
    
    try:
        # Process all categories
        process_stock_category(cursor, connection, most_watched, "most_watched")
        process_stock_category(cursor, connection, north_american_leaders, "north_american_leaders")
        process_stock_category(cursor, connection, global_market_leaders, "global_market_leaders")

        print("Scraped Data:")
        print(stock_data)

        return stock_data
    
    finally:
        # Close database connection
        cursor.close()
        connection.close()
