"""
Price Fetcher Module
Handles fetching prices from Yahoo Finance
"""

import yfinance as yf
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class PriceFetcher:
    """Fetch prices from Yahoo Finance"""
    
    def __init__(self):
        self.price_cache = {}
    
    def fetch_prices_for_symbols(self, symbols: List[str]) -> Dict[str, float]:
        """Fetch current prices for list of symbols"""
        prices = {}
        
        logger.info(f"Fetching prices for {len(symbols)} symbols from Yahoo Finance")
        
        for symbol in symbols:
            price_found = False
            
            # Special handling for Indian indices
            if symbol.upper() == 'NIFTY':
                yahoo_symbols = ['^NSEI']  # NIFTY 50 index
            elif symbol.upper() == 'BANKNIFTY':
                yahoo_symbols = ['^NSEBANK']  # Bank Nifty index
            elif symbol.upper() == 'FINNIFTY':
                yahoo_symbols = ['^CNXFIN']  # Nifty Financial Services
            elif symbol.upper() == 'MIDCPNIFTY':
                yahoo_symbols = ['^NSEMDCP50']  # Nifty Midcap 50
            else:
                # Regular stocks - try different Yahoo formats
                yahoo_symbols = [
                    f"{symbol}.NS",  # NSE
                    f"{symbol}.BO",  # BSE  
                    symbol           # Direct ticker
                ]
            
            for yahoo_symbol in yahoo_symbols:
                if price_found:
                    break
                    
                try:
                    ticker_obj = yf.Ticker(yahoo_symbol)
                    hist = ticker_obj.history(period="1d")
                    
                    if not hist.empty:
                        price = float(hist['Close'].iloc[-1])
                        if price and price > 0:
                            prices[symbol] = price
                            price_found = True
                            logger.debug(f"Found price for {symbol}: {price} using {yahoo_symbol}")
                    
                except Exception as e:
                    continue
            
            if not price_found:
                # Try fetching from info as fallback
                for yahoo_symbol in yahoo_symbols:
                    try:
                        ticker_obj = yf.Ticker(yahoo_symbol)
                        info = ticker_obj.info
                        
                        price = None
                        if 'currentPrice' in info and info['currentPrice']:
                            price = float(info['currentPrice'])
                        elif 'regularMarketPrice' in info and info['regularMarketPrice']:
                            price = float(info['regularMarketPrice'])
                        elif 'previousClose' in info and info['previousClose']:
                            price = float(info['previousClose'])
                        
                        if price and price > 0:
                            prices[symbol] = price
                            price_found = True
                            logger.debug(f"Found price for {symbol}: {price} from info")
                            break
                    except:
                        continue
            
            if not price_found:
                logger.warning(f"Could not fetch price for {symbol}")
        
        logger.info(f"Successfully fetched {len(prices)} out of {len(symbols)} prices")
        return prices