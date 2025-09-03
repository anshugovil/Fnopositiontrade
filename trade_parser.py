"""
Trade Parser Module
Handles parsing of GS and MS trade files and converts them to position format
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
import logging
from input_parser import Position, MONTH_CODE

logger = logging.getLogger(__name__)


class TradeParser:
    """Parser for trade files (GS and MS formats)"""
    
    def __init__(self, mapping_file: str = "futures mapping.csv"):
        self.mapping_file = mapping_file
        self.symbol_mappings = self._load_mappings()
        self.trades = []
        self.format_type = None
        
    def _load_mappings(self) -> Dict:
        """Load symbol mappings from CSV"""
        mappings = {}
        normalized_mappings = {}
        
        try:
            df = pd.read_csv(self.mapping_file)
            for idx, row in df.iterrows():
                if pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
                    symbol = str(row.iloc[0]).strip()
                    ticker = str(row.iloc[1]).strip()
                    
                    # Handle underlying (column 3)
                    underlying = None
                    if len(row) > 2 and pd.notna(row.iloc[2]):
                        underlying_val = str(row.iloc[2]).strip()
                        if underlying_val and underlying_val.upper() != 'NAN':
                            underlying = underlying_val
                    
                    # If no underlying specified, create default
                    if not underlying:
                        # Special handling for known indices
                        if symbol.upper() in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
                            underlying = f"{symbol.upper()} INDEX"
                        else:
                            underlying = f"{ticker} IS Equity"
                    
                    lot_size = 1
                    if len(row) > 4 and pd.notna(row.iloc[4]):
                        try:
                            lot_size = int(float(str(row.iloc[4]).strip()))
                        except (ValueError, TypeError):
                            lot_size = 1
                    
                    mapping = {
                        'ticker': ticker,
                        'underlying': underlying,
                        'lot_size': lot_size,
                        'original_symbol': symbol
                    }
                    mappings[symbol] = mapping
                    normalized_mappings[symbol.upper()] = mapping
            
            self.normalized_mappings = normalized_mappings
            logger.info(f"Loaded {len(mappings)} symbol mappings for trade parser")
            
        except Exception as e:
            logger.error(f"Error loading mapping file: {e}")
            self.normalized_mappings = {}
            
        return mappings
    
    def detect_format(self, df: pd.DataFrame) -> str:
        """Detect if it's MS or GS trade format"""
        # MS format detection - 14 columns, specific structure
        if df.shape[1] == 14:
            # Check for MS column structure
            try:
                # MS has Instr in column 4 (index 4)
                col4_vals = df.iloc[:, 4].dropna().astype(str).str.upper()
                if any(val in ['OPTSTK', 'OPTIDX', 'FUTSTK', 'FUTIDX'] for val in col4_vals):
                    logger.info("Detected MS trade format (14 columns)")
                    return 'MS'
            except:
                pass
        
        # GS format detection - needs to be implemented based on actual GS structure
        # For now, if not MS, assume GS
        logger.info("Detected GS trade format (non-MS structure)")
        return 'GS'
    
    def parse_trade_file(self, file_path: str) -> List[Position]:
        """Parse trade file and convert to positions"""
        # Read file
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, header=None if self._has_no_header(file_path) else 0)
        else:
            df = pd.read_excel(file_path, header=None if self._has_no_header(file_path) else 0)
        
        # Detect format
        self.format_type = self.detect_format(df)
        
        if self.format_type == 'MS':
            return self._parse_ms_trades(df)
        else:
            return self._parse_gs_trades(df)
    
    def _has_no_header(self, file_path: str) -> bool:
        """Check if file has headers or not"""
        # Simple check - read first row and see if it looks like headers
        try:
            if file_path.endswith('.csv'):
                first_row = pd.read_csv(file_path, nrows=1)
            else:
                first_row = pd.read_excel(file_path, nrows=1)
            
            # If first row has strings like 'Symbol', 'Expiry', etc., it's likely headers
            first_vals = first_row.iloc[0].astype(str)
            header_keywords = ['symbol', 'expiry', 'strike', 'option', 'instr', 'qty', 'price']
            if any(keyword in str(first_vals).lower() for keyword in header_keywords):
                return False  # Has headers
            return True  # No headers
        except:
            return True  # Assume no headers if can't determine
    
    def _parse_ms_trades(self, df: pd.DataFrame) -> List[Position]:
        """Parse MS format trade file"""
        positions_map = {}
        
        # MS format columns (0-indexed):
        # 4: Instr (OPTSTK/OPTIDX/FUTSTK/FUTIDX)
        # 5: Symbol
        # 6: Expiry Dt
        # 7: Lot Size
        # 8: Strike Price
        # 9: Option Type (CE/PE)
        # 10: B/S (Buy/Sell)
        # 12: Lots Traded
        
        for idx in range(len(df)):
            try:
                row = df.iloc[idx]
                
                # Skip if not enough columns
                if len(row) < 14:
                    continue
                
                instr = str(row.iloc[4]).strip().upper() if pd.notna(row.iloc[4]) else ""
                if instr not in ['OPTSTK', 'OPTIDX', 'FUTSTK', 'FUTIDX']:
                    continue
                
                symbol = str(row.iloc[5]).strip().upper() if pd.notna(row.iloc[5]) else ""
                expiry_str = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ""
                lot_size = int(float(row.iloc[7])) if pd.notna(row.iloc[7]) else 1
                strike = float(row.iloc[8]) if pd.notna(row.iloc[8]) else 0
                option_type = str(row.iloc[9]).strip().upper() if pd.notna(row.iloc[9]) else ""
                side = str(row.iloc[10]).strip().upper() if pd.notna(row.iloc[10]) else ""
                lots = float(row.iloc[12]) if pd.notna(row.iloc[12]) else 0
                
                if lots == 0:
                    continue
                
                # Parse expiry date
                expiry = self._parse_date(expiry_str)
                if not expiry:
                    continue
                
                # Determine security type
                if 'FUT' in instr:
                    security_type = 'Futures'
                    strike = 0  # Futures don't have strikes
                elif option_type in ['CE', 'C', 'CALL']:
                    security_type = 'Call'
                elif option_type in ['PE', 'P', 'PUT']:
                    security_type = 'Put'
                else:
                    continue
                
                # Determine position sign (BUY = positive, SELL = negative)
                if side.startswith('B'):
                    position_lots = lots
                elif side.startswith('S'):
                    position_lots = -lots
                else:
                    continue
                
                # Get mapping
                mapping = self.normalized_mappings.get(symbol)
                if not mapping:
                    logger.warning(f"No mapping found for symbol: {symbol}")
                    continue
                
                # Generate Bloomberg ticker
                bloomberg_ticker = self._generate_bloomberg_ticker(
                    mapping['ticker'], expiry, security_type, strike, 
                    'IDX' in instr  # Is index instrument
                )
                
                # Create unique key for aggregation
                key = (mapping['underlying'], bloomberg_ticker, symbol, expiry, security_type, strike, lot_size)
                
                # Aggregate positions
                if key in positions_map:
                    positions_map[key] += position_lots
                else:
                    positions_map[key] = position_lots
                    
            except Exception as e:
                logger.debug(f"Error parsing MS trade row {idx}: {e}")
        
        # Convert to Position objects
        positions = []
        for (underlying, bloomberg, symbol, expiry, sec_type, strike, lot_size), lots in positions_map.items():
            positions.append(Position(
                underlying_ticker=underlying,
                bloomberg_ticker=bloomberg,
                symbol=symbol,
                expiry_date=expiry,
                position_lots=lots,  # Can be negative for net sells
                security_type=sec_type,
                strike_price=strike,
                lot_size=lot_size
            ))
        
        logger.info(f"Parsed {len(positions)} net positions from MS trades")
        return positions
    
    def _parse_gs_trades(self, df: pd.DataFrame) -> List[Position]:
        """Parse GS format trade file - to be implemented based on actual format"""
        positions_map = {}
        
        # GS format parsing logic
        # This needs to be implemented based on actual GS file structure
        # For now, returning empty list
        
        logger.warning("GS format parsing not yet fully implemented")
        return []
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string in various formats"""
        date_str = str(date_str).strip()
        
        # Try different date formats
        formats = [
            '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y',
            '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',
            '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',
            '%d/%m/%y', '%d-%m-%y', '%d.%m.%y',
            '%m/%d/%y', '%m-%d-%y', '%m.%d.%y',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except:
                continue
        
        # Try pandas parser as fallback
        try:
            return pd.to_datetime(date_str)
        except:
            return None
    
    def _generate_bloomberg_ticker(self, ticker: str, expiry: datetime,
                                  security_type: str, strike: float,
                                  is_index: bool = False) -> str:
        """Generate Bloomberg ticker format"""
        ticker_upper = ticker.upper()
        
        # Check if this is an index
        if is_index or ticker_upper in ['NZ', 'NBZ', 'NIFTY', 'BANKNIFTY']:
            is_index = True
        
        if security_type == 'Futures':
            month_code = MONTH_CODE.get(expiry.month, "")
            year_code = str(expiry.year)[-1]
            
            if is_index:
                return f"{ticker}{month_code}{year_code} Index"
            else:
                return f"{ticker}={month_code}{year_code} IS Equity"
        else:
            date_str = expiry.strftime('%m/%d/%y')
            strike_str = str(int(strike)) if strike == int(strike) else str(strike)
            
            if is_index:
                if security_type == 'Call':
                    return f"{ticker} {date_str} C{strike_str} Index"
                else:
                    return f"{ticker} {date_str} P{strike_str} Index"
            else:
                if security_type == 'Call':
                    return f"{ticker} IS {date_str} C{strike_str} Equity"
                else:
                    return f"{ticker} IS {date_str} P{strike_str} Equity"
