"""
Trade Parser Module - Updated for GS Trade File Format
Converts GS format trade files to position format for netting with positions
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import logging
import re

logger = logging.getLogger(__name__)

@dataclass
class Trade:
    """Represents a single trade"""
    symbol: str
    expiry_date: datetime
    security_type: str  # Futures, Call, Put
    strike_price: float
    side: str  # B or S
    quantity: float  # In lots
    price: float
    tm_name: str
    instrument: str  # OPTSTK, OPTIDX, FUTSTK, FUTIDX


class TradeParser:
    """Parse GS format trade files and convert to position format"""
    
    # Column mappings for GS trade file format (0-indexed)
    TRADE_COLUMNS = {
        "tm_name": 3,        # Column D: TM NAME
        "instrument": 4,     # Column E: INSTR  
        "symbol": 5,         # Column F: Symbol
        "expiry": 6,         # Column G: Expiry Date
        "strike": 8,         # Column I: Strike Price
        "option_type": 9,    # Column J: Option Type (CE/PE)
        "side": 10,          # Column K: B/S (Buy/Sell)
        "quantity": 12,      # Column M: QTY
        "price": 13          # Column N: Avg Price
    }
    
    def __init__(self, mapping_file: str = "futures_mapping.csv"):
        """Initialize with symbol mapping"""
        try:
            # Try different possible names for the mapping file
            self.symbol_mappings = {}
            self.normalized_mappings = {}
            
            # Try to load mapping file with different names
            possible_names = ['futures_mapping.csv', 'futures mapping.csv']
            mapping_loaded = False
            
            for name in possible_names:
                try:
                    df = pd.read_csv(name)
                    self._load_mappings_from_df(df)
                    mapping_loaded = True
                    logger.info(f"Loaded mappings from {name}")
                    break
                except:
                    continue
            
            if not mapping_loaded:
                logger.warning("Could not load futures mapping file, using defaults")
                self._load_default_mappings()
                
        except Exception as e:
            logger.error(f"Error initializing trade parser: {e}")
            self._load_default_mappings()
        
        self.unmapped_trades = []
    
    def _load_mappings_from_df(self, df):
        """Load mappings from dataframe"""
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
                    if symbol.upper() in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
                        underlying = f"{symbol.upper()} INDEX"
                    else:
                        underlying = f"{ticker} IS Equity"
                
                lot_size = 1
                if len(row) > 4 and pd.notna(row.iloc[4]):
                    try:
                        lot_size = int(float(str(row.iloc[4]).strip()))
                    except:
                        lot_size = 1
                
                mapping = {
                    'ticker': ticker,
                    'underlying': underlying,
                    'lot_size': lot_size,
                    'original_symbol': symbol
                }
                self.symbol_mappings[symbol] = mapping
                self.normalized_mappings[symbol.upper()] = mapping
    
    def _load_default_mappings(self):
        """Load default mappings for common indices"""
        defaults = {
            'NIFTY': {'ticker': 'NIFTY', 'underlying': 'NIFTY INDEX', 'lot_size': 50},
            'BANKNIFTY': {'ticker': 'NSEBANK', 'underlying': 'BANKNIFTY INDEX', 'lot_size': 25},
            'FINNIFTY': {'ticker': 'FINNIFTY', 'underlying': 'FINNIFTY INDEX', 'lot_size': 25},
            'MIDCPNIFTY': {'ticker': 'NMIDSELP', 'underlying': 'MIDCPNIFTY INDEX', 'lot_size': 50}
        }
        self.symbol_mappings = defaults
        self.normalized_mappings = {k.upper(): v for k, v in defaults.items()}
    
    def parse_trade_file(self, file_path: str, password: Optional[str] = None) -> List[Trade]:
        """Parse GS format trade file and return list of trades"""
        try:
            # Read the file
            df = self._read_file(file_path, password)
            
            if df is None or df.empty:
                logger.error("Could not read trade file or file is empty")
                return []
            
            logger.info(f"Trade file shape: {df.shape}")
            
            trades = []
            data_started = False
            
            # Start from row 0 and look for data
            for idx in range(len(df)):
                try:
                    row = df.iloc[idx]
                    
                    # Skip if row doesn't have enough columns
                    if len(row) < 14:
                        continue
                    
                    # Check instrument column (column 4, 0-indexed)
                    if pd.isna(row.iloc[4]):
                        continue
                        
                    instrument = str(row.iloc[4]).strip().upper()
                    
                    # Skip header rows
                    if 'INSTR' in instrument or 'INSTRUMENT' in instrument:
                        data_started = True
                        logger.info(f"Found header at row {idx}")
                        continue
                    
                    # Check if this is a valid instrument type
                    if instrument not in ['OPTSTK', 'OPTIDX', 'FUTSTK', 'FUTIDX']:
                        if data_started:
                            logger.debug(f"Row {idx}: Invalid instrument '{instrument}'")
                        continue
                    
                    data_started = True
                    
                    # Parse the trade
                    trade = self._create_trade_from_row(row)
                    if trade:
                        trades.append(trade)
                        logger.debug(f"Added trade: {trade.symbol} {trade.side} {trade.quantity}")
                        
                except Exception as e:
                    logger.debug(f"Error parsing trade row {idx}: {e}")
                    continue
            
            logger.info(f"Parsed {len(trades)} trades from file")
            
            # Log sample of trades for debugging
            if trades:
                logger.info(f"Sample trades: {trades[:3]}")
            
            return trades
            
        except Exception as e:
            logger.error(f"Error parsing trade file: {e}")
            return []
    
    def _read_file(self, file_path: str, password: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Read Excel file with optional password support"""
        try:
            # First try without password
            df = pd.read_excel(file_path, header=None)
            logger.info(f"Successfully read file without password")
            return df
        except Exception as e:
            if 'encrypt' in str(e).lower():
                # Try with passwords
                passwords = ['Aurigin2017', 'Aurigin2024']
                if password:
                    passwords.insert(0, password)
                
                for pwd in passwords:
                    try:
                        import msoffcrypto
                        import io
                        
                        decrypted = io.BytesIO()
                        with open(file_path, 'rb') as f:
                            file = msoffcrypto.OfficeFile(f)
                            file.load_key(password=pwd)
                            file.decrypt(decrypted)
                        
                        decrypted.seek(0)
                        df = pd.read_excel(decrypted, header=None)
                        logger.info(f"Successfully read file with password")
                        return df
                    except:
                        continue
            else:
                logger.error(f"Error reading file: {e}")
                raise
        
        return None
    
    def _create_trade_from_row(self, row: pd.Series) -> Optional[Trade]:
        """Create Trade object from dataframe row"""
        try:
            # Extract trade details
            tm_name = str(row.iloc[self.TRADE_COLUMNS["tm_name"]]).strip() if pd.notna(row.iloc[self.TRADE_COLUMNS["tm_name"]]) else ""
            instrument = str(row.iloc[self.TRADE_COLUMNS["instrument"]]).strip().upper()
            symbol = str(row.iloc[self.TRADE_COLUMNS["symbol"]]).strip().upper() if pd.notna(row.iloc[self.TRADE_COLUMNS["symbol"]]) else ""
            
            # Parse expiry
            expiry_val = row.iloc[self.TRADE_COLUMNS["expiry"]]
            if pd.isna(expiry_val):
                logger.debug(f"No expiry date for symbol {symbol}")
                return None
            
            # Handle expiry date
            if isinstance(expiry_val, datetime):
                expiry = expiry_val
            elif isinstance(expiry_val, str):
                expiry = self._parse_date(expiry_val)
            else:
                expiry = pd.to_datetime(expiry_val)
            
            if not expiry:
                logger.debug(f"Could not parse expiry date: {expiry_val}")
                return None
            
            # Parse side
            side_val = row.iloc[self.TRADE_COLUMNS["side"]]
            if pd.isna(side_val):
                return None
            side = str(side_val).strip().upper()
            
            # Validate side
            if side not in ['B', 'S', 'BUY', 'SELL']:
                logger.debug(f"Invalid side: {side}")
                return None
            
            # Normalize side to B or S
            if side in ['BUY', 'B']:
                side = 'B'
            else:
                side = 'S'
            
            # Parse quantity
            try:
                quantity = float(row.iloc[self.TRADE_COLUMNS["quantity"]])
                if quantity == 0:
                    return None
            except:
                logger.debug(f"Invalid quantity: {row.iloc[self.TRADE_COLUMNS['quantity']]}")
                return None
            
            # Parse price
            try:
                price = float(row.iloc[self.TRADE_COLUMNS["price"]]) if pd.notna(row.iloc[self.TRADE_COLUMNS["price"]]) else 0
            except:
                price = 0
            
            # Determine security type and strike
            if 'FUT' in instrument:
                security_type = 'Futures'
                strike_price = 0
            else:  # Options
                option_type_val = row.iloc[self.TRADE_COLUMNS["option_type"]]
                if pd.isna(option_type_val):
                    logger.debug(f"No option type for option trade")
                    return None
                    
                option_type = str(option_type_val).strip().upper()
                
                # Parse strike
                strike_val = row.iloc[self.TRADE_COLUMNS["strike"]]
                if pd.notna(strike_val):
                    strike_str = str(strike_val).replace(',', '')
                    try:
                        strike_price = float(strike_str)
                    except:
                        strike_price = 0
                else:
                    strike_price = 0
                
                # Determine Call or Put
                if option_type in ['CE', 'C', 'CALL'] or option_type.startswith('C'):
                    security_type = 'Call'
                elif option_type in ['PE', 'P', 'PUT'] or option_type.startswith('P'):
                    security_type = 'Put'
                else:
                    logger.debug(f"Unknown option type: {option_type}")
                    return None
            
            return Trade(
                symbol=symbol,
                expiry_date=expiry,
                security_type=security_type,
                strike_price=strike_price,
                side=side,
                quantity=quantity,
                price=price,
                tm_name=tm_name,
                instrument=instrument
            )
            
        except Exception as e:
            logger.debug(f"Error creating trade from row: {e}")
            return None
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime"""
        date_str = str(date_str).strip().upper()
        
        # Try standard datetime parsing first
        try:
            return pd.to_datetime(date_str)
        except:
            pass
        
        # Try specific formats
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        
        # Try DD-MMM-YYYY format
        match = re.match(r'(\d{1,2})[- ]([A-Z]{3})[- ](\d{4})', date_str)
        if match:
            day = int(match.group(1))
            month = month_map.get(match.group(2), 0)
            year = int(match.group(3))
            if month:
                return datetime(year, month, day)
        
        return None
    
    def convert_trades_to_positions(self, trades: List[Trade]) -> Tuple[List, List[Dict]]:
        """
        Convert trades to position format for netting
        Returns: (positions, unmapped_trades)
        """
        if not trades:
            return [], []
        
        # Import Position class
        try:
            from input_parser import Position
        except ImportError:
            logger.error("Could not import Position class")
            return [], []
        
        # Group trades by unique key
        trade_groups = {}
        unmapped_trades = []
        
        for trade in trades:
            # Check if symbol is mapped
            mapping = None
            if trade.symbol in self.symbol_mappings:
                mapping = self.symbol_mappings[trade.symbol]
            elif trade.symbol in self.normalized_mappings:
                mapping = self.normalized_mappings[trade.symbol]
            
            if not mapping:
                unmapped_trades.append({
                    'symbol': trade.symbol,
                    'expiry': trade.expiry_date,
                    'type': trade.security_type,
                    'strike': trade.strike_price,
                    'side': trade.side,
                    'quantity': trade.quantity
                })
                logger.warning(f"Unmapped symbol: {trade.symbol}")
                continue
            
            # Create position key
            key = (
                mapping['underlying'],
                trade.symbol,
                trade.expiry_date.date(),
                trade.security_type,
                trade.strike_price
            )
            
            if key not in trade_groups:
                trade_groups[key] = {
                    'net_quantity': 0,
                    'mapping': mapping,
                    'expiry': trade.expiry_date
                }
            
            # Net the quantity - Buy adds, Sell subtracts
            if trade.side == 'B':
                trade_groups[key]['net_quantity'] += trade.quantity
            else:  # Sell
                trade_groups[key]['net_quantity'] -= trade.quantity
        
        # Convert to Position objects
        positions = []
        
        # Import month code mapping
        MONTH_CODE = {
            1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
            7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"
        }
        
        for (underlying, symbol, expiry_date, sec_type, strike), group_data in trade_groups.items():
            net_qty = group_data['net_quantity']
            mapping = group_data['mapping']
            expiry = group_data['expiry']
            
            # Skip zero net positions
            if abs(net_qty) < 0.0001:
                continue
            
            # Generate Bloomberg ticker
            ticker = mapping['ticker']
            if sec_type == 'Futures':
                month_code = MONTH_CODE.get(expiry.month, "")
                year_code = str(expiry.year)[-1]
                bloomberg_ticker = f"{ticker}={month_code}{year_code} IS Equity"
            else:
                date_str = expiry.strftime('%m/%d/%y')
                strike_str = str(int(strike)) if strike == int(strike) else str(strike)
                if sec_type == 'Call':
                    bloomberg_ticker = f"{ticker} IS {date_str} C{strike_str} Equity"
                else:
                    bloomberg_ticker = f"{ticker} IS {date_str} P{strike_str} Equity"
            
            # Create position
            position = Position(
                underlying_ticker=underlying,
                bloomberg_ticker=bloomberg_ticker,
                symbol=symbol,
                expiry_date=expiry,
                position_lots=net_qty,
                security_type=sec_type,
                strike_price=strike,
                lot_size=mapping.get('lot_size', 1)
            )
            
            positions.append(position)
        
        logger.info(f"Converted {len(trades)} trades to {len(positions)} net positions")
        if unmapped_trades:
            logger.warning(f"Found {len(unmapped_trades)} unmapped trade symbols")
        
        return positions, unmapped_trades
    
    def create_trade_summary(self, trades: List[Trade]) -> pd.DataFrame:
        """Create a summary dataframe of all trades"""
        if not trades:
            return pd.DataFrame()
        
        data = []
        for trade in trades:
            data.append({
                'TM Name': trade.tm_name,
                'Symbol': trade.symbol,
                'Expiry': trade.expiry_date.strftime('%Y-%m-%d'),
                'Type': trade.security_type,
                'Strike': trade.strike_price if trade.strike_price > 0 else '',
                'Side': trade.side,
                'Quantity': trade.quantity,
                'Price': trade.price
            })
        
        return pd.DataFrame(data)


def net_positions_with_trades(original_positions: List, 
                              trade_positions: List) -> List:
    """
    Net original positions with trades to get final positions
    """
    # Create position map with unique key
    position_map = {}
    
    # Add original positions
    for pos in original_positions:
        key = (
            pos.underlying_ticker,
            pos.symbol,
            pos.expiry_date.date(),
            pos.security_type,
            pos.strike_price
        )
        position_map[key] = pos
    
    # Net with trade positions
    for trade_pos in trade_positions:
        key = (
            trade_pos.underlying_ticker,
            trade_pos.symbol,
            trade_pos.expiry_date.date(),
            trade_pos.security_type,
            trade_pos.strike_price
        )
        
        if key in position_map:
            # Update existing position
            position_map[key].position_lots += trade_pos.position_lots
        else:
            # New position from trade
            position_map[key] = trade_pos
    
    # Filter out zero positions
    final_positions = [p for p in position_map.values() if abs(p.position_lots) > 0.0001]
    
    # Log summary
    logger.info(f"Position netting summary:")
    logger.info(f"  Original positions: {len(original_positions)}")
    logger.info(f"  Trade positions: {len(trade_positions)}")
    logger.info(f"  Final non-zero positions: {len(final_positions)}")
    
    return final_positions
