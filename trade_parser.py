"""
Trade Parser Module
Converts GS format trade files to position format for netting with positions
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import logging
import re
from input_parser import Position, InputParser, MONTH_CODE

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
    
    # Column mappings for GS trade file format
    TRADE_COLUMNS = {
        "tm_name": 3,        # Column 4: TM NAME
        "instrument": 4,     # Column 5: INSTR
        "symbol": 5,         # Column 6: Symbol
        "expiry": 6,         # Column 7: Expiry Date
        "strike": 8,         # Column 9: Strike Price
        "option_type": 9,    # Column 10: Option Type (CE/PE)
        "side": 10,          # Column 11: B/S (Buy/Sell)
        "quantity": 12,      # Column 13: QTY
        "price": 13          # Column 14: Avg Price
    }
    
    def __init__(self, mapping_file: str = "futures mapping.csv"):
        """Initialize with symbol mapping"""
        self.input_parser = InputParser(mapping_file)
        self.symbol_mappings = self.input_parser.symbol_mappings
        self.normalized_mappings = self.input_parser.normalized_mappings
        self.unmapped_trades = []
        
    def parse_trade_file(self, file_path: str, password: Optional[str] = None) -> List[Trade]:
        """Parse GS format trade file and return list of trades"""
        try:
            # Read the file (similar to input_parser logic)
            df = self._read_file(file_path, password)
            
            if df is None or df.empty:
                logger.error("Could not read trade file or file is empty")
                return []
            
            trades = []
            data_started = False
            
            for idx in range(len(df)):
                try:
                    row = df.iloc[idx]
                    
                    # Skip if row doesn't have enough columns
                    if len(row) < 14:
                        continue
                    
                    # Check if this is a valid data row
                    instrument = str(row[self.TRADE_COLUMNS["instrument"]]).strip().upper()
                    
                    # Skip non-trade rows
                    if instrument not in ['OPTSTK', 'OPTIDX', 'FUTSTK', 'FUTIDX']:
                        # Check if it's a header row
                        if not data_started and 'INSTR' in instrument:
                            data_started = True
                        continue
                    
                    data_started = True
                    
                    # Parse the trade
                    trade = self._create_trade_from_row(row)
                    if trade:
                        trades.append(trade)
                        
                except Exception as e:
                    logger.debug(f"Error parsing trade row {idx}: {e}")
                    continue
            
            logger.info(f"Parsed {len(trades)} trades from file")
            return trades
            
        except Exception as e:
            logger.error(f"Error parsing trade file: {e}")
            return []
    
    def _read_file(self, file_path: str, password: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Read Excel file with optional password support"""
        if file_path.endswith(('.xls', '.xlsx')):
            passwords = ['Aurigin2017', 'Aurigin2024', None]
            if password:
                passwords.insert(0, password)
            
            for pwd in passwords:
                try:
                    if pwd:
                        import msoffcrypto
                        import io
                        
                        decrypted = io.BytesIO()
                        with open(file_path, 'rb') as f:
                            file = msoffcrypto.OfficeFile(f)
                            file.load_key(password=pwd)
                            file.decrypt(decrypted)
                        
                        decrypted.seek(0)
                        return pd.read_excel(decrypted, header=None)
                    else:
                        return pd.read_excel(file_path, header=None)
                except Exception as e:
                    if 'encrypted' not in str(e).lower() and pwd is None:
                        logger.error(f"Error reading file: {e}")
                        raise
                    continue
        else:
            # CSV file
            return pd.read_csv(file_path, header=None)
        
        return None
    
    def _create_trade_from_row(self, row: pd.Series) -> Optional[Trade]:
        """Create Trade object from dataframe row"""
        try:
            # Extract trade details
            tm_name = str(row[self.TRADE_COLUMNS["tm_name"]]).strip()
            instrument = str(row[self.TRADE_COLUMNS["instrument"]]).strip().upper()
            symbol = str(row[self.TRADE_COLUMNS["symbol"]]).strip().upper()
            expiry_str = str(row[self.TRADE_COLUMNS["expiry"]]).strip()
            side = str(row[self.TRADE_COLUMNS["side"]]).strip().upper()
            
            # Parse quantity and price
            try:
                quantity = float(row[self.TRADE_COLUMNS["quantity"]])
            except:
                quantity = 0
                
            try:
                price = float(row[self.TRADE_COLUMNS["price"]])
            except:
                price = 0
            
            # Skip trades with zero quantity
            if quantity == 0:
                return None
            
            # Parse expiry date
            expiry = self.input_parser._parse_date(expiry_str)
            if not expiry:
                logger.warning(f"Could not parse expiry date: {expiry_str}")
                return None
            
            # Determine security type and strike
            if 'FUT' in instrument:
                security_type = 'Futures'
                strike_price = 0
            else:  # Options
                option_type = str(row[self.TRADE_COLUMNS["option_type"]]).strip().upper()
                strike_str = str(row[self.TRADE_COLUMNS["strike"]]).strip()
                
                # Clean strike price
                strike_str = strike_str.replace(',', '')
                try:
                    strike_price = float(strike_str)
                except:
                    strike_price = 0
                
                # Determine Call or Put
                if option_type in ['CE', 'C', 'CALL'] or option_type.startswith('C'):
                    security_type = 'Call'
                elif option_type in ['PE', 'P', 'PUT'] or option_type.startswith('P'):
                    security_type = 'Put'
                else:
                    logger.warning(f"Unknown option type: {option_type}")
                    return None
            
            # Validate side
            if side not in ['B', 'S']:
                logger.warning(f"Invalid side: {side}")
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
    
    def convert_trades_to_positions(self, trades: List[Trade]) -> Tuple[List[Position], List[Dict]]:
        """
        Convert trades to position format for netting
        Returns: (positions, unmapped_trades)
        """
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
                continue
            
            # Determine series based on instrument
            if 'STK' in trade.instrument:
                series = 'FUTSTK' if trade.security_type == 'Futures' else 'OPTSTK'
            else:
                series = 'FUTIDX' if trade.security_type == 'Futures' else 'OPTIDX'
            
            # Create position key
            key = (
                mapping['underlying'],  # underlying_ticker
                trade.symbol,           # symbol
                trade.expiry_date,
                trade.security_type,
                trade.strike_price,
                series
            )
            
            if key not in trade_groups:
                trade_groups[key] = {
                    'net_quantity': 0,
                    'mapping': mapping,
                    'trades': []
                }
            
            # Net the quantity - Buy adds, Sell subtracts
            if trade.side == 'B':
                trade_groups[key]['net_quantity'] += trade.quantity
            else:  # Sell
                trade_groups[key]['net_quantity'] -= trade.quantity
            
            trade_groups[key]['trades'].append(trade)
        
        # Convert to Position objects
        positions = []
        
        for (underlying, symbol, expiry, sec_type, strike, series), group_data in trade_groups.items():
            net_qty = group_data['net_quantity']
            mapping = group_data['mapping']
            
            # Skip zero net positions
            if abs(net_qty) < 0.0001:
                continue
            
            # Generate Bloomberg ticker
            bloomberg_ticker = self.input_parser._generate_bloomberg_ticker(
                mapping['ticker'], expiry, sec_type, strike
            )
            
            # Create position (can be negative for net short)
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


def net_positions_with_trades(original_positions: List[Position], 
                              trade_positions: List[Position]) -> List[Position]:
    """
    Net original positions with trades to get final positions
    
    Args:
        original_positions: List of original positions
        trade_positions: List of net positions from trades
        
    Returns:
        List of final positions after netting
    """
    # Create position map with unique key
    position_map = {}
    
    # Add original positions
    for pos in original_positions:
        key = (
            pos.underlying_ticker,
            pos.symbol,
            pos.expiry_date.date(),  # Use date only for matching
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
    
    # Return all positions (including shorts/zeros for transparency)
    final_positions = list(position_map.values())
    
    # Log summary
    original_count = len(original_positions)
    trade_count = len(trade_positions)
    final_count = len([p for p in final_positions if abs(p.position_lots) > 0.0001])
    zero_count = len([p for p in final_positions if abs(p.position_lots) <= 0.0001])
    short_count = len([p for p in final_positions if p.position_lots < -0.0001])
    
    logger.info(f"Position netting summary:")
    logger.info(f"  Original positions: {original_count}")
    logger.info(f"  Trade positions: {trade_count}")
    logger.info(f"  Final non-zero positions: {final_count}")
    logger.info(f"  Zero positions: {zero_count}")
    logger.info(f"  Short positions: {short_count}")
    
    # Filter out zero positions
    final_positions = [p for p in final_positions if abs(p.position_lots) > 0.0001]
    
    return final_positions