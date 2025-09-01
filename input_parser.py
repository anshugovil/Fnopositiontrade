"""
Input Parser Module
Handles parsing of different position file formats and symbol mapping
Supports both stock and index futures/options
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import re
import logging
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
MONTH_CODE = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"
}

@dataclass
class Position:
    """Represents a single position"""
    underlying_ticker: str
    bloomberg_ticker: str
    symbol: str
    expiry_date: datetime
    position_lots: float
    security_type: str  # Futures, Call, Put
    strike_price: float
    lot_size: int
    
    @property
    def is_future(self) -> bool:
        return self.security_type == 'Futures'
    
    @property
    def is_call(self) -> bool:
        return self.security_type == 'Call'
    
    @property
    def is_put(self) -> bool:
        return self.security_type == 'Put'


class InputParser:
    """Parser that handles all three input formats"""
    
    def __init__(self, mapping_file: str = "futures mapping.csv"):
        self.mapping_file = mapping_file
        self.normalized_mappings = {}
        self.symbol_mappings = self._load_mappings()
        self.positions = []
        self.unmapped_symbols = []
    
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
                        'original_symbol': symbol  # Keep original symbol for reference
                    }
                    mappings[symbol] = mapping
                    normalized_mappings[symbol.upper()] = mapping
            
            self.normalized_mappings = normalized_mappings
            logger.info(f"Loaded {len(mappings)} symbol mappings")
            
            # Log indices for debugging
            for sym in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
                if sym in mappings:
                    logger.info(f"{sym} mapped to: ticker={mappings[sym]['ticker']}, underlying={mappings[sym]['underlying']}")
                    
        except Exception as e:
            logger.error(f"Error loading mapping file: {e}")
            self.normalized_mappings = {}
            
        return mappings
    
    def parse_file(self, file_path: str) -> List[Position]:
        """Parse input file and return positions"""
        df = None
        
        # Try reading the file with different passwords if it's an Excel file
        if file_path.endswith(('.xls', '.xlsx')):
            passwords = ['Aurigin2017', 'Aurigin2024', None]  # Try these passwords first
            
            for pwd in passwords:
                try:
                    if pwd:
                        # Try with password
                        import msoffcrypto
                        import io
                        import tempfile
                        
                        decrypted = io.BytesIO()
                        with open(file_path, 'rb') as f:
                            file = msoffcrypto.OfficeFile(f)
                            file.load_key(password=pwd)
                            file.decrypt(decrypted)
                        
                        decrypted.seek(0)
                        df = pd.read_excel(decrypted, header=None)
                        logger.info(f"Successfully opened file with password")
                        break
                    else:
                        # Try without password
                        df = pd.read_excel(file_path, header=None)
                        break
                except Exception as e:
                    if 'encrypted' not in str(e).lower() and pwd is None:
                        # If it's not an encryption error and no password, it's some other issue
                        logger.error(f"Error reading file: {e}")
                        raise
                    continue
            
            # If still no success, prompt for password
            if df is None:
                import getpass
                user_pwd = getpass.getpass("Enter password for Excel file: ")
                try:
                    import msoffcrypto
                    import io
                    
                    decrypted = io.BytesIO()
                    with open(file_path, 'rb') as f:
                        file = msoffcrypto.OfficeFile(f)
                        file.load_key(password=user_pwd)
                        file.decrypt(decrypted)
                    
                    decrypted.seek(0)
                    df = pd.read_excel(decrypted, header=None)
                except Exception as e:
                    logger.error(f"Failed to open file with provided password: {e}")
                    return []
        else:
            # CSV file
            df = pd.read_csv(file_path, header=None)
        
        if df is None:
            logger.error("Could not read input file")
            return []
        
        format_type = self._detect_format(df)
        logger.info(f"Detected format: {format_type}")
        
        # Store format type for later use in naming
        self.format_type = format_type
        
        if format_type == 'BOD':
            return self._parse_bod(df)
        elif format_type == 'CONTRACT':
            return self._parse_contract(df)
        elif format_type == 'MS':
            return self._parse_ms(df)
        else:
            logger.error("Unknown file format")
            return []
    
    def _detect_format(self, df: pd.DataFrame) -> str:
        """Detect which format the file is in"""
        # Check for MS format first - it has very specific contract ID patterns in first column
        # MS format check - look deeper into the file as it might have many header rows
        if df.shape[1] >= 20:  # MS usually has 22+ columns
            ms_pattern_found = False
            # Check more rows as MS format might have headers
            for i in range(min(50, len(df))):  # Check up to 50 rows
                if pd.notna(df.iloc[i, 0]):
                    val = str(df.iloc[i, 0])
                    # MS format has contract IDs like FUTSTK/FUTIDX-SYMBOL-DATE-TYPE-STRIKE in first column
                    if (('FUTSTK' in val or 'OPTSTK' in val or 'FUTIDX' in val or 'OPTIDX' in val) 
                        and val.count('-') >= 4):
                        ms_pattern_found = True
                        logger.debug(f"Found MS pattern in row {i}: {val[:50]}")
                        break
            
            if ms_pattern_found:
                return 'MS'
        
        # Check for CONTRACT format - has contract IDs in column 3
        if df.shape[1] >= 12:
            for i in range(min(20, len(df))):
                if len(df.iloc[i]) > 3 and pd.notna(df.iloc[i, 3]):
                    val = str(df.iloc[i, 3])
                    if ('FUTSTK' in val or 'OPTSTK' in val or 'FUTIDX' in val or 'OPTIDX' in val) and '-' in val:
                        return 'CONTRACT'
        
        # Default to BOD for files with 16+ columns
        if df.shape[1] >= 16:
            return 'BOD'
        
        return 'UNKNOWN'
    
    def _parse_bod(self, df: pd.DataFrame) -> List[Position]:
        """Parse BOD format"""
        positions = []
        data_start = self._find_data_start_bod(df)
        
        for idx in range(data_start, len(df)):
            try:
                row = df.iloc[idx]
                if len(row) < 16 or pd.isna(row.iloc[1]):
                    continue
                
                symbol = str(row.iloc[1]).strip()
                series = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else 'EQ'
                expiry = pd.to_datetime(row.iloc[3]) if pd.notna(row.iloc[3]) else datetime.now() + timedelta(30)
                strike = float(row.iloc[4]) if pd.notna(row.iloc[4]) else 0.0
                option_type = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else ''
                lot_size = int(row.iloc[6]) if pd.notna(row.iloc[6]) else 1
                position_lots = float(row.iloc[15]) if pd.notna(row.iloc[15]) else 0.0
                
                if position_lots == 0:
                    continue
                
                # Determine instrument type based on series and option_type
                # FUTIDX/FUTSTK = Futures, OPTIDX/OPTSTK = Options
                series_upper = series.upper()
                if 'FUT' in series_upper:
                    inst_type = 'FF'  # Futures
                elif 'OPT' in series_upper:
                    # For options, use the option_type (CE or PE)
                    inst_type = option_type  # CE or PE
                else:
                    # If series doesn't indicate type, use option_type if present
                    inst_type = option_type if option_type else 'FF'
                
                position = self._create_position(
                    symbol, expiry, strike, inst_type, position_lots, lot_size, series
                )
                if position:
                    positions.append(position)
                    
            except Exception as e:
                logger.debug(f"Error parsing BOD row {idx}: {e}")
                
        logger.info(f"Parsed {len(positions)} positions from BOD format")
        return positions
    
    def _parse_contract(self, df: pd.DataFrame) -> List[Position]:
        """Parse Contract CSV format"""
        positions = []
        data_started = False
        
        for idx in range(len(df)):
            try:
                row = df.iloc[idx]
                if len(row) < 12:
                    continue
                
                if idx == 0:
                    if pd.notna(row[5]) and str(row[5]).strip().lower() == 'lot size':
                        continue
                
                contract_id = str(row[3]).strip() if pd.notna(row[3]) else ""
                
                # Handle both stock and index contracts
                if not contract_id or not ('FUTSTK' in contract_id or 'OPTSTK' in contract_id 
                                          or 'FUTIDX' in contract_id or 'OPTIDX' in contract_id):
                    continue
                
                try:
                    lot_size = int(float(row[5])) if pd.notna(row[5]) else 0
                except (ValueError, TypeError):
                    lot_size = 0
                
                try:
                    position_lots = float(row[10]) if pd.notna(row[10]) else 0
                except (ValueError, TypeError):
                    position_lots = 0
                
                if position_lots == 0:
                    continue
                
                parsed = self._parse_contract_id(contract_id)
                if parsed:
                    position = self._create_position(
                        parsed['symbol'], parsed['expiry'], parsed['strike'],
                        parsed['inst_type'], position_lots, lot_size, parsed['series']
                    )
                    if position:
                        positions.append(position)
                        data_started = True
                        
            except Exception as e:
                if data_started:
                    logger.debug(f"Error parsing CONTRACT row {idx}: {e}")
        
        logger.info(f"Parsed {len(positions)} positions from CONTRACT format")
        return positions
    
    def _parse_ms(self, df: pd.DataFrame) -> List[Position]:
        """Parse MS Position format"""
        positions = []
        valid_rows = 0
        
        # MS format might have positions in different columns
        position_columns = [21, 20, 19, 11, 10]  # Try different possible columns
        
        for idx in range(len(df)):
            try:
                row = df.iloc[idx]
                if len(row) < 22:
                    continue
                
                contract_id = str(row[0]).strip() if pd.notna(row[0]) else ""
                
                # Skip rows that don't have a valid contract ID pattern
                if not contract_id or '-' not in contract_id:
                    continue
                
                # Skip header rows or summary rows
                if any(keyword in contract_id.lower() for keyword in ['total', 'summary', 'net', 'mtm', 'payable', 'receivable']):
                    continue
                
                # Try to find position value in different columns
                position_lots = 0
                for col_idx in position_columns:
                    if col_idx < len(row):
                        try:
                            val = float(row[col_idx]) if pd.notna(row[col_idx]) else 0
                            if val != 0:
                                position_lots = val
                                break
                        except (ValueError, TypeError):
                            continue
                
                if position_lots == 0:
                    continue
                
                parsed = self._parse_contract_id(contract_id)
                if parsed:
                    position = self._create_position(
                        parsed['symbol'], parsed['expiry'], parsed['strike'],
                        parsed['inst_type'], position_lots, None, parsed['series']
                    )
                    if position:
                        positions.append(position)
                        valid_rows += 1
                        
            except Exception as e:
                logger.debug(f"Could not parse MS row {idx}: {e}")
        
        if valid_rows > 0:
            logger.info(f"Successfully parsed {valid_rows} positions from MS format")
        
        return positions
    
    def _find_data_start_bod(self, df: pd.DataFrame) -> int:
        """Find where data starts in BOD format"""
        for i in range(min(100, len(df))):
            if len(df.iloc[i]) < 16:
                continue
            
            col5_val = str(df.iloc[i, 4]).strip() if pd.notna(df.iloc[i, 4]) else ""
            if any(word in col5_val.lower() for word in ['strike', 'price', 'column', 'header']):
                continue
            
            try:
                if pd.notna(df.iloc[i, 4]):
                    float(df.iloc[i, 4])
                if pd.notna(df.iloc[i, 15]):
                    float(df.iloc[i, 15])
                return i
            except:
                continue
        return 0
    
    def _parse_contract_id(self, contract_id: str) -> Optional[Dict]:
        """Parse contract ID string - handles both stock and index contracts"""
        try:
            contract_id = contract_id.strip()
            if contract_id.endswith(' -0'):
                contract_id = contract_id[:-3]
            
            parts = contract_id.split('-')
            parts = [p.strip() for p in parts]
            
            if len(parts) < 5:
                return None
            
            series = parts[0]  # FUTSTK, OPTSTK, FUTIDX, OPTIDX
            strike_str = parts[-1].replace(',', '')
            inst_type = parts[-2]
            expiry_str = parts[-3]
            symbol_parts = parts[1:-3]
            symbol = '-'.join(symbol_parts) if symbol_parts else parts[1]
            
            expiry = self._parse_date(expiry_str)
            strike = float(strike_str)
            
            return {
                'series': series,
                'symbol': symbol,
                'expiry': expiry,
                'inst_type': inst_type,
                'strike': strike
            }
        except:
            return None
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string"""
        date_str = str(date_str).strip().upper()
        
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        
        match = re.match(r'(\d{1,2})([A-Z]{3})(\d{4})', date_str.replace('-', ''))
        if match:
            day = int(match.group(1))
            month = month_map.get(match.group(2), 0)
            year = int(match.group(3))
            if month:
                return datetime(year, month, day)
        
        return pd.to_datetime(date_str)
    
    def _create_position(self, symbol: str, expiry: datetime, strike: float,
                        inst_type: str, position_lots: float, lot_size: Optional[int],
                        series: str) -> Optional[Position]:
        """Create Position object from parsed data"""
        symbol_normalized = symbol.strip().upper()
        
        mapping = None
        if symbol in self.symbol_mappings:
            mapping = self.symbol_mappings[symbol]
        elif symbol_normalized in self.normalized_mappings:
            mapping = self.normalized_mappings[symbol_normalized]
        
        if not mapping:
            self.unmapped_symbols.append({
                'symbol': symbol,
                'expiry': expiry,
                'position_lots': position_lots
            })
            return None
        
        # Determine security type - handle both stock and index instruments
        inst_type = inst_type.upper()
        series_upper = series.upper() if series else ''
        
        # Check if it's a future based on inst_type or series
        if inst_type == 'FF' or 'FUT' in inst_type or 'FUT' in series_upper:
            security_type = 'Futures'
        elif inst_type in ['CE', 'C', 'CALL'] or (inst_type == 'CE'):
            security_type = 'Call'
        elif inst_type in ['PE', 'P', 'PUT'] or (inst_type == 'PE'):
            security_type = 'Put'
        else:
            # If we can't determine type, skip this position
            logger.debug(f"Could not determine security type for {symbol} with inst_type={inst_type}, series={series}")
            return None
        
        bloomberg_ticker = self._generate_bloomberg_ticker(
            mapping['ticker'], expiry, security_type, strike
        )
        
        return Position(
            underlying_ticker=mapping['underlying'],
            bloomberg_ticker=bloomberg_ticker,
            symbol=symbol,
            expiry_date=expiry,
            position_lots=position_lots,
            security_type=security_type,
            strike_price=strike,
            lot_size=lot_size or mapping.get('lot_size', 1)
        )
    
    def _generate_bloomberg_ticker(self, ticker: str, expiry: datetime,
                                  security_type: str, strike: float) -> str:
        """Generate Bloomberg ticker"""
        if security_type == 'Futures':
            month_code = MONTH_CODE.get(expiry.month, "")
            year_code = str(expiry.year)[-1]
            return f"{ticker}={month_code}{year_code} IS Equity"
        else:
            date_str = expiry.strftime('%m/%d/%y')
            strike_str = str(int(strike)) if strike == int(strike) else str(strike)
            if security_type == 'Call':
                return f"{ticker} IS {date_str} C{strike_str} Equity"
            else:
                return f"{ticker} IS {date_str} P{strike_str} Equity"