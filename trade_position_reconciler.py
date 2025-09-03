"""
Trade Position Reconciliation Module - Complete Version
Combines beginning positions with intraday trades to calculate post-trade positions
Generates complete pre-trade and post-trade delivery and IV reports
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass
import re
import os
from copy import copy
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment

# Import existing modules
from input_parser import Position
from excel_writer import ExcelWriter
from price_fetcher import PriceFetcher

logger = logging.getLogger(__name__)

@dataclass
class TradePosition:
    """Represents a position change from a trade"""
    underlying_ticker: str
    bloomberg_ticker: str
    symbol: str
    expiry_date: datetime
    position_change: float  # Positive for buy, negative for sell
    security_type: str  # Futures, Call, Put
    strike_price: float
    lot_size: int
    trade_price: float
    trade_type: str  # BUY or SELL

class TradePositionReconciler:
    """Main class for reconciling beginning positions with trades"""
    
    def __init__(self, mapping_file: str = "futures mapping.csv", usdinr_rate: float = 88.0):
        self.mapping_file = mapping_file
        self.usdinr_rate = usdinr_rate
        self.symbol_mappings = self._load_mappings()
        
    def _load_mappings(self) -> Dict:
        """Load symbol mappings from CSV"""
        mappings = {}
        try:
            df = pd.read_csv(self.mapping_file)
            for idx, row in df.iterrows():
                if pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
                    symbol = str(row.iloc[0]).strip().upper()
                    ticker = str(row.iloc[1]).strip()
                    
                    # Handle underlying
                    underlying = None
                    if len(row) > 2 and pd.notna(row.iloc[2]):
                        underlying_val = str(row.iloc[2]).strip()
                        if underlying_val and underlying_val.upper() != 'NAN':
                            underlying = underlying_val
                    
                    if not underlying:
                        if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
                            underlying = f"{symbol} INDEX"
                        else:
                            underlying = f"{ticker} IS Equity"
                    
                    lot_size = 1
                    if len(row) > 4 and pd.notna(row.iloc[4]):
                        try:
                            lot_size = int(float(str(row.iloc[4]).strip()))
                        except:
                            lot_size = 1
                    
                    mappings[symbol] = {
                        'ticker': ticker,
                        'underlying': underlying,
                        'lot_size': lot_size
                    }
        except Exception as e:
            logger.error(f"Error loading mapping file: {e}")
        
        return mappings
    
    def parse_ms_raw_trade_file(self, trade_file_path: str) -> List[TradePosition]:
        """Parse raw MS format trade file"""
        trade_positions = []
        
        try:
            df = pd.read_csv(trade_file_path, header=None)
            logger.info(f"Read MS file with {len(df)} rows and {len(df.columns)} columns")
            
            for idx, row in df.iterrows():
                try:
                    if len(row) < 14:
                        continue
                    
                    instrument = str(row[4]).upper() if pd.notna(row[4]) else ""
                    if instrument not in ['OPTSTK', 'FUTSTK', 'OPTIDX', 'FUTIDX']:
                        continue
                    
                    symbol = str(row[5]).strip().upper() if pd.notna(row[5]) else ""
                    if not symbol:
                        continue
                    
                    expiry_str = str(row[6]).strip() if pd.notna(row[6]) else ""
                    strike = float(row[8]) if pd.notna(row[8]) else 0.0
                    option_type = str(row[9]).strip().upper() if pd.notna(row[9]) else ""
                    side = str(row[10]).strip().upper() if pd.notna(row[10]) else ""
                    qty = float(row[12]) if pd.notna(row[12]) else 0.0
                    price = float(row[13]) if pd.notna(row[13]) else 0.0
                    lot_size = int(float(row[7])) if pd.notna(row[7]) else 1
                    
                    if qty == 0:
                        continue
                    
                    expiry = self._parse_date_flexible(expiry_str)
                    if not expiry:
                        continue
                    
                    if 'FUT' in instrument:
                        security_type = 'Futures'
                    elif option_type in ['CE', 'C']:
                        security_type = 'Call'
                    elif option_type in ['PE', 'P']:
                        security_type = 'Put'
                    else:
                        continue
                    
                    if side.startswith('B'):
                        position_change = qty
                        trade_type = 'BUY'
                    elif side.startswith('S'):
                        position_change = -qty
                        trade_type = 'SELL'
                    else:
                        continue
                    
                    mapping = self.symbol_mappings.get(symbol, {})
                    if not mapping:
                        if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']:
                            underlying = f"{symbol} INDEX"
                            ticker = symbol
                        else:
                            underlying = f"{symbol} IS Equity"
                            ticker = symbol
                    else:
                        underlying = mapping.get('underlying', f"{symbol} IS Equity")
                        ticker = mapping.get('ticker', symbol)
                    
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike
                    )
                    
                    trade_pos = TradePosition(
                        underlying_ticker=underlying,
                        bloomberg_ticker=bloomberg_ticker,
                        symbol=symbol,
                        expiry_date=expiry,
                        position_change=position_change,
                        security_type=security_type,
                        strike_price=strike,
                        lot_size=lot_size,
                        trade_price=price,
                        trade_type=trade_type
                    )
                    
                    trade_positions.append(trade_pos)
                    
                except Exception as e:
                    logger.debug(f"Error parsing row {idx}: {e}")
            
            logger.info(f"Successfully parsed {len(trade_positions)} trades from MS file")
            return trade_positions
            
        except Exception as e:
            logger.error(f"Error reading MS trade file: {e}")
            return []
    
    def parse_wafra_trade_file(self, trade_file_path: str) -> List[TradePosition]:
        """Parse WAFRA format trade file"""
        trade_positions = []
        
        try:
            df = pd.read_csv(trade_file_path)
            
            for _, row in df.iterrows():
                if pd.isna(row.get('Security')) or row.get('Security') == 'UPDATE':
                    continue
                
                security = str(row['Security'])
                position_details = self._parse_bloomberg_ticker(security)
                
                if not position_details:
                    continue
                
                transaction = str(row.get('Transaction', '')).upper()
                qty = float(row.get('Order_Quantity', 0))
                
                if transaction == 'BUY':
                    position_change = qty
                elif transaction == 'SELL':
                    position_change = -qty
                else:
                    continue
                
                trade_price = float(row.get('Order_Price', 0))
                
                trade_pos = TradePosition(
                    underlying_ticker=position_details['underlying'],
                    bloomberg_ticker=security,
                    symbol=position_details['symbol'],
                    expiry_date=position_details['expiry'],
                    position_change=position_change,
                    security_type=position_details['type'],
                    strike_price=position_details['strike'],
                    lot_size=int(row.get('Contract Size', 1)),
                    trade_price=trade_price,
                    trade_type=transaction
                )
                
                trade_positions.append(trade_pos)
                
        except Exception as e:
            logger.error(f"Error parsing WAFRA trade file: {e}")
        
        return trade_positions
    
    def _parse_date_flexible(self, date_str: str) -> Optional[datetime]:
        """Parse date string in various formats"""
        date_str = str(date_str).strip()
        
        formats = [
            "%d-%b-%Y", "%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d",
            "%d-%m-%Y", "%d.%m.%Y", "%d%b%Y", "%d-%b-%y", "%d %b %Y"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except:
                continue
        
        try:
            return pd.to_datetime(date_str, dayfirst=True)
        except:
            return None
    
    def _parse_bloomberg_ticker(self, ticker: str) -> Optional[Dict]:
        """Parse Bloomberg ticker to extract position details"""
        try:
            ticker = ticker.strip()
            
            if '=' in ticker and 'IS Equity' in ticker:
                match = re.match(r'([A-Z]+)=([A-Z])(\d)\s+IS\s+Equity', ticker)
                if match:
                    symbol = match.group(1)
                    month_code = match.group(2)
                    year = match.group(3)
                    
                    month_map = {'F':1,'G':2,'H':3,'J':4,'K':5,'M':6,
                                'N':7,'Q':8,'U':9,'V':10,'X':11,'Z':12}
                    month = month_map.get(month_code, 1)
                    year_full = 2020 + int(year)
                    expiry = self._get_expiry_date(year_full, month)
                    underlying = self._get_underlying_from_symbol(symbol)
                    
                    return {
                        'symbol': symbol,
                        'underlying': underlying,
                        'expiry': expiry,
                        'type': 'Futures',
                        'strike': 0
                    }
            
            elif 'Index' in ticker and '=' not in ticker and not ('/' in ticker):
                match = re.match(r'([A-Z]+)([A-Z])(\d)\s+Index', ticker)
                if match:
                    symbol = match.group(1)
                    month_code = match.group(2)
                    year = match.group(3)
                    
                    month_map = {'F':1,'G':2,'H':3,'J':4,'K':5,'M':6,
                                'N':7,'Q':8,'U':9,'V':10,'X':11,'Z':12}
                    month = month_map.get(month_code, 1)
                    year_full = 2020 + int(year)
                    expiry = self._get_expiry_date(year_full, month)
                    
                    return {
                        'symbol': symbol,
                        'underlying': f"{symbol} INDEX",
                        'expiry': expiry,
                        'type': 'Futures',
                        'strike': 0
                    }
            
            elif ('C' in ticker or 'P' in ticker) and '/' in ticker:
                if 'IS' in ticker and 'Equity' in ticker:
                    match = re.match(r'([A-Z]+)\s+IS\s+(\d{2})/(\d{2})/(\d{2})\s+([CP])(\d+)\s+Equity', ticker)
                    if match:
                        symbol = match.group(1)
                        month = int(match.group(2))
                        day = int(match.group(3))
                        year = 2000 + int(match.group(4))
                        opt_type = 'Call' if match.group(5) == 'C' else 'Put'
                        strike = float(match.group(6))
                        
                        expiry = datetime(year, month, day)
                        underlying = self._get_underlying_from_symbol(symbol)
                        
                        return {
                            'symbol': symbol,
                            'underlying': underlying,
                            'expiry': expiry,
                            'type': opt_type,
                            'strike': strike
                        }
                
                elif 'Index' in ticker:
                    match = re.match(r'([A-Z]+)\s+(\d{2})/(\d{2})/(\d{2})\s+([CP])(\d+)\s+Index', ticker)
                    if match:
                        symbol = match.group(1)
                        month = int(match.group(2))
                        day = int(match.group(3))
                        year = 2000 + int(match.group(4))
                        opt_type = 'Call' if match.group(5) == 'C' else 'Put'
                        strike = float(match.group(6))
                        
                        expiry = datetime(year, month, day)
                        
                        return {
                            'symbol': symbol,
                            'underlying': f"{symbol} INDEX",
                            'expiry': expiry,
                            'type': opt_type,
                            'strike': strike
                        }
            
        except Exception as e:
            logger.debug(f"Could not parse ticker {ticker}: {e}")
        
        return None
    
    def _get_underlying_from_symbol(self, symbol: str) -> str:
        """Get underlying ticker from symbol using mapping"""
        if symbol in self.symbol_mappings:
            return self.symbol_mappings[symbol].get('underlying', f"{symbol} IS Equity")
        return f"{symbol} IS Equity"
    
    def _get_expiry_date(self, year: int, month: int) -> datetime:
        """Calculate expiry date (last Thursday of month typically)"""
        import calendar
        
        last_day = calendar.monthrange(year, month)[1]
        
        for day in range(last_day, 0, -1):
            date_obj = datetime(year, month, day)
            if date_obj.weekday() == 3:  # Thursday
                return date_obj
        
        return datetime(year, month, last_day)
    
    def _generate_bloomberg_ticker(self, ticker: str, expiry: datetime,
                                  security_type: str, strike: float) -> str:
        """Generate Bloomberg ticker format"""
        
        MONTH_CODE = {
            1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
            7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"
        }
        
        ticker_upper = ticker.upper()
        is_index = ticker_upper in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NZ', 'NBZ', 'NF', 'NBF']
        
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
    
    def read_beginning_positions(self, excel_file: str) -> pd.DataFrame:
        """Read All_Positions sheet from beginning positions file"""
        try:
            df = pd.read_excel(excel_file, sheet_name='All_Positions')
            return df
        except Exception as e:
            logger.error(f"Error reading beginning positions: {e}")
            return pd.DataFrame()
    
    def combine_positions(self, beginning_df: pd.DataFrame, 
                         trade_positions: List[TradePosition]) -> pd.DataFrame:
        """Combine beginning positions with trades to get post-trade positions"""
        
        position_dict = {}
        
        for _, row in beginning_df.iterrows():
            key = (
                str(row['Symbol']),
                pd.to_datetime(row['Expiry']).strftime('%Y-%m-%d'),
                str(row['Type']),
                float(row['Strike']) if pd.notna(row['Strike']) and str(row['Strike']).strip() else 0
            )
            
            position_dict[key] = {
                'underlying': row['Underlying'],
                'symbol': row['Symbol'],
                'expiry': pd.to_datetime(row['Expiry']),
                'position': float(row['Position']),
                'type': row['Type'],
                'strike': float(row['Strike']) if pd.notna(row['Strike']) and str(row['Strike']).strip() else 0,
                'lot_size': int(row['Lot Size']) if pd.notna(row['Lot Size']) else 1
            }
        
        for trade in trade_positions:
            key = (
                trade.bloomberg_ticker,
                trade.expiry_date.strftime('%Y-%m-%d'),
                trade.security_type,
                trade.strike_price
            )
            
            if key in position_dict:
                position_dict[key]['position'] += trade.position_change
            else:
                position_dict[key] = {
                    'underlying': trade.underlying_ticker,
                    'symbol': trade.bloomberg_ticker,
                    'expiry': trade.expiry_date,
                    'position': trade.position_change,
                    'type': trade.security_type,
                    'strike': trade.strike_price,
                    'lot_size': trade.lot_size
                }
        
        post_trade_data = []
        for key, pos in position_dict.items():
            post_trade_data.append({
                'Underlying': pos['underlying'],
                'Symbol': pos['symbol'],
                'Expiry': pos['expiry'].strftime('%Y-%m-%d'),
                'Position': pos['position'],
                'Type': pos['type'],
                'Strike': pos['strike'] if pos['strike'] > 0 else '',
                'Lot Size': pos['lot_size']
            })
        
        post_trade_df = pd.DataFrame(post_trade_data)
        
        if not post_trade_df.empty:
            post_trade_df = post_trade_df.sort_values(
                by=['Underlying', 'Expiry', 'Strike'],
                ascending=[True, True, True]
            )
        
        return post_trade_df
    
    def generate_post_trade_report(self, beginning_file: str, trade_file: str, 
                                  output_file: str, trade_format: str = 'MS'):
        """Main method to generate complete post-trade report"""
        
        logger.info("Starting post-trade position reconciliation...")
        
        logger.info("Reading beginning positions...")
        beginning_df = self.read_beginning_positions(beginning_file)
        
        if beginning_df.empty:
            logger.error("No beginning positions found")
            return False
        
        logger.info(f"Found {len(beginning_df)} beginning positions")
        
        logger.info(f"Parsing {trade_format} trade file...")
        
        if trade_format == 'MS':
            trade_positions = self.parse_ms_raw_trade_file(trade_file)
        elif trade_format == 'WAFRA':
            trade_positions = self.parse_wafra_trade_file(trade_file)
        else:
            logger.error(f"Unsupported trade format: {trade_format}")
            return False
        
        logger.info(f"Parsed {len(trade_positions)} trades")
        
        logger.info("Combining positions...")
        post_trade_df = self.combine_positions(beginning_df, trade_positions)
        
        logger.info(f"Post-trade positions: {len(post_trade_df)}")
        
        positions = []
        for _, row in post_trade_df.iterrows():
            if row['Position'] != 0:
                pos = Position(
                    underlying_ticker=row['Underlying'],
                    bloomberg_ticker=row['Symbol'],
                    symbol=row['Symbol'].split()[0] if ' ' in str(row['Symbol']) else row['Symbol'],
                    expiry_date=pd.to_datetime(row['Expiry']),
                    position_lots=row['Position'],
                    security_type=row['Type'],
                    strike_price=float(row['Strike']) if row['Strike'] else 0,
                    lot_size=row['Lot Size']
                )
                positions.append(pos)
        
        logger.info("Fetching current prices...")
        price_fetcher = PriceFetcher()
        
        all_symbols = set()
        for _, row in beginning_df.iterrows():
            symbol = str(row['Symbol']).split()[0] if ' ' in str(row['Symbol']) else row['Symbol']
            all_symbols.add(symbol)
        for p in positions:
            all_symbols.add(p.symbol)
        
        prices = price_fetcher.fetch_prices_for_symbols(list(all_symbols))
        
        logger.info("Creating post-trade Excel report...")
        self._create_enhanced_report(
            output_file, beginning_df, trade_positions, 
            post_trade_df, positions, prices
        )
        
        logger.info(f"Post-trade report saved: {output_file}")
        return True
    
    def _create_enhanced_report(self, output_file: str, beginning_df: pd.DataFrame,
                           trade_positions: List[TradePosition], 
                           post_trade_df: pd.DataFrame,
                           positions: List[Position], 
                           prices: Dict[str, float]):
    """Create Excel report with complete pre and post trade positions"""
    
    # Create a single ExcelWriter instance
    writer = ExcelWriter(output_file, self.usdinr_rate)
    wb = writer.wb
    
    # Clear any default sheets
    for sheet in wb.worksheets:
        wb.remove(sheet)
    
    # Define styles
    header_font = Font(bold=True, size=11)
    header_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
    trade_fill = PatternFill(start_color="87CEEB", end_color="87CEEB", fill_type="solid")
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # ============= 1. SUMMARY SHEET =============
    ws_summary = wb.create_sheet("Summary")
    ws_summary.cell(1, 1, "TRADE RECONCILIATION SUMMARY").font = Font(bold=True, size=14)
    ws_summary.cell(3, 1, f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    ws_summary.cell(5, 1, f"Pre-Trade Positions: {len(beginning_df)}")
    ws_summary.cell(6, 1, f"Trades Executed: {len(trade_positions)}")
    ws_summary.cell(7, 1, f"Post-Trade Positions: {len([r for _, r in post_trade_df.iterrows() if r['Position'] != 0])}")
    
    # ============= 2. TRADES SHEET =============
    ws_trades = wb.create_sheet("Trades")
    trade_headers = ['Trade #', 'Underlying', 'Symbol', 'Expiry', 'Type', 'Strike', 'Side', 'Quantity', 'Price']
    
    for col, header in enumerate(trade_headers, 1):
        ws_trades.cell(1, col, header).font = header_font
        ws_trades.cell(1, col).fill = trade_fill
        ws_trades.cell(1, col).border = border
    
    for idx, trade in enumerate(trade_positions, 2):
        ws_trades.cell(idx, 1, idx-1)
        ws_trades.cell(idx, 2, trade.underlying_ticker)
        ws_trades.cell(idx, 3, trade.symbol)
        ws_trades.cell(idx, 4, trade.expiry_date.strftime('%Y-%m-%d'))
        ws_trades.cell(idx, 5, trade.security_type)
        ws_trades.cell(idx, 6, trade.strike_price if trade.strike_price > 0 else '')
        ws_trades.cell(idx, 7, trade.trade_type)
        ws_trades.cell(idx, 8, abs(trade.position_change))
        ws_trades.cell(idx, 9, trade.trade_price)
    
    # ============= 3. PRE-TRADE SHEETS =============
    # Convert beginning positions to Position objects
    pre_positions = []
    for _, row in beginning_df.iterrows():
        pos = Position(
            underlying_ticker=row['Underlying'],
            bloomberg_ticker=row['Symbol'],
            symbol=str(row['Symbol']).split()[0] if ' ' in str(row['Symbol']) else row['Symbol'],
            expiry_date=pd.to_datetime(row['Expiry']),
            position_lots=float(row['Position']),
            security_type=row['Type'],
            strike_price=float(row['Strike']) if row.get('Strike') and str(row['Strike']).strip() else 0,
            lot_size=int(row['Lot Size']) if row.get('Lot Size') else 1
        )
        pre_positions.append(pos)
    
    # Create PreTrade_All_Positions
    ws_pre_all = wb.create_sheet("PreTrade_All_Positions")
    self._write_positions_sheet(ws_pre_all, beginning_df, header_font, header_fill, border)
    
    # IMPORTANT: Now we manually create pre-trade deliverable and IV sheets
    # First, let's create the master sheet for pre-trade
    writer.write_master_sheet(pre_positions, prices)
    # Find the sheet that was just created and rename it
    for sheet in wb.worksheets:
        if sheet.title == "Master_All_Expiries":
            sheet.title = "PreTrade_Master"
            break
    
    # Create pre-trade expiry sheets
    pre_expiries = list(set(p.expiry_date for p in pre_positions))
    for expiry in sorted(pre_expiries):
        writer.write_expiry_sheet(expiry, pre_positions, prices)
        # Find and rename
        old_name = f"Expiry_{expiry.strftime('%Y_%m_%d')}"
        for sheet in wb.worksheets:
            if sheet.title == old_name:
                sheet.title = f"PreTrade_{old_name}"
                break
    
    # Create pre-trade IV master
    writer.write_iv_master_sheet(pre_positions, prices)
    for sheet in wb.worksheets:
        if sheet.title == "IV_All_Expiries":
            sheet.title = "PreTrade_IV_Master"
            break
    
    # Create pre-trade IV expiry sheets
    for expiry in sorted(pre_expiries):
        writer.write_iv_expiry_sheet(expiry, pre_positions, prices)
        old_name = f"IV_Expiry_{expiry.strftime('%Y_%m_%d')}"
        for sheet in wb.worksheets:
            if sheet.title == old_name:
                sheet.title = f"PreTrade_{old_name}"
                break
    
    # ============= 4. POST-TRADE SHEETS =============
    # Create PostTrade_All_Positions
    ws_post_all = wb.create_sheet("PostTrade_All_Positions")
    post_trade_non_zero = post_trade_df[post_trade_df['Position'] != 0].copy()
    self._write_positions_sheet(ws_post_all, post_trade_non_zero, header_font, header_fill, border)
    
    # Create post-trade deliverable and IV sheets
    writer.write_master_sheet(positions, prices)
    for sheet in wb.worksheets:
        if sheet.title == "Master_All_Expiries":
            sheet.title = "PostTrade_Master"
            break
    
    # Create post-trade expiry sheets
    post_expiries = list(set(p.expiry_date for p in positions))
    for expiry in sorted(post_expiries):
        writer.write_expiry_sheet(expiry, positions, prices)
        old_name = f"Expiry_{expiry.strftime('%Y_%m_%d')}"
        for sheet in wb.worksheets:
            if sheet.title == old_name:
                sheet.title = f"PostTrade_{old_name}"
                break
    
    # Create post-trade IV master
    writer.write_iv_master_sheet(positions, prices)
    for sheet in wb.worksheets:
        if sheet.title == "IV_All_Expiries":
            sheet.title = "PostTrade_IV_Master"
            break
    
    # Create post-trade IV expiry sheets
    for expiry in sorted(post_expiries):
        writer.write_iv_expiry_sheet(expiry, positions, prices)
        old_name = f"IV_Expiry_{expiry.strftime('%Y_%m_%d')}"
        for sheet in wb.worksheets:
            if sheet.title == old_name:
                sheet.title = f"PostTrade_{old_name}"
                break
    
    # Remove any "All_Positions" sheet if it was created
    for sheet in wb.worksheets:
        if sheet.title == "All_Positions":
            wb.remove(sheet)
            break
    
    # Save the workbook
    wb.save(output_file)
    logger.info(f"Complete report saved with all sheets: {output_file}")
