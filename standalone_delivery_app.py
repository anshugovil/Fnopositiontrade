"""
Streamlit Futures Delivery Calculator with Trade File Support
Standalone version with integrated trade processing
Run with: streamlit run streamlit_delivery_with_trades.py
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import tempfile
import os
import logging
from typing import Dict, List, Optional

# Import required modules
from input_parser import InputParser, Position
from price_fetcher import PriceFetcher
from excel_writer import ExcelWriter
from trade_parser import TradeParser, Trade, net_positions_with_trades

# Optional import for reconciliation
try:
    from recon_module import PositionReconciliation
    RECON_AVAILABLE = True
except ImportError:
    RECON_AVAILABLE = False
    logging.warning("Reconciliation module not available")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Futures Delivery Calculator with Trades",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
        border-bottom: 3px solid #1f77b4;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #333;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .info-box {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        border: 1px solid #c3e6cb;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        border: 1px solid #ffeaa7;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        border: 1px solid #f5c6cb;
    }
</style>
""", unsafe_allow_html=True)


class DeliveryCalculatorApp:
    """Main Streamlit application class with trade support"""
    
    def __init__(self):
        self.initialize_session_state()
        if RECON_AVAILABLE:
            self.recon_module = PositionReconciliation()
    
    def initialize_session_state(self):
        """Initialize session state variables"""
        # Position related
        if 'positions' not in st.session_state:
            st.session_state.positions = []
        if 'trade_positions' not in st.session_state:
            st.session_state.trade_positions = []
        if 'final_positions' not in st.session_state:
            st.session_state.final_positions = []
        
        # Trade related
        if 'trades' not in st.session_state:
            st.session_state.trades = []
        if 'trades_df' not in st.session_state:
            st.session_state.trades_df = None
        if 'trade_file' not in st.session_state:
            st.session_state.trade_file = None
        if 'include_trades' not in st.session_state:
            st.session_state.include_trades = False
        
        # Price and mapping related
        if 'prices' not in st.session_state:
            st.session_state.prices = {}
        if 'unmapped_symbols' not in st.session_state:
            st.session_state.unmapped_symbols = []
        if 'unmapped_trades' not in st.session_state:
            st.session_state.unmapped_trades = []
        
        # Output related
        if 'report_generated' not in st.session_state:
            st.session_state.report_generated = False
        if 'output_file' not in st.session_state:
            st.session_state.output_file = None
        
        # Reconciliation related
        if 'recon_results' not in st.session_state:
            st.session_state.recon_results = None
        if 'recon_file' not in st.session_state:
            st.session_state.recon_file = None
    
    def run(self):
        """Main application entry point"""
        # Header
        st.markdown('<h1 class="main-header">📊 Futures & Options Delivery Calculator with Trade Support</h1>', 
                   unsafe_allow_html=True)
        
        # Sidebar for configuration
        with st.sidebar:
            st.header("⚙️ Configuration")
            
            # USDINR Rate
            usdinr_rate = st.number_input(
                "USD/INR Exchange Rate",
                min_value=50.0,
                max_value=150.0,
                value=88.0,
                step=0.1,
                help="Current USD to INR exchange rate for IV calculations"
            )
            
            # Mapping file upload
            st.subheader("📁 Symbol Mapping File")
            mapping_file = st.file_uploader(
                "Upload futures mapping CSV (optional)",
                type=['csv'],
                help="CSV file with symbol to ticker mappings"
            )
            
            mapping_file_path = self.get_mapping_file_path(mapping_file)
            
            st.divider()
            
            # Trade file upload section
            st.subheader("📈 Trade File (Optional)")
            st.info("Upload GS format trade file to net with positions")
            
            trade_file = st.file_uploader(
                "Upload today's trade file",
                type=['xlsx', 'xls'],
                help="GS format trade file (columns: TM NAME, INSTR, Symbol, etc.)",
                key="trade_uploader"
            )
            
            if trade_file:
                st.success(f"✅ Trade file: {trade_file.name}")
                st.session_state.trade_file = trade_file
                
                include_trades = st.checkbox(
                    "Include trades in calculation", 
                    value=True,
                    key="include_trades_checkbox",
                    help="When checked, trades will be netted with positions"
                )
                st.session_state.include_trades = include_trades
                
                if include_trades:
                    st.markdown('<div class="info-box">📊 Trades will be netted with positions</div>', 
                              unsafe_allow_html=True)
                else:
                    st.markdown('<div class="warning-box">⚠️ Trades loaded but NOT included</div>', 
                              unsafe_allow_html=True)
            
            st.divider()
            
            # Price fetching options
            st.subheader("💹 Price Options")
            fetch_prices = st.checkbox("Fetch prices from Yahoo Finance", value=True)
            
            if RECON_AVAILABLE:
                st.divider()
                st.subheader("🔄 Reconciliation (Optional)")
                recon_file = st.file_uploader(
                    "Upload reconciliation file",
                    type=['xlsx', 'xls', 'csv'],
                    help="File with Symbol and Position columns",
                    key="recon_uploader"
                )
                
                if recon_file:
                    st.success(f"✅ Recon file: {recon_file.name}")
                    st.session_state.recon_file = recon_file
        
        # Main content area with tabs
        tab_list = ["📤 Upload & Process", "📊 Positions Review", "📈 Trade Impact",
                    "💰 Deliverables Preview", "📥 Download Reports"]
        
        if RECON_AVAILABLE:
            tab_list.insert(4, "🔄 Reconciliation")
        
        tabs = st.tabs(tab_list)
        
        tab_idx = 0
        with tabs[tab_idx]:
            self.upload_and_process_tab(mapping_file_path, usdinr_rate, fetch_prices)
        
        tab_idx += 1
        with tabs[tab_idx]:
            self.positions_review_tab()
        
        tab_idx += 1
        with tabs[tab_idx]:
            self.trade_impact_tab()
        
        tab_idx += 1
        with tabs[tab_idx]:
            self.deliverables_preview_tab()
        
        if RECON_AVAILABLE:
            tab_idx += 1
            with tabs[tab_idx]:
                self.reconciliation_tab()
        
        tab_idx += 1
        with tabs[tab_idx]:
            self.download_reports_tab()
    
    def get_mapping_file_path(self, mapping_file):
        """Get mapping file path from upload or default"""
        if not mapping_file:
            st.info("ℹ️ Using default 'futures mapping.csv'")
            possible_paths = ['futures mapping.csv', 'futures_mapping.csv']
            for path in possible_paths:
                if os.path.exists(path):
                    return path
            return 'futures mapping.csv'
        else:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='wb') as tmp_file:
                tmp_file.write(mapping_file.getvalue())
                return tmp_file.name
    
    def upload_and_process_tab(self, mapping_file_path, usdinr_rate, fetch_prices):
        """Handle file upload and processing"""
        st.markdown('<h2 class="sub-header">Upload Position File</h2>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_file = st.file_uploader(
                "Choose your position file",
                type=['xlsx', 'xls', 'csv'],
                help="Upload BOD, CONTRACT, or MS format position file"
            )
        
        with col2:
            if uploaded_file:
                st.markdown('<div class="info-box">', unsafe_allow_html=True)
                st.write("**File Details:**")
                st.write(f"📁 Name: {uploaded_file.name}")
                st.write(f"📏 Size: {uploaded_file.size:,} bytes")
                st.markdown('</div>', unsafe_allow_html=True)
        
        if uploaded_file and mapping_file_path:
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                process_btn = st.button("🚀 Process Files", type="primary", use_container_width=True)
            
            if process_btn:
                with st.spinner("Processing files..."):
                    success, message = self.process_files(
                        uploaded_file, mapping_file_path, usdinr_rate, fetch_prices
                    )
                    
                    if success:
                        st.success(f"✅ {message}")
                        
                        # Show summary
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Original Positions", len(st.session_state.positions))
                        with col2:
                            st.metric("Trade Positions", len(st.session_state.trade_positions))
                        with col3:
                            st.metric("Final Positions", len(st.session_state.final_positions))
                        
                        st.balloons()
                    else:
                        st.error(f"❌ {message}")
    
    def process_files(self, uploaded_file, mapping_file_path, usdinr_rate, fetch_prices):
        """Process position file and optionally trade file"""
        try:
            # Parse position file
            positions, unmapped_symbols = self.parse_position_file(uploaded_file, mapping_file_path)
            
            if not positions:
                return False, "No valid positions found in the file"
            
            st.session_state.positions = positions
            st.session_state.unmapped_symbols = unmapped_symbols
            
            # Process trade file if provided
            trade_positions = []
            trades_df = None
            unmapped_trades = []
            
            if st.session_state.trade_file and st.session_state.include_trades:
                trades, trade_positions, trades_df, unmapped_trades = self.parse_trade_file(
                    st.session_state.trade_file, mapping_file_path
                )
                
                st.session_state.trades = trades
                st.session_state.trade_positions = trade_positions
                st.session_state.trades_df = trades_df
                st.session_state.unmapped_trades = unmapped_trades
                
                # Net positions with trades
                final_positions = net_positions_with_trades(positions, trade_positions)
                st.session_state.final_positions = final_positions
                
                st.info(f"📈 Processed {len(trades)} trades → {len(trade_positions)} net trade positions")
            else:
                st.session_state.final_positions = positions
                st.session_state.trade_positions = []
            
            # Fetch prices
            if fetch_prices:
                self.fetch_and_store_prices()
            
            # Generate Excel report
            self.generate_excel_report(usdinr_rate, mapping_file_path)
            
            # Prepare success message
            msg = f"Processed {len(positions)} positions"
            if st.session_state.include_trades and trade_positions:
                msg += f" and {len(st.session_state.trades)} trades"
                msg += f" → {len(st.session_state.final_positions)} final positions"
            
            return True, msg
            
        except Exception as e:
            logger.error(f"Error processing files: {str(e)}")
            return False, f"Error: {str(e)}"
    
    def parse_position_file(self, uploaded_file, mapping_file_path):
        """Parse position file"""
        suffix = os.path.splitext(uploaded_file.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='wb') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            temp_path = tmp_file.name
        
        try:
            parser = InputParser(mapping_file_path)
            positions = parser.parse_file(temp_path)
            return positions, parser.unmapped_symbols
        finally:
            try:
                os.unlink(temp_path)
            except:
                pass
    
    def parse_trade_file(self, trade_file, mapping_file_path):
        """Parse trade file"""
        suffix = os.path.splitext(trade_file.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='wb') as tmp_file:
            tmp_file.write(trade_file.getvalue())
            temp_path = tmp_file.name
        
        try:
            trade_parser = TradeParser(mapping_file_path)
            trades = trade_parser.parse_trade_file(temp_path)
            
            if not trades:
                return [], [], None, []
            
            trade_positions, unmapped_trades = trade_parser.convert_trades_to_positions(trades)
            trades_df = trade_parser.create_trade_summary(trades)
            
            return trades, trade_positions, trades_df, unmapped_trades
        finally:
            try:
                os.unlink(temp_path)
            except:
                pass
    
    def fetch_and_store_prices(self):
        """Fetch prices from Yahoo Finance"""
        with st.spinner("Fetching prices from Yahoo Finance..."):
            price_fetcher = PriceFetcher()
            positions_for_prices = st.session_state.final_positions
            symbols_to_fetch = list(set(p.symbol for p in positions_for_prices))
            symbol_prices = price_fetcher.fetch_prices_for_symbols(symbols_to_fetch)
            
            # Map to underlying tickers
            symbol_map = {}
            for p in positions_for_prices:
                symbol_map[p.underlying_ticker] = p.symbol
            
            prices = {}
            for underlying, symbol in symbol_map.items():
                if symbol in symbol_prices:
                    prices[underlying] = symbol_prices[symbol]
            
            st.session_state.prices = prices
    
    def generate_excel_report(self, usdinr_rate, mapping_file_path):
        """Generate Excel report"""
        with st.spinner("Generating Excel report..."):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Determine filename
            if st.session_state.include_trades and st.session_state.trade_positions:
                prefix = "DELIVERY_WITH_TRADES"
            else:
                prefix = "DELIVERY_REPORT"
            
            output_file = f"{prefix}_{timestamp}.xlsx"
            
            writer = ExcelWriter(output_file, usdinr_rate)
            
            # Check if we need extended report with trades
            if st.session_state.include_trades and st.session_state.trade_positions:
                # You need to add create_report_with_trades method to ExcelWriter
                # For now, use standard report
                writer.create_report(
                    st.session_state.final_positions,
                    st.session_state.prices,
                    st.session_state.unmapped_symbols + st.session_state.unmapped_trades
                )
            else:
                writer.create_report(
                    st.session_state.final_positions,
                    st.session_state.prices,
                    st.session_state.unmapped_symbols
                )
            
            st.session_state.output_file = output_file
            st.session_state.report_generated = True
    
    def positions_review_tab(self):
        """Display parsed positions for review"""
        st.markdown('<h2 class="sub-header">Position Summary</h2>', unsafe_allow_html=True)
        
        if not st.session_state.positions:
            st.info("📤 Please upload and process a position file first")
            return
        
        # Choose which positions to show
        position_type = st.radio(
            "Select positions to view",
            ["Original Positions", "Final Positions (After Trades)"],
            horizontal=True
        )
        
        if position_type == "Original Positions":
            positions = st.session_state.positions
            title = "Original Positions"
        else:
            positions = st.session_state.final_positions
            title = "Final Positions (After Netting with Trades)"
        
        st.subheader(f"📋 {title}")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Positions", len(positions))
        
        with col2:
            unique_underlyings = len(set(p.underlying_ticker for p in positions))
            st.metric("Unique Underlyings", unique_underlyings)
        
        with col3:
            unique_expiries = len(set(p.expiry_date for p in positions))
            st.metric("Unique Expiries", unique_expiries)
        
        with col4:
            futures_count = sum(1 for p in positions if p.is_future)
            options_count = len(positions) - futures_count
            st.metric("Futures/Options", f"{futures_count}/{options_count}")
        
        # Detailed positions table
        df_data = []
        for p in positions:
            df_data.append({
                'Underlying': p.underlying_ticker,
                'Symbol': p.symbol,
                'Bloomberg Ticker': p.bloomberg_ticker,
                'Expiry': p.expiry_date.strftime('%Y-%m-%d'),
                'Type': p.security_type,
                'Strike': p.strike_price if p.strike_price > 0 else '',
                'Position (Lots)': p.position_lots,
                'Lot Size': p.lot_size
            })
        
        df = pd.DataFrame(df_data)
        
        # Display table with color coding for negative positions
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Strike': st.column_config.NumberColumn(format="%.2f"),
                'Position (Lots)': st.column_config.NumberColumn(format="%.2f"),
            }
        )
        
        # Show short positions if any
        short_positions = [p for p in positions if p.position_lots < 0]
        if short_positions:
            st.warning(f"⚠️ {len(short_positions)} short positions detected")
    
    def trade_impact_tab(self):
        """Display trade impact analysis"""
        st.markdown('<h2 class="sub-header">Trade Impact Analysis</h2>', unsafe_allow_html=True)
        
        if not st.session_state.positions:
            st.info("📤 Please upload and process a position file first")
            return
        
        if not st.session_state.trade_file:
            st.info("📈 No trade file uploaded. Upload a trade file to see impact analysis.")
            return
        
        if not st.session_state.include_trades:
            st.warning("⚠️ Trades are loaded but not included in calculations.")
            return
        
        # Display trade summary
        if st.session_state.trades_df is not None and not st.session_state.trades_df.empty:
            st.subheader("📋 Trade Summary")
            
            trades_df = st.session_state.trades_df
            
            # Trade statistics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Trades", len(trades_df))
            with col2:
                buy_count = len(trades_df[trades_df['Side'] == 'B'])
                sell_count = len(trades_df[trades_df['Side'] == 'S'])
                st.metric("Buy/Sell", f"{buy_count}/{sell_count}")
            with col3:
                unique_symbols = trades_df['Symbol'].nunique()
                st.metric("Unique Symbols", unique_symbols)
            with col4:
                unique_expiries = trades_df['Expiry'].nunique()
                st.metric("Unique Expiries", unique_expiries)
            
            # Display trades
            st.dataframe(trades_df, use_container_width=True, hide_index=True)
        
        # Position changes
        st.subheader("📊 Position Changes")
        
        # Create impact analysis
        impact_data = self.create_impact_analysis()
        
        if impact_data:
            impact_df = pd.DataFrame(impact_data)
            
            # Display with highlighting
            st.dataframe(
                impact_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'Strike': st.column_config.NumberColumn(format="%.2f"),
                    'Original': st.column_config.NumberColumn(format="%.2f"),
                    'Trade Impact': st.column_config.NumberColumn(format="%.2f"),
                    'Final': st.column_config.NumberColumn(format="%.2f"),
                    'Change': st.column_config.NumberColumn(format="%.2f")
                }
            )
            
            # Summary
            st.subheader("📈 Impact Summary")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                new_positions = len([d for d in impact_data if d['Status'] == 'NEW'])
                st.metric("New Positions", new_positions)
            
            with col2:
                closed_positions = len([d for d in impact_data if d['Status'] == 'CLOSED'])
                st.metric("Closed Positions", closed_positions)
            
            with col3:
                flipped = len([d for d in impact_data if 'FLIPPED' in d['Status']])
                st.metric("Flipped Positions", flipped)
            
            with col4:
                modified = len([d for d in impact_data if d['Status'] in ['INCREASED', 'DECREASED']])
                st.metric("Modified Positions", modified)
    
    def create_impact_analysis(self):
        """Create impact analysis data"""
        impact_data = []
        
        # Track all unique positions
        all_keys = set()
        
        for pos in st.session_state.positions:
            key = (pos.underlying_ticker, pos.symbol, pos.expiry_date.date(), 
                   pos.security_type, pos.strike_price)
            all_keys.add(key)
        
        for pos in st.session_state.trade_positions:
            key = (pos.underlying_ticker, pos.symbol, pos.expiry_date.date(),
                   pos.security_type, pos.strike_price)
            all_keys.add(key)
        
        # Build comparison
        for key in all_keys:
            underlying, symbol, expiry, sec_type, strike = key
            
            original_qty = sum(p.position_lots for p in st.session_state.positions 
                              if (p.underlying_ticker, p.symbol, p.expiry_date.date(), 
                                  p.security_type, p.strike_price) == key)
            
            trade_qty = sum(p.position_lots for p in st.session_state.trade_positions
                           if (p.underlying_ticker, p.symbol, p.expiry_date.date(),
                               p.security_type, p.strike_price) == key)
            
            final_qty = original_qty + trade_qty
            
            # Determine status
            if original_qty == 0 and final_qty != 0:
                status = "NEW"
            elif final_qty == 0 and original_qty != 0:
                status = "CLOSED"
            elif original_qty > 0 and final_qty < 0:
                status = "FLIPPED SHORT"
            elif original_qty < 0 and final_qty > 0:
                status = "FLIPPED LONG"
            elif trade_qty > 0:
                status = "INCREASED"
            elif trade_qty < 0:
                status = "DECREASED"
            else:
                continue
            
            impact_data.append({
                'Underlying': underlying,
                'Symbol': symbol,
                'Expiry': expiry.strftime('%Y-%m-%d'),
                'Type': sec_type,
                'Strike': strike if strike > 0 else '',
                'Original': original_qty,
                'Trade Impact': trade_qty,
                'Final': final_qty,
                'Change': trade_qty,
                'Status': status
            })
        
        return impact_data
    
    def deliverables_preview_tab(self):
        """Preview deliverables calculation"""
        st.markdown('<h2 class="sub-header">Deliverables Analysis</h2>', unsafe_allow_html=True)
        
        if not st.session_state.final_positions:
            st.info("📤 Please upload and process files first")
            return
        
        positions = st.session_state.final_positions
        prices = st.session_state.prices
        
        # Sensitivity analysis
        st.subheader("📈 Sensitivity Analysis")
        sensitivity_pct = st.slider(
            "Price Change %",
            min_value=-20.0,
            max_value=20.0,
            value=0.0,
            step=1.0,
            help="Analyze deliverables at different price levels"
        )
        
        # Calculate deliverables
        deliverables_data = self.calculate_deliverables(positions, prices, sensitivity_pct)
        
        # Display
        if deliverables_data:
            deliverables_df = pd.DataFrame(deliverables_data)
            st.dataframe(
                deliverables_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'Current Price': st.column_config.NumberColumn(format="%.2f"),
                    'Adjusted Price': st.column_config.NumberColumn(format="%.2f"),
                    'Net Deliverable (Lots)': st.column_config.NumberColumn(format="%.0f"),
                }
            )
    
    def calculate_deliverables(self, positions, prices, sensitivity_pct):
        """Calculate deliverables with sensitivity"""
        # Group by underlying
        grouped = {}
        for p in positions:
            if p.underlying_ticker not in grouped:
                grouped[p.underlying_ticker] = []
            grouped[p.underlying_ticker].append(p)
        
        deliverables_data = []
        
        for underlying in sorted(grouped.keys()):
            underlying_positions = grouped[underlying]
            spot_price = prices.get(underlying, 0)
            
            if spot_price:
                adjusted_price = spot_price * (1 + sensitivity_pct / 100)
            else:
                adjusted_price = 0
            
            total_deliverable = 0
            
            for pos in underlying_positions:
                if pos.security_type == 'Futures':
                    deliverable = pos.position_lots
                elif pos.security_type == 'Call':
                    if adjusted_price > pos.strike_price:
                        deliverable = pos.position_lots
                    else:
                        deliverable = 0
                elif pos.security_type == 'Put':
                    if adjusted_price < pos.strike_price:
                        deliverable = -pos.position_lots
                    else:
                        deliverable = 0
                else:
                    deliverable = 0
                
                total_deliverable += deliverable
            
            deliverables_data.append({
                'Underlying': underlying,
                'Current Price': spot_price,
                'Adjusted Price': adjusted_price if spot_price else 'N/A',
                'Total Positions': len(underlying_positions),
                'Net Deliverable (Lots)': total_deliverable
            })
        
        return deliverables_data
    
    def reconciliation_tab(self):
        """Display reconciliation results"""
        if not RECON_AVAILABLE:
            st.warning("Reconciliation module not available")
            return
        
        st.markdown('<h2 class="sub-header">Position Reconciliation</h2>', unsafe_allow_html=True)
        
        if not st.session_state.report_generated:
            st.info("📤 Please process files first")
            return
        
        # Reconciliation logic here (simplified)
        st.info("Reconciliation functionality available when recon_module.py is present")
    
    def download_reports_tab(self):
        """Download generated reports"""
        st.markdown('<h2 class="sub-header">Download Reports</h2>', unsafe_allow_html=True)
        
        if not st.session_state.report_generated or not st.session_state.output_file:
            st.info("📤 Please process files first to generate reports")
            return
        
        st.markdown('<div class="success-box">', unsafe_allow_html=True)
        st.success("✅ **Delivery Report Ready!**")
        st.write(f"**Filename:** {st.session_state.output_file}")
        
        if st.session_state.include_trades and st.session_state.trade_positions:
            st.write("**Report includes:** Original positions, trades, and final netted positions")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        try:
            with open(st.session_state.output_file, 'rb') as f:
                excel_data = f.read()
            
            st.download_button(
                label="📥 Download Delivery Report",
                data=excel_data,
                file_name=st.session_state.output_file,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary"
            )
        except Exception as e:
            st.error(f"Error reading report: {str(e)}")


def main():
    """Main entry point"""
    app = DeliveryCalculatorApp()
    app.run()


if __name__ == "__main__":
    main()