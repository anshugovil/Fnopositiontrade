"""
Trade Position Reconciler - Streamlit Application
Complete web interface for combining beginning positions with trades
Generates post-trade delivery and IV reports
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import tempfile
import os
import logging
from typing import Dict, List, Optional, Tuple

# Import the reconciler module
from trade_position_reconciler import TradePositionReconciler, TradePosition

# Import existing modules
from input_parser import InputParser, Position
from price_fetcher import PriceFetcher
from excel_writer import ExcelWriter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Post-Trade Position Reconciler",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
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
    .change-positive {
        color: green;
        font-weight: bold;
    }
    .change-negative {
        color: red;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)


class TradeReconcilerApp:
    """Main Streamlit application for trade reconciliation"""
    
    def __init__(self):
        self.initialize_session_state()
    
    def initialize_session_state(self):
        """Initialize session state variables"""
        if 'beginning_positions' not in st.session_state:
            st.session_state.beginning_positions = None
        if 'trade_positions' not in st.session_state:
            st.session_state.trade_positions = []
        if 'post_trade_positions' not in st.session_state:
            st.session_state.post_trade_positions = None
        if 'prices' not in st.session_state:
            st.session_state.prices = {}
        if 'report_generated' not in st.session_state:
            st.session_state.report_generated = False
        if 'output_file' not in st.session_state:
            st.session_state.output_file = None
        if 'reconciler' not in st.session_state:
            st.session_state.reconciler = None
    
    def run(self):
        """Main application entry point"""
        # Header
        st.markdown('<h1 class="main-header">📊 Post-Trade Position Reconciler</h1>', 
                   unsafe_allow_html=True)
        st.markdown("**Combine beginning positions with intraday trades to calculate post-trade deliverables**")
        
        # Sidebar configuration
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
            
            # Mapping file
            st.subheader("📁 Symbol Mapping")
            mapping_file = st.file_uploader(
                "Upload futures mapping CSV",
                type=['csv'],
                help="CSV file with symbol to ticker mappings"
            )
            
            mapping_file_path = 'futures mapping.csv'  # Default
            if mapping_file:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='wb') as tmp:
                    tmp.write(mapping_file.getvalue())
                    mapping_file_path = tmp.name
                st.success("✅ Custom mapping loaded")
            else:
                st.info("ℹ️ Using default mapping file")
            
            # Initialize reconciler
            if not st.session_state.reconciler:
                st.session_state.reconciler = TradePositionReconciler(
                    mapping_file=mapping_file_path,
                    usdinr_rate=usdinr_rate
                )
            
            st.divider()
            
            # Price fetching options
            st.subheader("💹 Price Options")
            fetch_prices = st.checkbox("Fetch prices from Yahoo Finance", value=True)
            
            st.divider()
            
            # Processing options
            st.subheader("🔄 Processing Options")
            show_zero_positions = st.checkbox("Show zero positions", value=False)
            consolidate_by_expiry = st.checkbox("Consolidate by expiry", value=True)
        
        # Main content with tabs
        tabs = st.tabs([
            "📁 1. Beginning Positions",
            "📈 2. Trade Files", 
            "🔄 3. Reconciliation",
            "📊 4. Post-Trade Analysis",
            "📥 5. Download Reports"
        ])
        
        with tabs[0]:
            self.beginning_positions_tab()
        
        with tabs[1]:
            self.trade_files_tab()
        
        with tabs[2]:
            self.reconciliation_tab(show_zero_positions)
        
        with tabs[3]:
            self.analysis_tab(fetch_prices)
        
        with tabs[4]:
            self.download_tab()
    
    def beginning_positions_tab(self):
        """Tab for uploading and viewing beginning positions"""
        st.markdown('<h2 class="sub-header">Upload Beginning Positions</h2>', 
                   unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            beginning_file = st.file_uploader(
                "Upload beginning positions Excel file",
                type=['xlsx', 'xls'],
                help="Upload the delivery report Excel from morning (with All_Positions sheet)"
            )
        
        with col2:
            if beginning_file:
                st.markdown('<div class="info-box">', unsafe_allow_html=True)
                st.write("**File Details:**")
                st.write(f"📄 Name: {beginning_file.name}")
                st.write(f"📊 Size: {beginning_file.size:,} bytes")
                st.markdown('</div>', unsafe_allow_html=True)
        
        if beginning_file:
            if st.button("📤 Load Beginning Positions", type="primary", use_container_width=True):
                with st.spinner("Loading positions..."):
                    try:
                        # Save temp file
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx', mode='wb') as tmp:
                            tmp.write(beginning_file.getvalue())
                            temp_path = tmp.name
                        
                        # Read All_Positions sheet
                        df = pd.read_excel(temp_path, sheet_name='All_Positions')
                        st.session_state.beginning_positions = df
                        
                        st.success(f"✅ Loaded {len(df)} beginning positions")
                        
                        # Clean up
                        os.unlink(temp_path)
                        
                    except Exception as e:
                        st.error(f"❌ Error loading file: {str(e)}")
        
        # Display beginning positions if loaded
        if st.session_state.beginning_positions is not None:
            df = st.session_state.beginning_positions
            
            st.subheader("📋 Beginning Positions Summary")
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Positions", len(df))
            
            with col2:
                unique_underlyings = df['Underlying'].nunique()
                st.metric("Unique Underlyings", unique_underlyings)
            
            with col3:
                total_lots = df['Position'].sum()
                st.metric("Total Lots", f"{total_lots:,.0f}")
            
            with col4:
                unique_expiries = df['Expiry'].nunique()
                st.metric("Unique Expiries", unique_expiries)
            
            # Group by underlying
            st.subheader("By Underlying")
            summary = df.groupby('Underlying').agg({
                'Position': 'sum',
                'Symbol': 'count'
            }).rename(columns={'Symbol': 'Count', 'Position': 'Total Position'})
            
            st.dataframe(summary, use_container_width=True)
            
            # Detailed view
            with st.expander("View Detailed Positions"):
                st.dataframe(df, use_container_width=True, hide_index=True)
    
    def trade_files_tab(self):
        """Tab for uploading and processing trade files"""
        st.markdown('<h2 class="sub-header">Upload Trade Files</h2>', 
                   unsafe_allow_html=True)
        
        # Trade format selection
        trade_format = st.radio(
            "Select trade file format:",
            ["WAFRA (MS Output)", "GS Format", "Direct Trade CSV"],
            help="Select the format of your trade file"
        )
        
        trade_file = st.file_uploader(
            "Upload trade file",
            type=['csv', 'xlsx'],
            help="Upload the trade file from your broker/system"
        )
        
        if trade_file:
            col1, col2 = st.columns([3, 1])
            
            with col1:
                if st.button("🔄 Process Trades", type="primary", use_container_width=True):
                    with st.spinner("Processing trades..."):
                        try:
                            # Save temp file
                            suffix = '.csv' if trade_file.name.endswith('.csv') else '.xlsx'
                            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='wb') as tmp:
                                tmp.write(trade_file.getvalue())
                                temp_path = tmp.name
                            
                            # Parse trades based on format
                            if trade_format == "WAFRA (MS Output)":
                                trades = st.session_state.reconciler.parse_wafra_trade_file(temp_path)
                            elif trade_format == "Direct Trade CSV":
                                trades = self.parse_direct_trade_csv(temp_path)
                            else:
                                st.error("GS format parsing not yet implemented")
                                trades = []
                            
                            st.session_state.trade_positions = trades
                            
                            if trades:
                                st.success(f"✅ Processed {len(trades)} trades")
                            else:
                                st.warning("⚠️ No valid trades found in file")
                            
                            # Clean up
                            os.unlink(temp_path)
                            
                        except Exception as e:
                            st.error(f"❌ Error processing trades: {str(e)}")
            
            with col2:
                st.markdown('<div class="info-box">', unsafe_allow_html=True)
                st.write(f"📄 {trade_file.name}")
                st.write(f"📊 {trade_file.size:,} bytes")
                st.markdown('</div>', unsafe_allow_html=True)
        
        # Display processed trades
        if st.session_state.trade_positions:
            trades = st.session_state.trade_positions
            
            st.subheader(f"📈 Processed Trades ({len(trades)} total)")
            
            # Summary by transaction type
            buy_trades = [t for t in trades if t.trade_type == 'BUY']
            sell_trades = [t for t in trades if t.trade_type == 'SELL']
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Buy Trades", len(buy_trades))
            with col2:
                st.metric("Sell Trades", len(sell_trades))
            
            # Convert to dataframe for display
            trades_data = []
            for t in trades:
                trades_data.append({
                    'Underlying': t.underlying_ticker,
                    'Symbol': t.bloomberg_ticker,
                    'Expiry': t.expiry_date.strftime('%Y-%m-%d'),
                    'Type': t.security_type,
                    'Strike': t.strike_price if t.strike_price > 0 else '',
                    'Trade': t.trade_type,
                    'Quantity': abs(t.position_change),
                    'Price': t.trade_price,
                    'Position Change': t.position_change
                })
            
            trades_df = pd.DataFrame(trades_data)
            
            # Group by underlying
            st.subheader("Trade Summary by Underlying")
            trade_summary = trades_df.groupby('Underlying').agg({
                'Position Change': 'sum',
                'Trade': 'count'
            }).rename(columns={'Trade': 'Trade Count'})
            
            st.dataframe(trade_summary, use_container_width=True)
            
            # Detailed trades
            with st.expander("View All Trades"):
                st.dataframe(trades_df, use_container_width=True, hide_index=True)
    
    def reconciliation_tab(self, show_zero_positions):
        """Tab for position reconciliation"""
        st.markdown('<h2 class="sub-header">Position Reconciliation</h2>', 
                   unsafe_allow_html=True)
        
        if st.session_state.beginning_positions is None:
            st.warning("⚠️ Please load beginning positions first (Tab 1)")
            return
        
        if not st.session_state.trade_positions:
            st.info("ℹ️ No trades loaded. Showing beginning positions only.")
            st.session_state.post_trade_positions = st.session_state.beginning_positions
            return
        
        if st.button("🔄 Reconcile Positions", type="primary", use_container_width=True):
            with st.spinner("Reconciling positions..."):
                # Combine positions
                post_trade_df = st.session_state.reconciler.combine_positions(
                    st.session_state.beginning_positions,
                    st.session_state.trade_positions
                )
                
                if not show_zero_positions:
                    post_trade_df = post_trade_df[post_trade_df['Position'] != 0]
                
                st.session_state.post_trade_positions = post_trade_df
                st.success("✅ Reconciliation complete!")
        
        if st.session_state.post_trade_positions is not None:
            beginning = st.session_state.beginning_positions
            post = st.session_state.post_trade_positions
            
            st.subheader("📊 Reconciliation Results")
            
            # Overall metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Beginning Positions",
                    len(beginning),
                    delta=None
                )
            
            with col2:
                trades_count = len(st.session_state.trade_positions)
                st.metric(
                    "Trades Processed",
                    trades_count,
                    delta=f"+{trades_count}"
                )
            
            with col3:
                st.metric(
                    "Post-Trade Positions",
                    len(post),
                    delta=f"{len(post) - len(beginning):+d}"
                )
            
            with col4:
                zero_positions = len(post[post['Position'] == 0]) if 'Position' in post.columns else 0
                st.metric(
                    "Closed Positions",
                    zero_positions
                )
            
            # Position changes by underlying
            st.subheader("Position Changes by Underlying")
            
            # Calculate changes
            begin_by_underlying = beginning.groupby('Underlying')['Position'].sum()
            post_by_underlying = post.groupby('Underlying')['Position'].sum()
            
            changes_data = []
            for underlying in set(list(begin_by_underlying.index) + list(post_by_underlying.index)):
                begin_pos = begin_by_underlying.get(underlying, 0)
                post_pos = post_by_underlying.get(underlying, 0)
                change = post_pos - begin_pos
                
                changes_data.append({
                    'Underlying': underlying,
                    'Beginning': begin_pos,
                    'Post-Trade': post_pos,
                    'Change': change,
                    'Change %': (change / begin_pos * 100) if begin_pos != 0 else 0
                })
            
            changes_df = pd.DataFrame(changes_data)
            
            # Style the dataframe
            def style_change(val):
                if val > 0:
                    return 'color: green; font-weight: bold'
                elif val < 0:
                    return 'color: red; font-weight: bold'
                return ''
            
            styled_df = changes_df.style.applymap(
                style_change, 
                subset=['Change', 'Change %']
            ).format({
                'Beginning': '{:,.0f}',
                'Post-Trade': '{:,.0f}',
                'Change': '{:+,.0f}',
                'Change %': '{:+.1f}%'
            })
            
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # Detailed reconciliation
            with st.expander("View Detailed Post-Trade Positions"):
                st.dataframe(post, use_container_width=True, hide_index=True)
    
    def analysis_tab(self, fetch_prices):
        """Tab for post-trade analysis"""
        st.markdown('<h2 class="sub-header">Post-Trade Deliverables Analysis</h2>', 
                   unsafe_allow_html=True)
        
        if st.session_state.post_trade_positions is None:
            st.warning("⚠️ Please complete reconciliation first (Tab 3)")
            return
        
        positions_df = st.session_state.post_trade_positions
        
        # Fetch prices if needed
        if fetch_prices and not st.session_state.prices:
            if st.button("💹 Fetch Current Prices", type="primary"):
                with st.spinner("Fetching prices..."):
                    # Convert to Position objects
                    positions = []
                    for _, row in positions_df.iterrows():
                        pos = Position(
                            underlying_ticker=row['Underlying'],
                            bloomberg_ticker=row['Symbol'],
                            symbol=row['Symbol'].split()[0],
                            expiry_date=pd.to_datetime(row['Expiry']),
                            position_lots=row['Position'],
                            security_type=row['Type'],
                            strike_price=float(row['Strike']) if row['Strike'] else 0,
                            lot_size=row['Lot Size']
                        )
                        positions.append(pos)
                    
                    # Fetch prices
                    price_fetcher = PriceFetcher()
                    symbols = list(set(p.symbol for p in positions))
                    prices = price_fetcher.fetch_prices_for_symbols(symbols)
                    st.session_state.prices = prices
                    
                    st.success(f"✅ Fetched prices for {len(prices)} symbols")
        
        if st.session_state.prices:
            prices = st.session_state.prices
            
            # Sensitivity analysis
            st.subheader("📈 Sensitivity Analysis")
            sensitivity = st.slider(
                "Price Change %",
                min_value=-20.0,
                max_value=20.0,
                value=0.0,
                step=1.0,
                help="Analyze deliverables at different price levels"
            )
            
            # Calculate deliverables
            deliverables_data = []
            
            # Group positions by underlying
            grouped = positions_df.groupby('Underlying')
            
            for underlying, group in grouped:
                # Get base price
                symbol = group.iloc[0]['Symbol'].split()[0]
                base_price = prices.get(symbol, 0)
                adjusted_price = base_price * (1 + sensitivity / 100) if base_price else 0
                
                total_deliverable = 0
                
                for _, pos in group.iterrows():
                    if pos['Type'] == 'Futures':
                        deliverable = pos['Position']
                    elif pos['Type'] == 'Call':
                        strike = float(pos['Strike']) if pos['Strike'] else 0
                        if adjusted_price > strike:
                            deliverable = pos['Position']
                        else:
                            deliverable = 0
                    elif pos['Type'] == 'Put':
                        strike = float(pos['Strike']) if pos['Strike'] else 0
                        if adjusted_price < strike:
                            deliverable = -pos['Position']
                        else:
                            deliverable = 0
                    else:
                        deliverable = 0
                    
                    total_deliverable += deliverable
                
                deliverables_data.append({
                    'Underlying': underlying,
                    'Current Price': base_price,
                    'Adjusted Price': adjusted_price,
                    'Net Position': group['Position'].sum(),
                    'Deliverable (Lots)': total_deliverable
                })
            
            deliverables_df = pd.DataFrame(deliverables_data)
            
            # Display deliverables
            st.subheader("📊 Deliverables Summary")
            st.dataframe(
                deliverables_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'Current Price': st.column_config.NumberColumn(format="%.2f"),
                    'Adjusted Price': st.column_config.NumberColumn(format="%.2f"),
                    'Net Position': st.column_config.NumberColumn(format="%.0f"),
                    'Deliverable (Lots)': st.column_config.NumberColumn(format="%.0f")
                }
            )
            
            # Expiry analysis
            st.subheader("📅 Expiry-wise Analysis")
            expiry_summary = positions_df.groupby('Expiry').agg({
                'Position': 'sum',
                'Symbol': 'count'
            }).rename(columns={'Symbol': 'Count', 'Position': 'Total Position'})
            
            st.dataframe(expiry_summary, use_container_width=True)
    
    def download_tab(self):
        """Tab for downloading reports"""
        st.markdown('<h2 class="sub-header">Download Reports</h2>', 
                   unsafe_allow_html=True)
        
        if st.session_state.post_trade_positions is None:
            st.warning("⚠️ Please complete reconciliation first")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📄 Generate Full Report")
            
            if st.button("📊 Generate Post-Trade Excel Report", type="primary", use_container_width=True):
                with st.spinner("Generating comprehensive report..."):
                    try:
                        # Generate report
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        output_file = f"POST_TRADE_DELIVERY_{timestamp}.xlsx"
                        
                        # Convert positions for report
                        positions = []
                        for _, row in st.session_state.post_trade_positions.iterrows():
                            pos = Position(
                                underlying_ticker=row['Underlying'],
                                bloomberg_ticker=row['Symbol'],
                                symbol=row['Symbol'].split()[0],
                                expiry_date=pd.to_datetime(row['Expiry']),
                                position_lots=row['Position'],
                                security_type=row['Type'],
                                strike_price=float(row['Strike']) if row['Strike'] else 0,
                                lot_size=row['Lot Size']
                            )
                            positions.append(pos)
                        
                        # Create report using reconciler's method
                        st.session_state.reconciler._create_enhanced_report(
                            output_file,
                            st.session_state.beginning_positions,
                            st.session_state.trade_positions,
                            st.session_state.post_trade_positions,
                            positions,
                            st.session_state.prices
                        )
                        
                        st.session_state.output_file = output_file
                        st.session_state.report_generated = True
                        
                        st.success("✅ Report generated successfully!")
                        
                    except Exception as e:
                        st.error(f"❌ Error generating report: {str(e)}")
        
        with col2:
            st.subheader("📥 Download Files")
            
            if st.session_state.report_generated and st.session_state.output_file:
                try:
                    with open(st.session_state.output_file, 'rb') as f:
                        excel_data = f.read()
                    
                    st.download_button(
                        label="📥 Download Post-Trade Report",
                        data=excel_data,
                        file_name=st.session_state.output_file,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        type="primary"
                    )
                    
                    # Also offer position CSV download
                    csv = st.session_state.post_trade_positions.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Positions CSV",
                        data=csv,
                        file_name=f"post_trade_positions_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        type="secondary"
                    )
                    
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
            else:
                st.info("Generate report first to enable downloads")
    
    def parse_direct_trade_csv(self, file_path):
        """Parse a simple CSV with trade data"""
        try:
            df = pd.read_csv(file_path)
            trades = []
            
            # Expected columns: Symbol, Expiry, Type, Strike, Side, Quantity, Price
            for _, row in df.iterrows():
                # Create trade position
                # This is a simplified parser - extend as needed
                pass
            
            return trades
        except Exception as e:
            logger.error(f"Error parsing direct trade CSV: {e}")
            return []


def main():
    """Main entry point"""
    app = TradeReconcilerApp()
    app.run()


if __name__ == "__main__":
    main()