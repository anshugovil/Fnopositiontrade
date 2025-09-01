"""
Updated sections for streamlit_delivery_app.py to add trade file support
Add these modifications to your existing streamlit_delivery_app.py
"""

# Add to imports section
from trade_parser import TradeParser, Trade, net_positions_with_trades

# Update initialize_session_state method
def initialize_session_state(self):
    """Initialize session state variables"""
    if 'positions' not in st.session_state:
        st.session_state.positions = []
    if 'trade_positions' not in st.session_state:
        st.session_state.trade_positions = []
    if 'final_positions' not in st.session_state:
        st.session_state.final_positions = []
    if 'trades' not in st.session_state:
        st.session_state.trades = []
    if 'trades_df' not in st.session_state:
        st.session_state.trades_df = None
    if 'prices' not in st.session_state:
        st.session_state.prices = {}
    if 'unmapped_symbols' not in st.session_state:
        st.session_state.unmapped_symbols = []
    if 'unmapped_trades' not in st.session_state:
        st.session_state.unmapped_trades = []
    if 'report_generated' not in st.session_state:
        st.session_state.report_generated = False
    if 'output_file' not in st.session_state:
        st.session_state.output_file = None
    if 'recon_results' not in st.session_state:
        st.session_state.recon_results = None
    if 'recon_file' not in st.session_state:
        st.session_state.recon_file = None
    if 'trade_file' not in st.session_state:
        st.session_state.trade_file = None
    if 'include_trades' not in st.session_state:
        st.session_state.include_trades = False

# Update sidebar section in run method
def run(self):
    """Main application entry point"""
    # Header
    st.markdown('<h1 class="main-header">📊 Futures & Options Delivery Calculator</h1>', 
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
            "Upload futures mapping CSV",
            type=['csv'],
            help="CSV file with symbol to ticker mappings"
        )
        
        mapping_file_path = None
        if not mapping_file:
            st.info("ℹ️ Using default 'futures mapping.csv'")
            possible_paths = ['futures mapping.csv', 'futures_mapping.csv']
            for path in possible_paths:
                if os.path.exists(path):
                    mapping_file_path = path
                    break
            if not mapping_file_path:
                mapping_file_path = 'futures mapping.csv'
        else:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='wb') as tmp_file:
                tmp_file.write(mapping_file.getvalue())
                mapping_file_path = tmp_file.name
        
        st.divider()
        
        # Trade file upload section
        st.subheader("📈 Trade File (Optional)")
        trade_file = st.file_uploader(
            "Upload today's trade file",
            type=['xlsx', 'xls'],
            help="GS format trade file to net with positions",
            key="trade_uploader"
        )
        
        if trade_file:
            st.success(f"✅ Trade file loaded: {trade_file.name}")
            st.session_state.trade_file = trade_file
            include_trades = st.checkbox(
                "Include trades in calculation", 
                value=True,
                key="include_trades_checkbox"
            )
            st.session_state.include_trades = include_trades
            
            if include_trades:
                st.info("📊 Trades will be netted with positions")
            else:
                st.warning("⚠️ Trades loaded but will NOT be included")
        
        st.divider()
        
        # Price fetching options
        st.subheader("💹 Price Options")
        fetch_prices = st.checkbox("Fetch prices from Yahoo Finance", value=True)
        
        st.divider()
        
        # Reconciliation options
        st.subheader("🔄 Reconciliation (Optional)")
        st.info("Upload a recon file to compare positions")
        recon_file = st.file_uploader(
            "Upload reconciliation file",
            type=['xlsx', 'xls', 'csv'],
            help="File with Symbol and Position columns to reconcile against",
            key="recon_uploader"
        )
        
        if recon_file:
            st.success(f"✅ Recon file loaded: {recon_file.name}")
            st.session_state.recon_file = recon_file
    
    # Main content area with tabs
    tabs = st.tabs(["📤 Upload & Process", "📊 Positions Review", "📈 Trade Impact",
                    "💰 Deliverables Preview", "🔄 Reconciliation", "📥 Download Reports"])
    
    with tabs[0]:
        self.upload_and_process_tab(mapping_file_path, usdinr_rate, fetch_prices)
    
    with tabs[1]:
        self.positions_review_tab()
    
    with tabs[2]:
        self.trade_impact_tab()
    
    with tabs[3]:
        self.deliverables_preview_tab()
    
    with tabs[4]:
        self.reconciliation_tab()
    
    with tabs[5]:
        self.download_reports_tab()

# Add new trade processing method
def process_trade_file(self, trade_file, mapping_file_path):
    """Process trade file and convert to positions"""
    try:
        # Save trade file temporarily
        suffix = os.path.splitext(trade_file.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='wb') as tmp_file:
            tmp_file.write(trade_file.getvalue())
            trade_file_path = tmp_file.name
        
        # Parse trades
        trade_parser = TradeParser(mapping_file_path)
        trades = trade_parser.parse_trade_file(trade_file_path)
        
        if not trades:
            return False, "No valid trades found in file", [], [], None, []
        
        # Convert trades to positions
        trade_positions, unmapped_trades = trade_parser.convert_trades_to_positions(trades)
        
        # Create trade summary dataframe
        trades_df = trade_parser.create_trade_summary(trades)
        
        # Clean up temp file
        try:
            os.unlink(trade_file_path)
        except:
            pass
        
        return True, f"Processed {len(trades)} trades", trades, trade_positions, trades_df, unmapped_trades
        
    except Exception as e:
        logger.error(f"Error processing trade file: {str(e)}")
        return False, f"Error processing trade file: {str(e)}", [], [], None, []

# Update process_file method to handle trades
def process_file(self, uploaded_file, mapping_file_path, password, usdinr_rate, fetch_prices):
    """Process the uploaded file and optionally net with trades"""
    try:
        # Save uploaded file temporarily
        suffix = os.path.splitext(uploaded_file.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode='wb') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            input_file_path = tmp_file.name
        
        # Parse positions
        parser = InputParser(mapping_file_path)
        positions = parser.parse_file(input_file_path)
        
        if not positions:
            return False, "No valid positions found in the file"
        
        st.session_state.positions = positions
        st.session_state.unmapped_symbols = parser.unmapped_symbols
        
        # Process trade file if provided and included
        trade_positions = []
        trades = []
        trades_df = None
        unmapped_trades = []
        
        if st.session_state.trade_file and st.session_state.include_trades:
            success, message, trades, trade_positions, trades_df, unmapped_trades = self.process_trade_file(
                st.session_state.trade_file, mapping_file_path
            )
            
            if success:
                st.session_state.trades = trades
                st.session_state.trade_positions = trade_positions
                st.session_state.trades_df = trades_df
                st.session_state.unmapped_trades = unmapped_trades
                st.info(f"📈 {message}")
                
                # Net positions with trades
                final_positions = net_positions_with_trades(positions, trade_positions)
                st.session_state.final_positions = final_positions
            else:
                st.warning(f"Trade processing failed: {message}")
                st.session_state.final_positions = positions
        else:
            st.session_state.final_positions = positions
        
        # Fetch prices if enabled
        if fetch_prices:
            with st.spinner("Fetching prices from Yahoo Finance..."):
                price_fetcher = PriceFetcher()
                # Use final positions for price fetching
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
        
        # Generate Excel report
        with st.spinner("Generating Excel report..."):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            format_type = getattr(parser, 'format_type', 'UNKNOWN')
            
            # Adjust filename if trades are included
            if st.session_state.include_trades and trade_positions:
                if format_type in ['BOD', 'CONTRACT']:
                    prefix = "GS_AURIGIN_DELIVERY_WITH_TRADES"
                elif format_type == 'MS':
                    prefix = "MS_WAFRA_DELIVERY_WITH_TRADES"
                else:
                    prefix = "DELIVERY_REPORT_WITH_TRADES"
            else:
                if format_type in ['BOD', 'CONTRACT']:
                    prefix = "GS_AURIGIN_DELIVERY"
                elif format_type == 'MS':
                    prefix = "MS_WAFRA_DELIVERY"
                else:
                    prefix = "DELIVERY_REPORT"
            
            output_file = f"{prefix}_{timestamp}.xlsx"
            
            writer = ExcelWriter(output_file, usdinr_rate)
            
            # Generate report based on whether trades are included
            if st.session_state.include_trades and trade_positions:
                # Add the new methods to writer (you need to add these to excel_writer.py)
                writer.create_report_with_trades(
                    positions,
                    trade_positions,
                    st.session_state.final_positions,
                    st.session_state.prices,
                    parser.unmapped_symbols,
                    unmapped_trades,
                    trades_df
                )
            else:
                writer.create_report(
                    st.session_state.final_positions,
                    st.session_state.prices,
                    parser.unmapped_symbols
                )
            
            st.session_state.output_file = output_file
            st.session_state.report_generated = True
        
        # Clean up temp file
        try:
            os.unlink(input_file_path)
        except:
            pass
        
        position_msg = f"Successfully processed {len(positions)} positions"
        if st.session_state.include_trades and trade_positions:
            position_msg += f" and {len(trades)} trades → {len(st.session_state.final_positions)} final positions"
        
        return True, position_msg
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return False, f"Error processing file: {str(e)}"

# Add new Trade Impact tab
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
        st.warning("⚠️ Trades are loaded but not included in calculations. Check 'Include trades' in sidebar.")
        return
    
    # Display trade summary
    if st.session_state.trades_df is not None and not st.session_state.trades_df.empty:
        st.subheader("📋 Trade Summary")
        
        # Trade statistics
        col1, col2, col3, col4 = st.columns(4)
        
        trades_df = st.session_state.trades_df
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
        
        # Display trades table
        st.dataframe(trades_df, use_container_width=True, hide_index=True)
    
    # Display position changes
    st.subheader("📊 Position Changes")
    
    original_positions = st.session_state.positions
    trade_positions = st.session_state.trade_positions
    final_positions = st.session_state.final_positions
    
    # Create comparison dataframe
    comparison_data = []
    
    # Track all unique positions
    all_keys = set()
    
    for pos in original_positions:
        key = (pos.underlying_ticker, pos.symbol, pos.expiry_date.date(), 
               pos.security_type, pos.strike_price)
        all_keys.add(key)
    
    for pos in trade_positions:
        key = (pos.underlying_ticker, pos.symbol, pos.expiry_date.date(),
               pos.security_type, pos.strike_price)
        all_keys.add(key)
    
    # Build comparison data
    for key in all_keys:
        underlying, symbol, expiry, sec_type, strike = key
        
        original_qty = sum(p.position_lots for p in original_positions 
                          if (p.underlying_ticker, p.symbol, p.expiry_date.date(), 
                              p.security_type, p.strike_price) == key)
        
        trade_qty = sum(p.position_lots for p in trade_positions
                       if (p.underlying_ticker, p.symbol, p.expiry_date.date(),
                           p.security_type, p.strike_price) == key)
        
        final_qty = sum(p.position_lots for p in final_positions
                       if (p.underlying_ticker, p.symbol, p.expiry_date.date(),
                           p.security_type, p.strike_price) == key)
        
        if abs(trade_qty) > 0.0001:  # Only show positions with trade impact
            comparison_data.append({
                'Underlying': underlying,
                'Symbol': symbol,
                'Expiry': expiry.strftime('%Y-%m-%d'),
                'Type': sec_type,
                'Strike': strike if strike > 0 else '',
                'Original': original_qty,
                'Trade Impact': trade_qty,
                'Final': final_qty,
                'Change': final_qty - original_qty
            })
    
    if comparison_data:
        comparison_df = pd.DataFrame(comparison_data)
        
        # Sort by absolute change
        comparison_df['Abs_Change'] = comparison_df['Change'].abs()
        comparison_df = comparison_df.sort_values('Abs_Change', ascending=False)
        comparison_df = comparison_df.drop('Abs_Change', axis=1)
        
        # Display with color coding
        st.dataframe(
            comparison_df,
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
        
        # Summary metrics
        st.subheader("📈 Impact Summary")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            new_positions = len([d for d in comparison_data if d['Original'] == 0])
            st.metric("New Positions", new_positions)
        
        with col2:
            closed_positions = len([d for d in comparison_data if d['Final'] == 0])
            st.metric("Closed Positions", closed_positions)
        
        with col3:
            flipped = len([d for d in comparison_data 
                         if (d['Original'] > 0 and d['Final'] < 0) or 
                            (d['Original'] < 0 and d['Final'] > 0)])
            st.metric("Flipped Positions", flipped)
    else:
        st.info("No trade impact to display")