"""
Delivery Report Runner - Enhanced Version
Main orchestrator that coordinates all modules including trade file processing
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import Optional, List

# Import our modules
from input_parser import InputParser, Position
from trade_parser import TradeParser
from price_fetcher import PriceFetcher
from excel_writer import ExcelWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DeliveryReportGenerator:
    """Main class that orchestrates the entire process"""
    
    def __init__(self, input_file: str, mapping_file: str = "futures mapping.csv", 
                 usdinr_rate: float = 88.0, trade_file: str = None):
        self.input_file = input_file
        self.trade_file = trade_file
        self.mapping_file = mapping_file
        self.usdinr_rate = usdinr_rate
        self.parser = InputParser(mapping_file)
        self.trade_parser = TradeParser(mapping_file) if trade_file else None
        self.price_fetcher = PriceFetcher()
    
    def generate_report(self, output_file: str = None):
        """Generate complete delivery report with optional trade positions"""
        # Step 1: Parse input position file
        logger.info(f"Parsing position file: {self.input_file}")
        positions = self.parser.parse_file(self.input_file)
        logger.info(f"Parsed {len(positions)} positions")
        
        if not positions:
            logger.error("No positions found in input file")
            return
        
        # Step 2: Parse trade file if provided
        trade_positions = None
        if self.trade_file:
            logger.info(f"Parsing trade file: {self.trade_file}")
            trade_positions = self.trade_parser.parse_trade_file(self.trade_file)
            logger.info(f"Parsed {len(trade_positions)} net trade positions")
            
            if not trade_positions:
                logger.warning("No valid trade positions found in trade file")
        
        # Determine output filename based on format if not specified
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            format_type = getattr(self.parser, 'format_type', 'UNKNOWN')
            
            if format_type in ['BOD', 'CONTRACT']:
                prefix = "GS_AURIGIN_DELIVERY"
            elif format_type == 'MS':
                prefix = "MS_WAFRA_DELIVERY"
            else:
                prefix = "DELIVERY_REPORT"
            
            # Add TRADE suffix if trade file was provided
            if trade_positions:
                prefix += "_WITH_TRADES"
            
            output_file = f"{prefix}_{timestamp}.xlsx"
        
        logger.info("="*60)
        logger.info("Starting Delivery Report Generation")
        logger.info(f"Format Detected: {getattr(self.parser, 'format_type', 'UNKNOWN')}")
        logger.info(f"USDINR Rate: {self.usdinr_rate}")
        if trade_positions:
            logger.info(f"Trade Format: {getattr(self.trade_parser, 'format_type', 'UNKNOWN')}")
        logger.info("="*60)
        
        # Step 3: Get unique symbols for price fetching
        all_positions = positions + (trade_positions if trade_positions else [])
        symbol_map = {}
        for p in all_positions:
            symbol_map[p.underlying_ticker] = p.symbol
        
        symbols_to_fetch = list(set(p.symbol for p in all_positions))
        logger.info(f"Found {len(symbols_to_fetch)} unique symbols to fetch prices for")
        
        # Step 4: Fetch prices
        logger.info("Fetching prices from Yahoo Finance...")
        symbol_prices = self.price_fetcher.fetch_prices_for_symbols(symbols_to_fetch)
        
        # Map symbol prices to underlying tickers
        prices = {}
        for underlying, symbol in symbol_map.items():
            if symbol in symbol_prices:
                prices[underlying] = symbol_prices[symbol]
        
        logger.info(f"Mapped prices for {len(prices)} underlyings")
        
        # Step 5: Create Excel report with trade support
        logger.info("Creating Excel report...")
        writer = ExcelWriter(output_file, self.usdinr_rate)
        
        # Combine unmapped symbols from both parsers
        unmapped = self.parser.unmapped_symbols
        if self.trade_parser and hasattr(self.trade_parser, 'unmapped_symbols'):
            unmapped.extend(self.trade_parser.unmapped_symbols)
        
        writer.create_report(positions, prices, unmapped, trade_positions)
        
        logger.info("="*60)
        logger.info(f"Report generated successfully: {output_file}")
        if trade_positions:
            logger.info("Report includes:")
            logger.info("  - Start_All_Positions sheet")
            logger.info("  - Trade_Positions sheet")
            logger.info("  - Final_All_Positions sheet")
            logger.info("  - Initial and Final calculation sheets")
        logger.info("="*60)
        
        return output_file


def select_input_file(prompt_text="SELECT INPUT POSITION FILE"):
    """Interactive file selection from current directory"""
    excel_files = []
    csv_files = []
    
    for file in os.listdir('.'):
        if file.endswith(('.xlsx', '.xls')):
            excel_files.append(file)
        elif file.endswith('.csv'):
            csv_files.append(file)
    
    all_files = excel_files + csv_files
    
    if not all_files:
        print("No Excel or CSV files found in current directory.")
        return None
    
    print("\n" + "="*60)
    print(prompt_text)
    print("="*60)
    print("\nAvailable files in current directory:\n")
    
    file_index = 1
    file_map = {}
    
    if excel_files:
        print("Excel Files:")
        for file in sorted(excel_files):
            print(f"  [{file_index}] {file}")
            file_map[file_index] = file
            file_index += 1
    
    if csv_files:
        print("\nCSV Files:")
        for file in sorted(csv_files):
            print(f"  [{file_index}] {file}")
            file_map[file_index] = file
            file_index += 1
    
    print("\n" + "-"*60)
    
    while True:
        try:
            choice = input(f"\nEnter file number (1-{len(all_files)}), 'skip' to skip, or 'q' to quit: ").strip()
            
            if choice.lower() == 'q':
                print("Exiting...")
                return None
            
            if choice.lower() == 'skip':
                return 'SKIP'
            
            choice_num = int(choice)
            if 1 <= choice_num <= len(all_files):
                selected_file = file_map[choice_num]
                print(f"\nSelected: {selected_file}")
                return selected_file
            else:
                print(f"Please enter a number between 1 and {len(all_files)}")
        except ValueError:
            print("Invalid input. Please enter a number, 'skip', or 'q' to quit.")
        except KeyboardInterrupt:
            print("\n\nExiting...")
            return None


def main():
    parser = argparse.ArgumentParser(description='Generate Physical Delivery Report with Optional Trade Processing')
    parser.add_argument('input_file', nargs='?', help='Input position file (Excel or CSV)')
    parser.add_argument('--trade', help='Trade file (Excel or CSV) for trade position calculation')
    parser.add_argument('--output', help='Output Excel file name')
    parser.add_argument('--usdinr', type=float, default=88.0, help='USDINR exchange rate (default: 88)')
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive mode for file selection')
    
    args = parser.parse_args()
    
    # Determine input file
    if args.input_file and not args.interactive:
        input_file = args.input_file
    else:
        input_file = select_input_file("SELECT INPUT POSITION FILE")
        if not input_file:
            sys.exit(0)
    
    # Verify input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        sys.exit(1)
    
    # Determine trade file
    trade_file = None
    if args.trade:
        trade_file = args.trade
    elif args.interactive:
        print("\n" + "="*60)
        print("OPTIONAL: Trade File Selection")
        print("="*60)
        trade_input = input("\nDo you want to include a trade file? (y/n) [n]: ").strip().lower()
        if trade_input == 'y':
            trade_file = select_input_file("SELECT TRADE FILE (MS/GS Format)")
            if trade_file == 'SKIP':
                trade_file = None
                print("Skipping trade file...")
            elif trade_file and not os.path.exists(trade_file):
                logger.error(f"Trade file not found: {trade_file}")
                trade_file = None
    
    # Fixed mapping file
    mapping_file = 'futures mapping.csv'
    
    # Verify mapping file exists
    if not os.path.exists(mapping_file):
        logger.error(f"Mapping file not found: {mapping_file}")
        print("\nPlease ensure 'futures mapping.csv' is in the current directory.")
        sys.exit(1)
    
    # Get USDINR rate if interactive
    usdinr_rate = args.usdinr
    if args.interactive:
        usdinr_input = input(f"\nUSDINR exchange rate (default: 88): ").strip()
        if usdinr_input:
            try:
                usdinr_rate = float(usdinr_input)
            except ValueError:
                print("Invalid rate, using default: 88")
                usdinr_rate = 88.0
    
    # Output filename will be auto-generated based on format
    output_file = args.output
    
    print("\n" + "="*60)
    print("STARTING DELIVERY REPORT GENERATION")
    print("="*60)
    print(f"Position File: {input_file}")
    if trade_file:
        print(f"Trade File: {trade_file}")
    print(f"Mapping File: {mapping_file}")
    print(f"USDINR Rate: {usdinr_rate}")
    print(f"Output File: Will be auto-generated based on format")
    print("="*60 + "\n")
    
    # Generate the report
    generator = DeliveryReportGenerator(input_file, mapping_file, usdinr_rate, trade_file)
    output_filename = generator.generate_report(output_file)
    
    if output_filename:
        print(f"\nOutput saved as: {output_filename}")
    
    if args.interactive:
        input("\nPress Enter to exit...")
    
    sys.exit(0)


if __name__ == "__main__":
    # Check if no arguments provided, default to interactive mode
    if len(sys.argv) == 1:
        sys.argv.append('--interactive')
    main()
