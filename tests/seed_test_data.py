"""
Seed test data into KDB+ for stress testing
Quickly populates realTimeData table with synthetic data
"""

import pykx as kx
import time
import random
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def seed_data(host='localhost', port=5006, num_records=1000, symbols=None):
    """Seed test data into KDB+ ingest process"""
    
    if symbols is None:
        symbols = ['AAPL', 'TSLA', 'GOOGL', 'MSFT', 'AMZN']
    
    logger.info("="*60)
    logger.info(f"SEEDING TEST DATA")
    logger.info(f"Target: {host}:{port}")
    logger.info(f"Records: {num_records} ({num_records // len(symbols)} per symbol)")
    logger.info(f"Symbols: {symbols}")
    logger.info("="*60)
    
    try:
        # Connect
        logger.info("Connecting to KDB+ ingest process...")
        conn = kx.SyncQConnection(host=host, port=port)
        logger.info("✅ Connected successfully")
        
        # Initialize prices
        prices = {sym: random.uniform(100, 500) for sym in symbols}
        
        # Seed data
        records_inserted = 0
        start_time = time.time()
        
        for i in range(num_records):
            symbol = symbols[i % len(symbols)]
            
            # Random walk price
            change_pct = random.gauss(0, 0.005)
            close = prices[symbol] * (1 + change_pct)
            high = close * (1 + abs(random.gauss(0, 0.002)))
            low = close * (1 - abs(random.gauss(0, 0.002)))
            open_price = random.uniform(low, high)
            volume = random.randint(500000, 5000000)
            
            # Update price
            prices[symbol] = close
            
            # Insert
            try:
                conn('.ingestRealTimeData',
                    int(time.time() * 1000),
                    symbol,
                    round(open_price, 2),
                    round(high, 2),
                    round(low, 2),
                    round(close, 2),
                    volume)
                records_inserted += 1
                
                # Log progress
                if (i + 1) % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = records_inserted / elapsed
                    logger.info(f"Progress: {i+1}/{num_records} | "
                               f"Rate: {rate:.1f} records/sec")
            
            except Exception as e:
                logger.error(f"Failed to insert record {i+1}: {e}")
            
            # Small delay to avoid overwhelming
            time.sleep(0.001)
        
        # Summary
        elapsed = time.time() - start_time
        logger.info("="*60)
        logger.info("✅ DATA SEEDING COMPLETED")
        logger.info(f"  Records inserted: {records_inserted}/{num_records}")
        logger.info(f"  Duration: {elapsed:.2f}s")
        logger.info(f"  Throughput: {records_inserted/elapsed:.1f} records/sec")
        logger.info("="*60)
        
        # Verify
        logger.info("\nVerifying data...")
        total_records = conn("count realTimeData").py()
        logger.info(f"Total records in realTimeData: {total_records}")
        
        by_symbol = conn("select count i by sym from realTimeData").pd()
        logger.info("\nRecords by symbol:")
        logger.info(by_symbol.to_string())
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Seeding failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    import sys
    
    # Parse arguments
    num_records = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    
    seed_data(num_records=num_records)

