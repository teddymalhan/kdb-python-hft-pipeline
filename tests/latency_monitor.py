"""
End-to-End Latency Monitor for HFT Platform
Measures latency from data injection to dashboard visibility
"""

import pykx as kx
import time
import statistics
from datetime import datetime
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LatencyMonitor:
    """Monitor end-to-end latency across the pipeline"""
    
    def __init__(self):
        self.ingest_conn = None
        self.process_conn = None
        self.latency_samples = []
    
    def connect(self):
        """Connect to both KDB+ processes"""
        logger.info("="*60)
        logger.info("Attempting to connect to KDB+ processes...")
        logger.info("="*60)
        
        try:
            logger.info("Connecting to ingest process (localhost:5006)...")
            self.ingest_conn = kx.SyncQConnection(host='localhost', port=5006)
            logger.info("✅ Successfully connected to ingest process on port 5006")
            
            logger.info("Connecting to processing process (localhost:5011)...")
            self.process_conn = kx.SyncQConnection(host='localhost', port=5011)
            logger.info("✅ Successfully connected to processing process on port 5011")
            
            # Test connections
            logger.info("Testing ingest connection...")
            test1 = self.ingest_conn("1+1").py()
            logger.info(f"Ingest connection test: 1+1 = {test1}")
            
            logger.info("Testing processing connection...")
            test2 = self.process_conn("1+1").py()
            logger.info(f"Processing connection test: 1+1 = {test2}")
            
            # Check available data
            try:
                record_count = self.ingest_conn("count realTimeData").py()
                logger.info(f"Found {record_count} records in realTimeData table")
            except Exception as e:
                logger.warning(f"Could not query realTimeData: {e}")
            
            print("\n✅ All connections established successfully\n")
            return True
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            logger.error("Make sure KDB+ processes are running on ports 5006 and 5011")
            logger.error("Start them with:")
            logger.error("  Terminal 1: q realtime_ingest.q -p 5006")
            logger.error("  Terminal 2: q process.q -p 5011")
            return False
    
    def measure_ingest_latency(self, iterations=100):
        """Measure latency of writing to ingest process"""
        logger.info("\n" + "="*60)
        logger.info("TEST 1: MEASURING INGEST LATENCY")
        logger.info(f"Testing {iterations} write operations to KDB+ ingest process")
        logger.info("="*60)
        
        latencies = []
        failed_writes = 0
        
        for i in range(iterations):
            timestamp = int(time.time() * 1000)
            
            try:
                start = time.time()
                self.ingest_conn('.ingestRealTimeData',
                               timestamp,
                               'TEST',
                               100.0, 101.0, 99.0, 100.5,
                               1000000)
                latency_ms = (time.time() - start) * 1000
                latencies.append(latency_ms)
                
                # Log progress every 20 iterations
                if (i + 1) % 20 == 0:
                    current_avg = sum(latencies) / len(latencies)
                    logger.info(f"Progress: {i+1}/{iterations} writes | "
                               f"Current avg latency: {current_avg:.2f}ms")
                
            except Exception as e:
                failed_writes += 1
                logger.error(f"Write {i+1} failed: {e}")
            
            time.sleep(0.01)  # Small delay between tests
        
        logger.info(f"✅ Completed {len(latencies)} successful writes, {failed_writes} failures")
        return self._analyze_latencies(latencies, "Ingest Write")
    
    def measure_query_latency(self, query_type='simple'):
        """Measure query response latency"""
        logger.info("\n" + "="*60)
        logger.info(f"TEST 2: MEASURING QUERY LATENCY ({query_type.upper()})")
        logger.info("="*60)
        
        queries = {
            'simple': "count realTimeData",
            'aggregation': "select last close by sym from realTimeData",
            'complex': "select avg close, max high, min low by sym, 5 xbar time.minute from realTimeData"
        }
        
        query = queries.get(query_type, queries['simple'])
        logger.info(f"Query: {query}")
        logger.info(f"Executing query 100 times...")
        
        latencies = []
        errors = 0
        
        for i in range(100):
            try:
                start = time.time()
                result = self.ingest_conn(query)
                latency_ms = (time.time() - start) * 1000
                latencies.append(latency_ms)
                
                # Log first result size
                if i == 0:
                    try:
                        result_df = result.pd()
                        logger.info(f"Query returned {len(result_df)} rows")
                    except:
                        logger.info(f"Query returned: {result.py()}")
                
                # Log progress every 25 iterations
                if (i + 1) % 25 == 0:
                    current_avg = sum(latencies) / len(latencies)
                    logger.info(f"Progress: {i+1}/100 queries | "
                               f"Current avg latency: {current_avg:.2f}ms")
                
            except Exception as e:
                errors += 1
                logger.error(f"Query {i+1} failed: {e}")
            
            time.sleep(0.01)
        
        logger.info(f"✅ Completed {len(latencies)} successful queries, {errors} failures")
        return self._analyze_latencies(latencies, f"Query ({query_type})")
    
    def measure_processing_latency(self):
        """
        Measure processing latency by comparing timestamps
        between realTimeData and trade tables
        """
        logger.info("\n" + "="*60)
        logger.info("TEST 3: MEASURING PROCESSING LATENCY")
        logger.info("Comparing timestamps between ingest and processing tables")
        logger.info("="*60)
        
        try:
            # Get latest records from both tables
            logger.info("Querying latest records from realTimeData...")
            ingest_latest = self.ingest_conn("select last time by sym from realTimeData").pd()
            logger.info(f"Found {len(ingest_latest)} symbols in realTimeData")
            
            logger.info("Querying latest records from trade table...")
            process_latest = self.process_conn("select last time by sym from trade").pd()
            logger.info(f"Found {len(process_latest)} symbols in trade table")
            
            if ingest_latest.empty:
                logger.warning("⚠️ No data in realTimeData table")
                return None
            
            if process_latest.empty:
                logger.warning("⚠️ No data in trade table (processing may not have run yet)")
                return None
            
            # Calculate time difference for each symbol
            results = {}
            logger.info("\nCalculating processing delays...")
            for symbol in ingest_latest.index:
                if symbol in process_latest.index:
                    ingest_time = ingest_latest.loc[symbol, 'time']
                    process_time = process_latest.loc[symbol, 'time']
                    
                    # Convert to timestamps and calculate difference
                    delay_seconds = (process_time - ingest_time).total_seconds()
                    results[symbol] = delay_seconds
                    logger.info(f"  {symbol}: {delay_seconds:.2f}s delay")
                else:
                    logger.warning(f"  {symbol}: Not yet processed")
            
            if results:
                avg_delay = statistics.mean(results.values())
                min_delay = min(results.values())
                max_delay = max(results.values())
                
                logger.info(f"\n📊 Processing Latency Summary:")
                logger.info(f"  Average: {avg_delay:.2f}s")
                logger.info(f"  Min: {min_delay:.2f}s")
                logger.info(f"  Max: {max_delay:.2f}s")
                logger.info(f"  Symbols processed: {len(results)}")
                
                return results
            else:
                logger.warning("⚠️ No matching symbols found between tables")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error measuring processing latency: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def measure_data_freshness(self, duration_seconds=60):
        """
        Continuously monitor how fresh the data is
        (time between now and latest data point)
        """
        print(f"\n{'='*60}")
        print(f"MONITORING DATA FRESHNESS ({duration_seconds}s)")
        print(f"{'='*60}")
        
        freshness_samples = []
        end_time = time.time() + duration_seconds
        
        while time.time() < end_time:
            try:
                latest = self.ingest_conn("select max time from realTimeData").py()
                if latest:
                    latest_time = latest[0][0] if isinstance(latest, list) else latest
                    now = datetime.now()
                    
                    # Calculate age of latest data
                    age_seconds = (now - latest_time).total_seconds() if hasattr(latest_time, 'total_seconds') else 0
                    freshness_samples.append(age_seconds)
                    
                    if len(freshness_samples) % 10 == 0:
                        print(f"Current data age: {age_seconds:.2f}s")
            
            except Exception as e:
                print(f"⚠️ Error checking freshness: {e}")
            
            time.sleep(1)
        
        if freshness_samples:
            return self._analyze_latencies(freshness_samples, "Data Freshness (age)")
        return None
    
    def measure_round_trip_latency(self, iterations=50):
        """
        Measure complete round-trip latency:
        Write -> Process -> Read back
        """
        logger.info("\n" + "="*60)
        logger.info("TEST 4: MEASURING ROUND-TRIP LATENCY")
        logger.info(f"Testing Write -> Read cycle ({iterations} iterations)")
        logger.info("="*60)
        
        latencies = []
        failures = 0
        symbol = 'LATENCY_TEST'
        
        for i in range(iterations):
            # Write unique data
            test_price = 100.0 + i
            
            try:
                start = time.time()
                
                # 1. Write to ingest
                self.ingest_conn('.ingestRealTimeData',
                               int(time.time() * 1000),
                               symbol,
                               test_price, test_price + 1, test_price - 1, test_price,
                               1000000)
                
                # 2. Small wait
                time.sleep(0.1)
                
                # 3. Read back
                result = self.ingest_conn(f"select from realTimeData where sym=`{symbol}, close={test_price}")
                
                latency_ms = (time.time() - start) * 1000
                
                if not result.pd().empty:
                    latencies.append(latency_ms)
                    
                    # Log progress every 10 iterations
                    if (i + 1) % 10 == 0:
                        current_avg = sum(latencies) / len(latencies)
                        logger.info(f"Progress: {i+1}/{iterations} round-trips | "
                                   f"Current avg latency: {current_avg:.2f}ms")
                else:
                    failures += 1
                    logger.warning(f"Round-trip {i+1}: Data not found after write")
                
            except Exception as e:
                failures += 1
                logger.error(f"Round-trip {i+1} failed: {e}")
            
            time.sleep(0.5)
        
        logger.info(f"✅ Completed {len(latencies)} successful round-trips, {failures} failures")
        return self._analyze_latencies(latencies, "Round-Trip")
    
    def _analyze_latencies(self, latencies, test_name):
        """Analyze and display latency statistics"""
        if not latencies:
            logger.warning(f"⚠️ No latency data collected for {test_name}")
            return None
        
        sorted_lat = sorted(latencies)
        stats = {
            'test': test_name,
            'count': len(latencies),
            'min': min(latencies),
            'max': max(latencies),
            'mean': statistics.mean(latencies),
            'median': statistics.median(latencies),
            'p95': sorted_lat[int(len(sorted_lat) * 0.95)],
            'p99': sorted_lat[int(len(sorted_lat) * 0.99)],
            'stdev': statistics.stdev(latencies) if len(latencies) > 1 else 0
        }
        
        logger.info(f"\n📊 {test_name} - Latency Statistics:")
        logger.info(f"  Samples: {stats['count']}")
        logger.info(f"  Min:     {stats['min']:.2f} ms")
        logger.info(f"  Mean:    {stats['mean']:.2f} ms")
        logger.info(f"  Median:  {stats['median']:.2f} ms")
        logger.info(f"  P95:     {stats['p95']:.2f} ms")
        logger.info(f"  P99:     {stats['p99']:.2f} ms")
        logger.info(f"  Max:     {stats['max']:.2f} ms")
        logger.info(f"  StdDev:  {stats['stdev']:.2f} ms")
        
        # Performance assessment
        if test_name.startswith("Ingest"):
            if stats['p95'] < 1.0:
                logger.info("  ✅ Performance: Excellent (p95 < 1ms)")
            elif stats['p95'] < 5.0:
                logger.info("  ✅ Performance: Good (p95 < 5ms)")
            elif stats['p95'] < 10.0:
                logger.info("  ⚠️ Performance: Acceptable (p95 < 10ms)")
            else:
                logger.info("  ❌ Performance: Poor (p95 > 10ms) - Consider optimization")
        
        return stats
    
    def run_full_latency_analysis(self):
        """Run complete latency analysis suite"""
        logger.info("\n" + "="*70)
        logger.info("🚀 STARTING FULL LATENCY ANALYSIS SUITE")
        logger.info("="*70)
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70 + "\n")
        
        if not self.connect():
            logger.error("❌ Failed to connect to KDB+ processes. Aborting tests.")
            return
        
        results = {}
        start_time = time.time()
        
        # Test 1: Ingest write latency
        logger.info("\n🔹 Starting Test 1 of 6: Ingest Write Latency")
        results['ingest_write'] = self.measure_ingest_latency(100)
        logger.info("⏸️  Cooling down for 2 seconds...\n")
        time.sleep(2)
        
        # Test 2: Query latencies (3 types)
        logger.info("\n🔹 Starting Test 2 of 6: Simple Query Latency")
        results['query_simple'] = self.measure_query_latency('simple')
        logger.info("⏸️  Cooling down for 2 seconds...\n")
        time.sleep(2)
        
        logger.info("\n🔹 Starting Test 3 of 6: Aggregation Query Latency")
        results['query_aggregation'] = self.measure_query_latency('aggregation')
        logger.info("⏸️  Cooling down for 2 seconds...\n")
        time.sleep(2)
        
        logger.info("\n🔹 Starting Test 4 of 6: Complex Query Latency")
        results['query_complex'] = self.measure_query_latency('complex')
        logger.info("⏸️  Cooling down for 2 seconds...\n")
        time.sleep(2)
        
        # Test 3: Processing latency
        logger.info("\n🔹 Starting Test 5 of 6: Processing Latency")
        results['processing'] = self.measure_processing_latency()
        logger.info("⏸️  Cooling down for 2 seconds...\n")
        time.sleep(2)
        
        # Test 4: Round-trip
        logger.info("\n🔹 Starting Test 6 of 6: Round-Trip Latency")
        results['round_trip'] = self.measure_round_trip_latency(50)
        
        # Calculate total test time
        total_time = time.time() - start_time
        
        # Save results
        output = {
            'timestamp': datetime.now().isoformat(),
            'total_duration_seconds': total_time,
            'results': results
        }
        
        with open('latency_analysis.json', 'w') as f:
            json.dump(output, f, indent=2, default=str)
        
        logger.info("\n" + "="*70)
        logger.info("✅ LATENCY ANALYSIS SUITE COMPLETED")
        logger.info("="*70)
        logger.info(f"Total test duration: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Results saved to: latency_analysis.json")
        logger.info("="*70 + "\n")
        
        # Print summary
        logger.info("📊 PERFORMANCE SUMMARY:")
        if results.get('ingest_write'):
            logger.info(f"  Ingest Write: {results['ingest_write']['mean']:.2f}ms avg, "
                       f"{results['ingest_write']['p95']:.2f}ms p95")
        if results.get('query_simple'):
            logger.info(f"  Simple Query: {results['query_simple']['mean']:.2f}ms avg, "
                       f"{results['query_simple']['p95']:.2f}ms p95")
        if results.get('round_trip'):
            logger.info(f"  Round-Trip: {results['round_trip']['mean']:.2f}ms avg, "
                       f"{results['round_trip']['p95']:.2f}ms p95")
        
        return results


if __name__ == "__main__":
    monitor = LatencyMonitor()
    monitor.run_full_latency_analysis()

