"""
Stress Test Data Generator for HFT Platform
Simulates high-frequency market data to test system limits
"""

import pykx as kx
import time
import random
import threading
import logging
from datetime import datetime
from collections import defaultdict
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StressTestMetrics:
    """Track performance metrics during stress testing"""
    
    def __init__(self):
        self.messages_sent = 0
        self.errors = 0
        self.latencies = []
        self.start_time = None
        self.lock = threading.Lock()
    
    def record_message(self, latency_ms=None):
        with self.lock:
            self.messages_sent += 1
            if latency_ms:
                self.latencies.append(latency_ms)
    
    def record_error(self):
        with self.lock:
            self.errors += 1
    
    def get_stats(self):
        with self.lock:
            duration = time.time() - self.start_time if self.start_time else 0
            throughput = self.messages_sent / duration if duration > 0 else 0
            
            latency_stats = {}
            if self.latencies:
                sorted_lat = sorted(self.latencies)
                latency_stats = {
                    'min': min(self.latencies),
                    'max': max(self.latencies),
                    'avg': sum(self.latencies) / len(self.latencies),
                    'p50': sorted_lat[len(sorted_lat) // 2],
                    'p95': sorted_lat[int(len(sorted_lat) * 0.95)],
                    'p99': sorted_lat[int(len(sorted_lat) * 0.99)]
                }
            
            return {
                'messages_sent': self.messages_sent,
                'errors': self.errors,
                'duration_seconds': duration,
                'throughput_msg_per_sec': throughput,
                'error_rate': self.errors / self.messages_sent if self.messages_sent > 0 else 0,
                'latency_ms': latency_stats
            }


class MarketDataGenerator:
    """Generate realistic market data for stress testing"""
    
    def __init__(self, symbols, initial_prices=None):
        self.symbols = symbols
        self.prices = initial_prices or {sym: random.uniform(100, 500) for sym in symbols}
        self.volumes = {sym: random.randint(1000000, 5000000) for sym in symbols}
    
    def generate_bar(self, symbol):
        """Generate a realistic OHLCV bar"""
        # Random walk price movement
        change_pct = random.gauss(0, 0.002)  # 0.2% std dev
        close = self.prices[symbol] * (1 + change_pct)
        
        # OHLC with realistic relationships
        high = close * (1 + abs(random.gauss(0, 0.001)))
        low = close * (1 - abs(random.gauss(0, 0.001)))
        open_price = random.uniform(low, high)
        
        # Volume with randomness
        volume = int(self.volumes[symbol] * random.uniform(0.5, 1.5))
        
        # Update price for next bar
        self.prices[symbol] = close
        
        return {
            'time': int(time.time() * 1000),  # milliseconds
            'symbol': symbol,
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': volume
        }


class StressTester:
    """Main stress testing orchestrator"""
    
    def __init__(self, host='localhost', port=5006):
        self.host = host
        self.port = port
        self.conn = None
        self.metrics = StressTestMetrics()
        self.generator = None
    
    def connect(self):
        """Connect to KDB+ ingest process"""
        try:
            self.conn = kx.SyncQConnection(host=self.host, port=self.port)
            logger.info(f"Connected to KDB+ on {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def send_bar(self, bar):
        """Send a single bar to KDB+ and measure latency"""
        try:
            start = time.time()
            self.conn('.ingestRealTimeData',
                     bar['time'],
                     bar['symbol'],
                     bar['open'],
                     bar['high'],
                     bar['low'],
                     bar['close'],
                     bar['volume'])
            latency_ms = (time.time() - start) * 1000
            self.metrics.record_message(latency_ms)
            return True
        except Exception as e:
            logger.error(f"Error sending bar: {e}")
            self.metrics.record_error()
            return False
    
    def burst_test(self, symbols, messages_per_symbol=100, delay_ms=0):
        """
        Test 1: Burst Load Test
        Send rapid bursts of messages to test throughput
        """
        logger.info(f"\n{'='*60}")
        logger.info("TEST 1: BURST LOAD TEST")
        logger.info(f"Symbols: {len(symbols)}, Messages/symbol: {messages_per_symbol}")
        logger.info(f"Delay between messages: {delay_ms}ms")
        logger.info(f"{'='*60}\n")
        
        self.generator = MarketDataGenerator(symbols)
        self.metrics = StressTestMetrics()
        self.metrics.start_time = time.time()
        
        for i in range(messages_per_symbol):
            for symbol in symbols:
                bar = self.generator.generate_bar(symbol)
                self.send_bar(bar)
                
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
            
            if (i + 1) % 10 == 0:
                stats = self.metrics.get_stats()
                logger.info(f"Progress: {i+1}/{messages_per_symbol} batches | "
                           f"Throughput: {stats['throughput_msg_per_sec']:.2f} msg/s")
        
        return self.metrics.get_stats()
    
    def sustained_load_test(self, symbols, duration_seconds=60, msg_per_second=10):
        """
        Test 2: Sustained Load Test
        Maintain constant message rate for extended period
        """
        logger.info(f"\n{'='*60}")
        logger.info("TEST 2: SUSTAINED LOAD TEST")
        logger.info(f"Duration: {duration_seconds}s, Target: {msg_per_second} msg/s")
        logger.info(f"{'='*60}\n")
        
        self.generator = MarketDataGenerator(symbols)
        self.metrics = StressTestMetrics()
        self.metrics.start_time = time.time()
        
        interval = 1.0 / msg_per_second
        end_time = time.time() + duration_seconds
        
        while time.time() < end_time:
            symbol = random.choice(symbols)
            bar = self.generator.generate_bar(symbol)
            self.send_bar(bar)
            time.sleep(interval)
            
            # Log progress every 10 seconds
            if int(time.time() - self.metrics.start_time) % 10 == 0:
                stats = self.metrics.get_stats()
                logger.info(f"Running... {stats['messages_sent']} messages sent | "
                           f"Errors: {stats['errors']}")
        
        return self.metrics.get_stats()
    
    def spike_test(self, symbols, normal_rate=5, spike_rate=100, 
                   spike_duration=10, total_duration=60):
        """
        Test 3: Spike Test
        Simulate normal load with periodic spikes
        """
        logger.info(f"\n{'='*60}")
        logger.info("TEST 3: SPIKE TEST")
        logger.info(f"Normal: {normal_rate} msg/s, Spike: {spike_rate} msg/s")
        logger.info(f"Spike duration: {spike_duration}s, Total: {total_duration}s")
        logger.info(f"{'='*60}\n")
        
        self.generator = MarketDataGenerator(symbols)
        self.metrics = StressTestMetrics()
        self.metrics.start_time = time.time()
        
        end_time = time.time() + total_duration
        last_spike = time.time()
        
        while time.time() < end_time:
            elapsed = time.time() - last_spike
            
            # Determine if in spike period
            if elapsed < spike_duration:
                rate = spike_rate
                logger.info("🔥 SPIKE ACTIVE")
            else:
                rate = normal_rate
                if elapsed > spike_duration + 20:  # Spike every 30s
                    last_spike = time.time()
            
            interval = 1.0 / rate
            symbol = random.choice(symbols)
            bar = self.generator.generate_bar(symbol)
            self.send_bar(bar)
            time.sleep(interval)
        
        return self.metrics.get_stats()
    
    def concurrent_connections_test(self, symbols, num_connections=5, 
                                   messages_per_connection=50):
        """
        Test 4: Concurrent Connections
        Multiple clients sending simultaneously
        """
        logger.info(f"\n{'='*60}")
        logger.info("TEST 4: CONCURRENT CONNECTIONS TEST")
        logger.info(f"Connections: {num_connections}, Messages/connection: {messages_per_connection}")
        logger.info(f"{'='*60}\n")
        
        def worker(worker_id):
            try:
                conn = kx.SyncQConnection(host=self.host, port=self.port)
                generator = MarketDataGenerator(symbols)
                
                for i in range(messages_per_connection):
                    symbol = random.choice(symbols)
                    bar = generator.generate_bar(symbol)
                    
                    start = time.time()
                    conn('.ingestRealTimeData',
                        bar['time'], bar['symbol'], bar['open'],
                        bar['high'], bar['low'], bar['close'], bar['volume'])
                    latency_ms = (time.time() - start) * 1000
                    
                    self.metrics.record_message(latency_ms)
                
                logger.info(f"Worker {worker_id} completed")
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                self.metrics.record_error()
        
        self.metrics = StressTestMetrics()
        self.metrics.start_time = time.time()
        
        threads = []
        for i in range(num_connections):
            t = threading.Thread(target=worker, args=(i,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        return self.metrics.get_stats()
    
    def memory_leak_test(self, symbols, duration_minutes=5):
        """
        Test 5: Memory Leak Detection
        Monitor memory usage over extended period
        """
        logger.info(f"\n{'='*60}")
        logger.info("TEST 5: MEMORY LEAK TEST")
        logger.info(f"Duration: {duration_minutes} minutes")
        logger.info(f"{'='*60}\n")
        
        self.generator = MarketDataGenerator(symbols)
        self.metrics = StressTestMetrics()
        self.metrics.start_time = time.time()
        
        end_time = time.time() + (duration_minutes * 60)
        check_interval = 30  # Check memory every 30s
        last_check = time.time()
        
        memory_samples = []
        
        while time.time() < end_time:
            symbol = random.choice(symbols)
            bar = self.generator.generate_bar(symbol)
            self.send_bar(bar)
            time.sleep(0.1)  # 10 msg/s
            
            # Periodic memory check
            if time.time() - last_check > check_interval:
                try:
                    # Query KDB+ for memory usage
                    mem_usage = self.conn('.Q.w[]').py()
                    memory_samples.append({
                        'time': time.time() - self.metrics.start_time,
                        'used_mb': mem_usage.get('used', 0) / (1024 * 1024),
                        'heap_mb': mem_usage.get('heap', 0) / (1024 * 1024),
                        'peak_mb': mem_usage.get('peak', 0) / (1024 * 1024)
                    })
                    logger.info(f"Memory: {memory_samples[-1]}")
                except Exception as e:
                    logger.error(f"Memory check failed: {e}")
                
                last_check = time.time()
        
        stats = self.metrics.get_stats()
        stats['memory_samples'] = memory_samples
        return stats
    
    def print_results(self, test_name, stats):
        """Pretty print test results"""
        logger.info(f"\n{'='*60}")
        logger.info(f"RESULTS: {test_name}")
        logger.info(f"{'='*60}")
        logger.info(f"Messages Sent: {stats['messages_sent']}")
        logger.info(f"Errors: {stats['errors']} ({stats['error_rate']*100:.2f}%)")
        logger.info(f"Duration: {stats['duration_seconds']:.2f}s")
        logger.info(f"Throughput: {stats['throughput_msg_per_sec']:.2f} msg/s")
        
        if stats.get('latency_ms'):
            lat = stats['latency_ms']
            logger.info(f"\nLatency (ms):")
            logger.info(f"  Min: {lat['min']:.2f}")
            logger.info(f"  Avg: {lat['avg']:.2f}")
            logger.info(f"  P50: {lat['p50']:.2f}")
            logger.info(f"  P95: {lat['p95']:.2f}")
            logger.info(f"  P99: {lat['p99']:.2f}")
            logger.info(f"  Max: {lat['max']:.2f}")
        
        logger.info(f"{'='*60}\n")
    
    def run_all_tests(self):
        """Run complete stress test suite"""
        if not self.connect():
            return
        
        symbols = ['AAPL', 'TSLA', 'GOOGL', 'MSFT', 'AMZN']
        
        # Test 1: Burst
        stats1 = self.burst_test(symbols, messages_per_symbol=100, delay_ms=0)
        self.print_results("Burst Load Test", stats1)
        time.sleep(5)
        
        # Test 2: Sustained
        stats2 = self.sustained_load_test(symbols, duration_seconds=60, msg_per_second=20)
        self.print_results("Sustained Load Test", stats2)
        time.sleep(5)
        
        # Test 3: Spike
        stats3 = self.spike_test(symbols, normal_rate=5, spike_rate=50)
        self.print_results("Spike Test", stats3)
        time.sleep(5)
        
        # Test 4: Concurrent
        stats4 = self.concurrent_connections_test(symbols, num_connections=5)
        self.print_results("Concurrent Connections Test", stats4)
        time.sleep(5)
        
        # Test 5: Memory (shorter for demo)
        stats5 = self.memory_leak_test(symbols, duration_minutes=2)
        self.print_results("Memory Leak Test", stats5)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("STRESS TEST SUITE COMPLETED")
        logger.info("="*60)
        
        # Save results to JSON
        results = {
            'timestamp': datetime.now().isoformat(),
            'tests': {
                'burst_load': stats1,
                'sustained_load': stats2,
                'spike': stats3,
                'concurrent': stats4,
                'memory_leak': stats5
            }
        }
        
        with open('stress_test_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info("Results saved to stress_test_results.json")


if __name__ == "__main__":
    tester = StressTester(host='localhost', port=5006)
    
    # Run individual test or full suite
    import sys
    if len(sys.argv) > 1:
        test_type = sys.argv[1]
        symbols = ['AAPL', 'TSLA', 'GOOGL', 'MSFT', 'AMZN']
        
        if tester.connect():
            if test_type == 'burst':
                stats = tester.burst_test(symbols, messages_per_symbol=100)
            elif test_type == 'sustained':
                stats = tester.sustained_load_test(symbols, duration_seconds=60)
            elif test_type == 'spike':
                stats = tester.spike_test(symbols)
            elif test_type == 'concurrent':
                stats = tester.concurrent_connections_test(symbols)
            elif test_type == 'memory':
                stats = tester.memory_leak_test(symbols, duration_minutes=5)
            else:
                print("Unknown test type. Options: burst, sustained, spike, concurrent, memory")
                sys.exit(1)
            
            tester.print_results(test_type.upper(), stats)
    else:
        # Run full suite
        tester.run_all_tests()

