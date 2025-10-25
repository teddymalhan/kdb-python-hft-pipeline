import websocket
import json
import pykx as kx

import time
import sys
import logging
import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
logger = logging.getLogger()

# Authentication token for Finazon (replace with your actual token)
api_key = ''
dataset = 'us_stocks_essential'  # Free tier dataset
tickers = ['AAPL', 'TSLA']  # Trial account allows max 2 concurrent tickers

# Global variables
q = None
ws = None


def on_message(ws, message):
    try:
        data = json.loads(message)
        logger.debug(f"Received data: {data}")
        
        # Handle incoming status messages
        if isinstance(data, list) and len(data) > 0:
            if 'ev' in data[0] and data[0]['ev'] == 'status':
                logger.info(f"Status message: {data}")
                return
            # Handle list of bar data
            for bar in data:
                if 's' in bar:  # Check if it's a valid bar with symbol
                    process_bar(bar)
        elif isinstance(data, dict) and 's' in data:
            # Handle single bar data
            process_bar(data)

    except Exception as e:
        logger.error(f"Error in on_message: {e}")

def process_bar(bar_data):
    """Process and ingest a single bar of data"""
    try:
        # Send data to KDB+ (order: time, sym, open, high, low, close, volume)
        logger.debug(f"Ingesting bar: {bar_data}")
        q('.ingestRealTimeData', 
          bar_data.get('t'), 
          bar_data.get('s'), 
          bar_data.get('o'), 
          bar_data.get('h'), 
          bar_data.get('l'), 
          bar_data.get('c'), 
          bar_data.get('v'))
    except Exception as e:
        logger.error(f"Error processing bar: {e}")

def on_error(ws, error):
    logger.error(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info(f"### closed ### Code: {close_status_code}, Reason: {close_msg}")
    logger.info("Attempting to reconnect in 5 seconds...")
    time.sleep(5)
    reconnect()
    
def subscribe(wsapp, dataset, tickers):
    sub_request = {
        'event': 'subscribe',
        'dataset': dataset,
        'tickers': tickers,
        'channel': 'bars',
        'frequency': '10s',
        'aggregation': '1m'
    }
    wsapp.send(json.dumps(sub_request))

def on_open(ws):
    logger.info("### opened ###")
    try:
        print('Connection is opened')
        subscribe(ws, dataset, tickers)
    except Exception as e:
        logger.error(f"Error in on_open: {e}")

def reconnect():
    """Reconnect to websocket"""
    logger.info("Reconnecting...")
    start_websocket()

def start_websocket():
    """Start the websocket connection"""
    global ws
    ws = websocket.WebSocketApp(f'wss://ws.finazon.io/v1?apikey={api_key}',
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    logger.info("Starting websocket connection...")
    ws.run_forever()

if __name__ == "__main__":
    websocket.enableTrace(True)
    
    logger.info("start")

    # Initialize and open connection to KDB+ ingest process
    global q
    try:
        q = kx.SyncQConnection(host='localhost', port=5006)
        logger.info("Connected to KDB+ ingest process on port 5006")
    except Exception as e:
        logger.error(f"Error connecting to KDB+: {e}")
        sys.exit(1)

    # Start websocket with auto-reconnect
    start_websocket()
