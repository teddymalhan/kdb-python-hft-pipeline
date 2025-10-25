import streamlit as st
import pykx as kx
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="HFT Real-Time Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .stAlert {
        margin-top: 10px;
    }
    </style>
""", unsafe_allow_html=True)

# Title and description
st.title("📈 High-Frequency Trading Dashboard")
st.markdown("Real-time monitoring of stock data with technical indicators")

# Sidebar configuration
st.sidebar.header("⚙️ Configuration")
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 1, 30, 5)
selected_symbols = st.sidebar.multiselect(
    "Select Symbols",
    ["AAPL", "TSLA", "GOOG"],
    default=["AAPL", "TSLA", "GOOG"]
)

# Connection status
st.sidebar.header("🔌 Connection Status")

@st.cache_resource
def connect_to_kdb():
    """Establish connections to KDB+ processes"""
    try:
        ingest_conn = kx.SyncQConnection(host='localhost', port=5006)
        process_conn = kx.SyncQConnection(host='localhost', port=5011)
        return ingest_conn, process_conn, True
    except Exception as e:
        st.sidebar.error(f"Connection failed: {e}")
        return None, None, False

ingest_conn, process_conn, connected = connect_to_kdb()

if connected:
    st.sidebar.success("✅ Connected to KDB+")
    st.sidebar.info("🔵 Port 5006: Ingest")
    st.sidebar.info("🔵 Port 5011: Processing")
    
    # Test connection with simple query
    try:
        test_result = ingest_conn("1+1").py()
        st.sidebar.success(f"✅ Ingest responsive")
    except Exception as e:
        st.sidebar.error(f"⚠️ Ingest connection issue: {e}")
    
    try:
        test_result = process_conn("1+1").py()
        st.sidebar.success(f"✅ Processing responsive")
    except Exception as e:
        st.sidebar.error(f"⚠️ Processing connection issue: {e}")
else:
    st.sidebar.error("❌ Not Connected")
    st.error("Cannot connect to KDB+ processes. Make sure they are running on ports 5006 and 5011.")
    st.stop()

def get_realtime_data(conn, symbols):
    """Fetch real-time data from ingest process"""
    try:
        # Query for selected symbols
        if symbols:
            symbol_list = "`" + "`".join(symbols)
            query = f"select from realTimeData where sym in ({symbol_list})"
        else:
            query = "select from realTimeData"
        
        result = conn(query)
        df = result.pd()
        return df
    except Exception as e:
        # Only show error if it's not a "no data" situation
        if "type" not in str(e).lower():
            st.warning(f"Waiting for real-time data... ({str(e)[:50]})")
        return pd.DataFrame()

def get_processed_data(conn, symbols):
    """Fetch processed data with moving averages"""
    try:
        if symbols:
            symbol_list = "`" + "`".join(symbols)
            query = f"select from trade where sym in ({symbol_list})"
        else:
            query = "select from trade"
        
        result = conn(query)
        df = result.pd()
        return df
    except Exception as e:
        # Processed data may not exist yet - that's okay
        return pd.DataFrame()

def get_statistics(conn):
    """Get summary statistics"""
    stats = {}
    
    try:
        # Total records
        total_records = conn("count realTimeData").py()
        stats['total_records'] = total_records
    except Exception as e:
        stats['total_records'] = 0
        st.warning(f"Could not fetch total records: {str(e)}")
    
    try:
        # Records by symbol
        by_symbol = conn("select count i by sym from realTimeData")
        stats['by_symbol'] = by_symbol.pd()
    except Exception as e:
        stats['by_symbol'] = pd.DataFrame()
    
    try:
        # Latest prices
        latest = conn("select last close by sym from realTimeData")
        stats['latest_prices'] = latest.pd()
    except Exception as e:
        stats['latest_prices'] = pd.DataFrame()
    
    return stats

def create_price_chart(df, symbol):
    """Create interactive price chart with all indicators"""
    symbol_data = df[df['sym'] == symbol].sort_values('time')
    
    if symbol_data.empty:
        return None
    
    # Check if we have indicator data
    has_indicators = 'rsi_14' in symbol_data.columns and 'macd' in symbol_data.columns
    
    if has_indicators:
        # Full chart with all indicators
        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            subplot_titles=(f'{symbol} - Price & Indicators', 'Volume', 'RSI (14)', 'MACD'),
            row_heights=[0.45, 0.15, 0.2, 0.2]
        )
    else:
        # Simple chart with just price and volume
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=(f'{symbol} - Price & Moving Averages', 'Volume'),
            row_heights=[0.7, 0.3]
        )
    
    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=symbol_data['time'],
            open=symbol_data['open'],
            high=symbol_data['high'],
            low=symbol_data['low'],
            close=symbol_data['close'],
            name='Price'
        ),
        row=1, col=1
    )
    
    # Moving averages (if available)
    if 'moving_average_20' in symbol_data.columns:
        fig.add_trace(
            go.Scatter(
                x=symbol_data['time'],
                y=symbol_data['moving_average_20'],
                name='MA(20)',
                line=dict(color='orange', width=2)
            ),
            row=1, col=1
        )
    
    if 'expo_average_20' in symbol_data.columns:
        fig.add_trace(
            go.Scatter(
                x=symbol_data['time'],
                y=symbol_data['expo_average_20'],
                name='EMA(20)',
                line=dict(color='blue', width=2)
            ),
            row=1, col=1
        )
    
    if 'expo_average_50' in symbol_data.columns:
        fig.add_trace(
            go.Scatter(
                x=symbol_data['time'],
                y=symbol_data['expo_average_50'],
                name='EMA(50)',
                line=dict(color='red', width=2)
            ),
            row=1, col=1
        )
    
    # Bollinger Bands (if available)
    if 'bb_upper' in symbol_data.columns:
        fig.add_trace(
            go.Scatter(
                x=symbol_data['time'],
                y=symbol_data['bb_upper'],
                name='BB Upper',
                line=dict(color='gray', width=1, dash='dash'),
                showlegend=True
            ),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=symbol_data['time'],
                y=symbol_data['bb_lower'],
                name='BB Lower',
                line=dict(color='gray', width=1, dash='dash'),
                fill='tonexty',
                fillcolor='rgba(128, 128, 128, 0.1)',
                showlegend=True
            ),
            row=1, col=1
        )
    
    # Volume bars
    fig.add_trace(
        go.Bar(
            x=symbol_data['time'],
            y=symbol_data['volume'],
            name='Volume',
            marker_color='lightblue'
        ),
        row=2, col=1
    )
    
    # RSI (if available)
    if has_indicators and 'rsi_14' in symbol_data.columns:
        fig.add_trace(
            go.Scatter(
                x=symbol_data['time'],
                y=symbol_data['rsi_14'],
                name='RSI(14)',
                line=dict(color='purple', width=2)
            ),
            row=3, col=1
        )
        # Add overbought/oversold lines
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=3, col=1, annotation_text="Overbought")
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=3, col=1, annotation_text="Oversold")
        fig.add_hline(y=50, line_dash="dot", line_color="gray", row=3, col=1)
    
    # MACD (if available)
    if has_indicators and 'macd' in symbol_data.columns:
        fig.add_trace(
            go.Scatter(
                x=symbol_data['time'],
                y=symbol_data['macd'],
                name='MACD',
                line=dict(color='blue', width=2)
            ),
            row=4, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=symbol_data['time'],
                y=symbol_data['macd_signal'],
                name='Signal',
                line=dict(color='orange', width=2)
            ),
            row=4, col=1
        )
        # MACD histogram
        histogram = symbol_data['macd'] - symbol_data['macd_signal']
        fig.add_trace(
            go.Bar(
                x=symbol_data['time'],
                y=histogram,
                name='Histogram',
                marker_color=['green' if val >= 0 else 'red' for val in histogram]
            ),
            row=4, col=1
        )
    
    # Update layout
    if has_indicators:
        fig.update_layout(
            height=1000,
            showlegend=True,
            xaxis_rangeslider_visible=False,
            hovermode='x unified'
        )
        fig.update_xaxes(title_text="Time", row=4, col=1)
        fig.update_yaxes(title_text="Price ($)", row=1, col=1)
        fig.update_yaxes(title_text="Volume", row=2, col=1)
        fig.update_yaxes(title_text="RSI", row=3, col=1, range=[0, 100])
        fig.update_yaxes(title_text="MACD", row=4, col=1)
    else:
        fig.update_layout(
            height=600,
            showlegend=True,
            xaxis_rangeslider_visible=False,
            hovermode='x unified'
        )
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_yaxes(title_text="Price ($)", row=1, col=1)
        fig.update_yaxes(title_text="Volume", row=2, col=1)
    
    return fig

def detect_trading_signals(df):
    """Detect potential trading signals"""
    signals = []
    
    if df.empty or 'moving_average_20' not in df.columns:
        return signals
    
    for symbol in df['sym'].unique():
        symbol_data = df[df['sym'] == symbol].sort_values('time')
        
        if len(symbol_data) < 2:
            continue
        
        latest = symbol_data.iloc[-1]
        previous = symbol_data.iloc[-2]
        
        # Golden Cross: MA20 crosses above MA50
        if ('moving_average_20' in latest and 'expo_average_50' in latest and
            latest['moving_average_20'] > latest['expo_average_50'] and
            previous['moving_average_20'] <= previous['expo_average_50']):
            signals.append({
                'symbol': symbol,
                'signal': '🟢 BUY (Golden Cross)',
                'time': latest['time'],
                'price': latest['close']
            })
        
        # Death Cross: MA20 crosses below MA50
        elif ('moving_average_20' in latest and 'expo_average_50' in latest and
              latest['moving_average_20'] < latest['expo_average_50'] and
              previous['moving_average_20'] >= previous['expo_average_50']):
            signals.append({
                'symbol': symbol,
                'signal': '🔴 SELL (Death Cross)',
                'time': latest['time'],
                'price': latest['close']
            })
        
        # RSI Signals
        if 'rsi_14' in latest:
            if latest['rsi_14'] < 30:
                signals.append({
                    'symbol': symbol,
                    'signal': '🟢 BUY (RSI Oversold)',
                    'time': latest['time'],
                    'price': latest['close']
                })
            elif latest['rsi_14'] > 70:
                signals.append({
                    'symbol': symbol,
                    'signal': '🔴 SELL (RSI Overbought)',
                    'time': latest['time'],
                    'price': latest['close']
                })
        
        # MACD Crossover
        if 'macd' in latest and 'macd_signal' in latest:
            if (latest['macd'] > latest['macd_signal'] and 
                previous['macd'] <= previous['macd_signal']):
                signals.append({
                    'symbol': symbol,
                    'signal': '🟢 BUY (MACD Bullish Cross)',
                    'time': latest['time'],
                    'price': latest['close']
                })
            elif (latest['macd'] < latest['macd_signal'] and 
                  previous['macd'] >= previous['macd_signal']):
                signals.append({
                    'symbol': symbol,
                    'signal': '🔴 SELL (MACD Bearish Cross)',
                    'time': latest['time'],
                    'price': latest['close']
                })
    
    return signals

# Main dashboard layout
placeholder = st.empty()

# Initialize iteration counter for unique keys
if 'iteration' not in st.session_state:
    st.session_state.iteration = 0

# Auto-refresh loop
while True:
    st.session_state.iteration += 1
    with placeholder.container():
        # Fetch data
        realtime_df = get_realtime_data(ingest_conn, selected_symbols)
        processed_df = get_processed_data(process_conn, selected_symbols)
        stats = get_statistics(ingest_conn)
        
        # Header metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Records", stats.get('total_records', 0))
        
        with col2:
            if not realtime_df.empty:
                latest_time = realtime_df['time'].max()
                st.metric("🕐 Latest Update", latest_time.strftime('%H:%M:%S') if hasattr(latest_time, 'strftime') else str(latest_time))
            else:
                st.metric("🕐 Latest Update", "N/A")
        
        with col3:
            st.metric("📈 Symbols", len(selected_symbols))
        
        with col4:
            if not processed_df.empty:
                st.metric("🔄 Processed Records", len(processed_df))
            else:
                st.metric("🔄 Processed Records", 0)
        
        # Trading signals
        if not processed_df.empty:
            signals = detect_trading_signals(processed_df)
            if signals:
                st.subheader("🚨 Trading Signals")
                for signal in signals:
                    st.warning(f"{signal['signal']} - {signal['symbol']} @ ${signal['price']:.2f}")
        
        # Latest prices
        st.subheader("💰 Latest Prices")
        if 'latest_prices' in stats and not stats['latest_prices'].empty:
            price_cols = st.columns(len(selected_symbols))
            for idx, symbol in enumerate(selected_symbols):
                with price_cols[idx]:
                    if symbol in stats['latest_prices'].index:
                        price = stats['latest_prices'].loc[symbol, 'close']
                        st.metric(f"{symbol}", f"${price:.2f}")
        
        # Charts for each symbol
        st.subheader("📊 Real-Time Charts")
        
        # Determine which dataframe to use for charts
        chart_df = processed_df if not processed_df.empty else realtime_df
        
        if not chart_df.empty:
            # Create tabs for each symbol
            tabs = st.tabs(selected_symbols)
            
            for idx, symbol in enumerate(selected_symbols):
                with tabs[idx]:
                    fig = create_price_chart(chart_df, symbol)
                    if fig:
                        st.plotly_chart(fig, width='stretch', key=f"chart_{symbol}_{st.session_state.iteration}")
                    else:
                        st.info(f"No data available for {symbol} yet. Waiting for incoming data...")
        else:
            st.info("📡 Waiting for data... Make sure the websocket client is running!")
        
        # Data tables
        st.subheader("📋 Data Tables")
        
        tab1, tab2 = st.tabs(["Real-Time Data", "Processed Data (with Indicators)"])
        
        with tab1:
            if not realtime_df.empty:
                st.dataframe(
                    realtime_df.sort_values('time', ascending=False).head(50),
                    width='stretch'
                )
            else:
                st.info("No real-time data available yet")
        
        with tab2:
            if not processed_df.empty:
                st.dataframe(
                    processed_df.sort_values('time', ascending=False).head(50),
                    width='stretch'
                )
            else:
                st.info("No processed data available yet. Processing happens every 10 seconds.")
        
        # Statistics by symbol
        if 'by_symbol' in stats and not stats['by_symbol'].empty:
            st.subheader("📈 Records by Symbol")
            st.bar_chart(stats['by_symbol'])
        
        # Footer
        st.markdown("---")
        st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Auto-refresh every {refresh_rate} seconds")
    
    # Wait before next refresh
    time.sleep(refresh_rate)

