// Connect to the ticker plant (RDB) running on localhost port 5006
h: hopen `:localhost:5006;

// Create a view into the realTimeData table on the RDB
// The :: makes it a global variable visible to the timer
realTimeData:: h"realTimeData";

// Define the schema for our new processed table, including the new calculated columns
trade:([] time:`timestamp$(); sym:`symbol$(); high:`float$(); low:`float$(); open:`float$(); close:`float$(); volume:`int$(); 
    moving_average_20:`float$(); expo_average_20:`float$(); expo_average_50:`float$();
    rsi_14:`float$(); macd:`float$(); macd_signal:`float$(); 
    bb_upper:`float$(); bb_middle:`float$(); bb_lower:`float$());

// --- Helper functions for indicators ---
// RSI calculation (Relative Strength Index)
rsi:{[period; prices]
    diffs: deltas prices;
    gains: diffs where diffs > 0;
    losses: abs diffs where diffs < 0;
    avgGain: mavg[period; gains];
    avgLoss: mavg[period; losses];
    rs: avgGain % avgLoss;
    rsiVal: 100 - 100 % 1 + rs;
    :rsiVal
    };

// MACD calculation (Moving Average Convergence Divergence)
macd:{[prices]
    ema12: ema[12; prices];
    ema26: ema[26; prices];
    macdLine: ema12 - ema26;
    signal: ema[9; macdLine];
    :(macdLine; signal)
    };

// Bollinger Bands calculation
bollinger:{[period; numStdDev; prices]
    movingAvg: mavg[period; prices];
    deviation: numStdDev * mdev[period; prices];
    upperBand: movingAvg + deviation;
    lowerBand: movingAvg - deviation;
    :(upperBand; movingAvg; lowerBand)
    };

// --- Define the processing function ---
calculateMovingAverage:{[data]
    closes: data`close;
    
    // Moving averages
    ma20: mavg[20; closes];
    ema20: ema[20; closes];
    ema50: ema[50; closes];
    
    // RSI (14-period)
    rsi14: rsi[14; closes];
    
    // MACD
    macdData: macd[closes];
    macdLine: macdData[0];
    macdSignal: macdData[1];
    
    // Bollinger Bands (20-period, 2 std dev)
    bbData: bollinger[20; 2; closes];
    bbUpper: bbData[0];
    bbMiddle: bbData[1];
    bbLower: bbData[2];
    
    // Get the last values
    lastRow: last data;
    
    // Insert the final processed row into our local 'trade' table
    `trade insert (lastRow`time; lastRow`sym; lastRow`high; lastRow`low; lastRow`open; lastRow`close; lastRow`volume; 
        last ma20; last ema20; last ema50;
        last rsi14; last macdLine; last macdSignal;
        last bbUpper; last bbMiddle; last bbLower);
    };

// --- Set up a timer ---
// This timer will trigger every 10 seconds (10000 ms)
.z.ts: {
    // Process each symbol separately
    dataAAPL: select from realTimeData where sym=`AAPL;
    dataTSLA: select from realTimeData where sym=`TSLA;
    dataGOOG: select from realTimeData where sym=`GOOG;
    
    if[0 < count dataAAPL; calculateMovingAverage[dataAAPL]];
    if[0 < count dataTSLA; calculateMovingAverage[dataTSLA]];
    if[0 < count dataGOOG; calculateMovingAverage[dataGOOG]];
    };

// Set timer interval in milliseconds
\t 10000; 

\