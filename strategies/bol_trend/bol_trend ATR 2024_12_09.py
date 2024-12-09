import sys
sys.path.append("./live_tools")
import ccxt
import ta
import pandas as pd
from utilities.perp_bitget import PerpBitget
from utilities.custom_indicators import get_n_columns
from utilities.var import ValueAtRisk
from datetime import datetime
import time
import json
import copy

now = datetime.now()
current_time = now.strftime("%d/%m/%Y %H:%M:%S")
print("--- Start Execution Time :", current_time, "---")

# Load API keys
f = open("./live_tools/secret.json")
secret = json.load(f)
f.close()

# Configuration
account_to_select = "bitget_exemple"
production = True
timeframe = "1h"
trade_type = ["long", "short"]
leverage = 1
max_var = 1
max_side_exposition = 1

params_coin = {
    "BTC/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    # Add other coins here...
}

# MODIFICATION ATR: AJOUT DE LA FONCTION DE CALCUL ATR
def calculate_atr(df, window=14):
    """CALCUL DE L'ATR POUR LES STOPS LOSS DYNAMIQUES"""
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift(1))
    low_close = abs(df['low'] - df['close'].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=window).mean()
    return atr

# Modifications des fonctions pour intégrer l'ATR
def open_long(row):
    """Logic to open a long position."""
    if (
        row['n1_close'] < row['n1_higher_band'] 
        and (row['close'] > row['higher_band']) 
        and (row['close'] > row['long_ma'])
    ):
        return True
    return False

# MODIFICATION ATR: SORTIE LONG AVEC ATR
def close_long(row, entry_price, atr, multiplier=2):
    """LOGIQUE DE SORTIE LONG AVEC ATR"""
    trailing_stop = entry_price - (atr * multiplier)
    if row['close'] < trailing_stop:
        return True
    return False

def open_short(row):
    """Logic to open a short position."""
    if (
        row['n1_close'] > row['n1_lower_band'] 
        and (row['close'] < row['lower_band']) 
        and (row['close'] < row['long_ma'])        
    ):
        return True
    return False

# MODIFICATION ATR: SORTIE SHORT AVEC ATR
def close_short(row, entry_price, atr, multiplier=2):
    """LOGIQUE DE SORTIE SHORT AVEC ATR"""
    trailing_stop = entry_price + (atr * multiplier)
    if row['close'] > trailing_stop:
        return True
    return False

print(f"--- Bollinger Trend on {len(params_coin)} tokens {timeframe} Leverage x{leverage} ---")

bitget = PerpBitget(
    apiKey=secret[account_to_select]["apiKey"],
    secret=secret[account_to_select]["secret"],
    password=secret[account_to_select]["password"],
)

# Get data
df_list = {}
for pair in params_coin:
    temp_data = bitget.get_more_last_historical_async(pair, timeframe, 1000)
    if len(temp_data) == 990:
        df_list[pair] = temp_data
    else:
        print(f"Pair {pair} not loaded, length: {len(temp_data)}")
print("Data OHLCV loaded 100%")

for pair in df_list:
    df = df_list[pair]
    params = params_coin[pair]
    bol_band = ta.volatility.BollingerBands(close=df["close"], window=params["bb_window"], window_dev=params["bb_std"])
    df["lower_band"] = bol_band.bollinger_lband()
    df["higher_band"] = bol_band.bollinger_hband()
    df["ma_band"] = bol_band.bollinger_mavg()

    df['long_ma'] = ta.trend.sma_indicator(close=df['close'], window=params["long_ma_window"])
    
    # MODIFICATION ATR: AJOUT DE L'ATR AUX INDICATEURS
    df['atr'] = calculate_atr(df)

    df["n1_close"] = df["close"].shift(1)
    df["n1_lower_band"] = df["lower_band"].shift(1)
    df["n1_higher_band"] = df["higher_band"].shift(1)

    df['iloc'] = range(len(df))

print("Indicators loaded 100%")

var = ValueAtRisk(df_list=df_list.copy())
var.update_cov(current_date=df_list["BTC/USDT:USDT"].index[-1], occurance_data=989)
print("Value At Risk loaded 100%")

usd_balance = float(bitget.get_usdt_equity())
print("USD balance :", round(usd_balance, 2), "$")

positions_data = bitget.get_open_position()
position_list = [
    {"pair": d["symbol"], "side": d["side"], "size": float(d["contracts"]) * float(d["contractSize"]), 
     "market_price":d["info"]["marketPrice"], 
     "usd_size": float(d["contracts"]) * float(d["contractSize"]) * float(d["info"]["marketPrice"]), 
     "open_price": d["entryPrice"]}
    for d in positions_data if d["symbol"] in df_list]

positions = {}
for pos in position_list:
    positions[pos["pair"]] = {"side": pos["side"], "size": pos["size"], "market_price": pos["market_price"], 
                              "usd_size": pos["usd_size"], "open_price": pos["open_price"]}

print(f"{len(positions)} active positions ({list(positions.keys())})")

# Closing Positions
positions_to_delete = []
for pair in positions:
    row = df_list[pair].iloc[-2]
    last_price = float(df_list[pair].iloc[-1]["close"])
    atr = df_list[pair].iloc[-1]["atr"]  # MODIFICATION ATR: RÉCUPÉRATION DE L'ATR
    position = positions[pair]

    if position["side"] == "long" and close_long(row, position["open_price"], atr):
        print(f"Closing long position on {pair}")
        if production:
            bitget.place_market_order(pair, "sell", position["size"], reduce=True)
            positions_to_delete.append(pair)

    elif position["side"] == "short" and close_short(row, position["open_price"], atr):
        print(f"Closing short position on {pair}")
        if production:
            bitget.place_market_order(pair, "buy", position["size"], reduce=True)
            positions_to_delete.append(pair)

for pair in positions_to_delete:
    del positions[pair]

# Opening Positions
for pair in df_list:
    if pair not in positions:
        row = df_list[pair].iloc[-2]
        last_price = float(df_list[pair].iloc[-1]["close"])
        params = params_coin[pair]
        pct_sizing = params["wallet_exposure"]
        if open_long(row) and "long" in trade_type:
            print(f"Opening long position on {pair}")
            # Add logic to place long order
        elif open_short(row) and "short" in trade_type:
            print(f"Opening short position on {pair}")
            # Add logic to place short order

now = datetime.now()
current_time = now.strftime("%d/%m/%Y %H:%M:%S")
print("--- End Execution Time :", current_time, "---")
