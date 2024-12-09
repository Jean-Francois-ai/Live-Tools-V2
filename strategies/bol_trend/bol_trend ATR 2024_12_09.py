import sys
sys.path.append("./Live-Tools-V2")
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
from secret import ACCOUNTS

now = datetime.now()
current_time = now.strftime("%d/%m/%Y %H:%M:%S")
print("--- Start Execution Time :", current_time, "---")

account = ACCOUNTS["bitget1"]

production = True
timeframe = "1h"
types = ["long", "short"]
leverage = 2
max_var = 1
max_side_exposition = 1

params_coin = {

    "BTC/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "AAVE/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "APE/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "APT/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "AVAX/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "AXS/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "C98/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "CRV/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "DOGE/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "DOT/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "DYDX/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "ETH/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "FIL/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "FTM/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "BNB/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "GALA/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "GMT/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "GRT/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "KNC/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "KSM/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "LRC/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "MANA/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "MASK/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "MATIC/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "NEAR/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "ONE/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "OP/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "SAND/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "SHIB/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "SOL/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "STG/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "WAVES/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "YFI/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "WOO/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 1,
        "long_ma_window": 500
    },
    "EGLD/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "ETC/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "JASMY/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "ROSE/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "XRP/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "EOS/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "BCH/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
    "LTC/USDT:USDT": {
        "wallet_exposure": 0.05,
        "bb_window": 100,
        "bb_std": 2.25,
        "long_ma_window": 500
    },
}

# AJOUT DE LA FONCTION DE CALCUL ATR
def calculate_atr(df, window=14):
    """CALCUL DE L'ATR POUR STOP LOSS DYNAMIQUE."""
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift(1))
    low_close = abs(df['low'] - df['close'].shift(1))
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=window).mean()
    return atr

# Modifications des fonctions pour intégrer l'ATR
def open_long(row):
    return (
        row['n1_close'] < row['n1_higher_band'] 
        and row['close'] > row['higher_band'] 
        and row['close'] > row['long_ma']
    )

def close_long(row, entry_price, atr, multiplier=2):
    """FERMETURE DYNAMIQUE LONG AVEC ATR."""
    trailing_stop = entry_price - (atr * multiplier)
    return row['close'] < trailing_stop

def open_short(row):
    return (
        row['n1_close'] > row['n1_lower_band'] 
        and row['close'] < row['lower_band'] 
        and row['close'] < row['long_ma']
    )

def close_short(row, entry_price, atr, multiplier=2):
    """FERMETURE DYNAMIQUE SHORT AVEC ATR."""
    trailing_stop = entry_price + (atr * multiplier)
    return row['close'] > trailing_stop

print(f"--- Bollinger Trend on {len(params_coin)} tokens {timeframe} Leverage x{leverage} ---")

bitget = PerpBitget(
    apiKey=account["apiKey"],
    secret=account["secret"],
    password=account["password"],
)

# Chargement des données
df_list = {}
for pair in params_coin:
    temp_data = bitget.get_more_last_historical(pair, timeframe, 1000)
    if len(temp_data) == 1000:
        df_list[pair] = temp_data
    else:
        print(f"Pair {pair} not loaded, length: {len(temp_data)}")
print("Data OHLCV loaded 100%")

# Calcul des indicateurs
for pair in df_list:
    df = df_list[pair]
    params = params_coin[pair]
    bol_band = ta.volatility.BollingerBands(close=df["close"], window=params["bb_window"], window_dev=params["bb_std"])
    df["lower_band"] = bol_band.bollinger_lband()
    df["higher_band"] = bol_band.bollinger_hband()
    df["ma_band"] = bol_band.bollinger_mavg()

    df['long_ma'] = ta.trend.SMAIndicator(close=df['close'], window=params["long_ma_window"]).sma_indicator()
    
    # AJOUT DE L'ATR AUX INDICATEURS
    df['atr'] = calculate_atr(df)

    df["n1_close"] = df["close"].shift(1)
    df["n1_lower_band"] = df["lower_band"].shift(1)
    df["n1_higher_band"] = df["higher_band"].shift(1)

    df['iloc'] = range(len(df))

print("Indicators loaded 100%")

# Calcul de la Value at Risk
var = ValueAtRisk(df_list=df_list.copy())
var.update_cov(current_date=df_list["BTC/USDT:USDT"].index[-1], occurance_data=989)
print("Value At Risk loaded 100%")

# Récupération du solde en USD
usd_balance = float(bitget.get_usdt_equity())
print("USD balance :", round(usd_balance, 2), "$")

# Récupération des positions ouvertes
positions_data = bitget.get_open_position()
position_list = []

for d in positions_data:
    if d["symbol"] in df_list:
        position_info = {
            "pair": d["symbol"],
            "side": d["side"],
            "size": float(d["contracts"]) * float(d["contractSize"]),
            "market_price": float(d["info"]["marketPrice"]),
            "usd_size": float(d["contracts"]) * float(d["contractSize"]) * float(d["info"]["marketPrice"]),
            "open_price": float(d["entryPrice"])
        }
        position_list.append(position_info)

positions = {}
for pos in position_list:
    positions[pos["pair"]] = {
        "side": pos["side"],
        "size": pos["size"],
        "market_price": pos["market_price"],
        "usd_size": pos["usd_size"],
        "open_price": pos["open_price"]
    }

print(f"{len(positions)} active positions ({list(positions.keys())})")

# Vérification pour fermer les positions
positions_to_delete = []
for pair in positions:
    row = df_list[pair].iloc[-2]
    atr = df_list[pair].iloc[-1]['atr']  # UTILISATION DE L'ATR
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

# Ouverture de nouvelles positions
for pair in df_list:
    if pair not in positions:
        row = df_list[pair].iloc[-2]
        last_price = float(df_list[pair].iloc[-1]["close"])
        pct_sizing = params_coin[pair]["wallet_exposure"]
        atr = df_list[pair].iloc[-1]['atr']  # UTILISATION DE L'ATR

        if open_long(row) and "long" in types:
            print(f"Opening long position on {pair}")
            # Logique d'ouverture de position longue

        elif open_short(row) and "short" in types:
            print(f"Opening short position on {pair}")
            # Logique d'ouverture de position courte

now = datetime.now()
current_time = now.strftime("%d/%m/%Y %H:%M:%S")
print("--- End Execution Time :", current_time, "---")
