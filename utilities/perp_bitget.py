import ccxt
import pandas as pd
import time
from multiprocessing.pool import ThreadPool as Pool
import numpy as np

class PerpBitget():
    def __init__(self, apiKey=None, secret=None, password=None):
        if apiKey is None or secret is None or password is None:
            self._auth = False
            self._session = ccxt.bitget()
        else:
            self._auth = True
            self._session = ccxt.bitget({
                "apiKey": apiKey,
                "secret": secret,
                "password": password,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'swap',  # Spécifie que nous travaillons avec des contrats perpétuels
                },
                'verbose': False,  # Passez à True pour activer les logs détaillés
            })
        self.market = self._session.load_markets()

    def authentication_required(fn):
        """Annotation for methods that require auth."""
        def wrapped(self, *args, **kwargs):
            if not self._auth:
                raise Exception("You must be authenticated to use this method")
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def get_last_historical(self, symbol, timeframe, limit):
        result = pd.DataFrame(data=self._session.fetch_ohlcv(
            symbol, timeframe, since=None, limit=limit))
        result = result.rename(
            columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})
        result = result.set_index('timestamp')
        result.index = pd.to_datetime(result.index, unit='ms')
        return result

    def get_more_last_historical(self, symbol, timeframe, limit):
        batch_size = 100
        timeframe_in_seconds = self._session.parse_timeframe(timeframe)
        total_iterations = int((limit + batch_size - 1) / batch_size)

        all_data = []
        for i in range(total_iterations):
            since = round(time.time() * 1000) - ((i + 1) * batch_size * timeframe_in_seconds * 1000)
            try:
                print(f"Fetching data for {symbol} since {since}")
                data = self._session.fetch_ohlcv(symbol, timeframe, since=since, limit=batch_size)
                print(f"Fetched {len(data)} candles for {symbol}")
                all_data.extend(data)
            except Exception as err:
                print(f"Error fetching data for {symbol}: {type(err).__name__} - {err}")
                time.sleep(1)

        # Supprimer les doublons éventuels
        all_data = [list(t) for t in set(tuple(element) for element in all_data)]

        # Trier les bougies par timestamp
        all_data.sort(key=lambda x: x[0])

        # Convertir en DataFrame
        result = pd.DataFrame(data=all_data, columns=[
                              'timestamp', 'open', 'high', 'low', 'close', 'volume'])
        result['timestamp'] = pd.to_datetime(result['timestamp'], unit='ms')
        result = result.set_index('timestamp')
        result = result.sort_index()

        return result

    def get_bid_ask_price(self, symbol):
        try:
            ticker = self._session.fetch_ticker(symbol)
        except Exception as err:
            raise Exception(err)
        return {"bid": ticker["bid"], "ask": ticker["ask"]}

    def get_min_order_amount(self, symbol):
        return self._session.markets_by_id[symbol]["info"]["minProvideSize"]

    def convert_amount_to_precision(self, symbol, amount):
        return self._session.amount_to_precision(symbol, amount)

    def convert_price_to_precision(self, symbol, price):
        return self._session.price_to_precision(symbol, price)

    @authentication_required
    def place_limit_order(self, symbol, side, amount, price, reduce=False):
        try:
            return self._session.create_order(
                symbol,
                'limit',
                side,
                self.convert_amount_to_precision(symbol, amount),
                self.convert_price_to_precision(symbol, price),
                params={"reduceOnly": reduce}
            )
        except Exception as err:
            raise Exception(err)

    @authentication_required
    def place_limit_stop_loss(self, symbol, side, amount, trigger_price, price, reduce=False):
        try:
            return self._session.create_order(
                symbol,
                'limit',
                side,
                self.convert_amount_to_precision(symbol, amount),
                self.convert_price_to_precision(symbol, price),
                params={
                    'stopPrice': self.convert_price_to_precision(symbol, trigger_price),  # votre stop price
                    "triggerType": "market_price",
                    "reduceOnly": reduce
                }
            )
        except Exception as err:
            raise Exception(err)

    @authentication_required
    def place_market_order(self, symbol, side, amount, reduce=False):
        try:
            return self._session.create_order(
                symbol,
                'market',
                side,
                self.convert_amount_to_precision(symbol, amount),
                None,
                params={"reduceOnly": reduce}
            )
        except Exception as err:
            raise Exception(err)

    @authentication_required
    def place_market_stop_loss(self, symbol, side, amount, trigger_price, reduce=False):
        try:
            return self._session.create_order(
                symbol,
                'market',
                side,
                self.convert_amount_to_precision(symbol, amount),
                None,
                params={
                    'stopPrice': self.convert_price_to_precision(symbol, trigger_price),  # votre stop price
                    "triggerType": "market_price",
                    "reduceOnly": reduce
                }
            )
        except Exception as err:
            raise Exception(err)

    @authentication_required
    def get_balance_of_one_coin(self, coin):
        try:
            all_balance = self._session.fetch_balance()
            return all_balance['total'][coin]
        except KeyError:
            return 0
        except Exception as err:
            raise Exception("An error occurred", err)

    @authentication_required
    def get_all_balance(self):
        try:
            all_balance = self._session.fetch_balance()
            return all_balance
        except Exception as err:
            raise Exception("An error occurred", err)

    @authentication_required
    def get_usdt_equity(self):
        try:
            balance_info = self._session.fetch_balance()
            usdt_equity = balance_info['info']['data'][0]['usdtEquity']
            return usdt_equity
        except Exception as err:
            raise Exception("An error occurred", err)

    @authentication_required
    def get_open_order(self, symbol, conditionnal=False):
        try:
            return self._session.fetch_open_orders(symbol, params={'stop': conditionnal})
        except Exception as err:
            raise Exception("An error occurred", err)

    @authentication_required
    def get_my_orders(self, symbol):
        try:
            return self._session.fetch_orders(symbol)
        except Exception as err:
            raise Exception("An error occurred", err)

    @authentication_required
    def get_open_position(self, symbol=None):
        try:
            params = {
                "type": "swap",      # Spécifie que nous voulons les contrats perpétuels
                "marginCoin": "USDT"  # Spécifie la monnaie de marge
            }
            if symbol is not None:
                market = self._session.market(symbol)
                params['symbol'] = market['id']
                symbols = [symbol]
            else:
                symbols = None
            positions = self._session.fetch_positions(symbols=symbols, params=params)
            true_positions = []
            for position in positions:
                if float(position['contracts']) > 0:
                    true_positions.append(position)
            return true_positions
        except Exception as err:
            raise Exception("An error occurred in get_open_position", err)

    @authentication_required
    def cancel_order_by_id(self, id, symbol, conditionnal=False):
        try:
            if conditionnal:
                return self._session.cancel_order(id, symbol, params={'stop': True, "planType": "normal_plan"})
            else:
                return self._session.cancel_order(id, symbol)
        except Exception as err:
            raise Exception("An error occurred in cancel_order_by_id", err)

    @authentication_required
    def cancel_all_open_order(self):
        try:
            return self._session.cancel_all_orders(
                params={
                    "marginCoin": "USDT",
                }
            )
        except Exception as err:
            raise Exception("An error occurred in cancel_all_open_order", err)

    @authentication_required
    def cancel_order_ids(self, ids=[], symbol=None):
        try:
            return self._session.cancel_orders(
                ids=ids,
                symbol=symbol,
                params={
                    "marginCoin": "USDT",
                }
            )
        except Exception as err:
            raise Exception("An error occurred in cancel_order_ids", err)
