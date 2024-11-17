import ccxt
import pandas as pd
import time
from multiprocessing.pool import ThreadPool as Pool
import numpy as np

class PerpBitget():
    def __init__(self, publicapi=None, secretapi=None, password=None):
        bitget_auth_object = {
            "apiKey": publicapi,
            "secret": secretapi,
            "password": password,
            'options': {
                'defaultType': 'swap',
            }
        }
        if bitget_auth_object['secret'] is None:
            self._auth = False
            self._session = ccxt.bitget()
        else:
            self._auth = True
            self._session = ccxt.bitget(bitget_auth_object)
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
            symbol, timeframe, None, limit=limit))
        result = result.rename(
            columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})
        result = result.set_index(result['timestamp'])
        result.index = pd.to_datetime(result.index, unit='ms')
        del result['timestamp']
        return result

   def get_more_last_historical_async(self, symbol, timeframe, limit):
    max_threads = 4

    def worker(i):
        try:
            timeframe_in_seconds = self._session.parse_timeframe(timeframe)
            since = round(time.time() * 1000) - (i * 100 * timeframe_in_seconds * 1000)
            print(f"Fetching data for {symbol} since {since}")
            data = self._session.fetch_ohlcv(
                symbol, timeframe, since=since, limit=100)
            print(f"Fetched {len(data)} candles for {symbol}")
            return data
        except Exception as err:
            print(f"Error fetching data for {symbol}: {type(err).__name__} - {err}")
            return []

    pool = Pool(max_threads)
    iterations = list(range(0, limit, 100))

    full_result = pool.map(worker, iterations)
    full_result_flat = [candle for batch in full_result for candle in batch]

    if not full_result_flat:
        raise ValueError(f"No data retrieved for {symbol}.")

    result = pd.DataFrame(data=full_result_flat, columns=[
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
                    'stopPrice': self.convert_price_to_precision(symbol, trigger_price),
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
                self.convert_price_to_precision(symbol, trigger_price),
                params={
                    'stopPrice': self.convert_price_to_precision(symbol, trigger_price),
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
            usdt_equity = self._session.fetch_balance()["info"][0]["usdtEquity"]
            return usdt_equity
        except Exception as err:
            raise Exception("An error occurred", err)

    @authentication_required
    def get_open_order(self, symbol, conditional=False):
        try:
            return self._session.fetch_open_orders(symbol, params={'stop': conditional})
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
            positions = self._session.fetch_positions(params={
                "productType": "umcbl",
            })
            true_positions = []
            for position in positions:
                if float(position['contracts']) > 0 and (symbol is None or position['symbol'] == symbol):
                    true_positions.append(position)
            return true_positions
        except Exception as err:
            raise Exception("An error occurred in get_open_position", err)

    @authentication_required
    def cancel_order_by_id(self, id, symbol, conditional=False):
        try:
            if conditional:
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
