import ccxt
import pandas as pd
import time

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
                    'defaultType': 'swap',  # Assure que nous utilisons les marchés perpétuels
                },
                'verbose': False,
            })
        self.market = self._session.load_markets()

    def authentication_required(fn):
        def wrapped(self, *args, **kwargs):
            if not self._auth:
                raise Exception("Vous devez être authentifié pour utiliser cette méthode")
            else:
                return fn(self, *args, **kwargs)
        return wrapped

    def get_hold_side(self, side, reduce=False):
        if side.lower() == 'buy':
            return 'close_short' if reduce else 'long'
        elif side.lower() == 'sell':
            return 'close_long' if reduce else 'short'
        else:
            raise ValueError(f"Invalid side: {side}")

    def get_last_historical(self, symbol, timeframe, limit):
        data = self._session.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return df

    def get_more_last_historical(self, symbol, timeframe, limit):
        batch_size = 100
        timeframe_in_seconds = self._session.parse_timeframe(timeframe)
        total_iterations = int((limit + batch_size - 1) / batch_size)
        all_data = []
        for i in range(total_iterations):
            since = int(time.time() * 1000) - ((i + 1) * batch_size * timeframe_in_seconds * 1000)
            try:
                data = self._session.fetch_ohlcv(symbol, timeframe, since=since, limit=batch_size)
                all_data.extend(data)
            except Exception as err:
                print(f"Erreur lors de la récupération des données pour {symbol}: {type(err).__name__} - {err}")
                time.sleep(1)
        # Supprimer les doublons et trier
        all_data = list({tuple(row) for row in all_data})
        all_data.sort()
        df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        return df

    def get_bid_ask_price(self, symbol):
        try:
            ticker = self._session.fetch_ticker(symbol)
            return {"bid": ticker["bid"], "ask": ticker["ask"]}
        except Exception as err:
            raise Exception(err)

    def get_min_order_amount(self, symbol):
        market = self._session.market(symbol)
        return market["limits"]["amount"]["min"]

    def convert_amount_to_precision(self, symbol, amount):
        return self._session.amount_to_precision(symbol, amount)

    def convert_price_to_precision(self, symbol, price):
        return self._session.price_to_precision(symbol, price)

    @authentication_required
    def place_limit_order(self, symbol, side, amount, price, reduce=False):
        try:
            params = {
                "reduceOnly": reduce,
                "holdSide": self.get_hold_side(side, reduce),
            }
            order = self._session.create_order(
                symbol,
                'limit',
                side,
                amount,
                price,
                params=params
            )
            return order
        except Exception as err:
            raise Exception(err)

    @authentication_required
    def place_limit_stop_loss(self, symbol, side, amount, trigger_price, price, reduce=False):
        try:
            params = {
                'stopPrice': self.convert_price_to_precision(symbol, trigger_price),
                "triggerType": "market_price",
                "reduceOnly": reduce,
                'stop': True,
                "holdSide": self.get_hold_side(side, reduce),
            }
            order = self._session.create_order(
                symbol,
                'limit',
                side,
                amount,
                price,
                params=params
            )
            return order
        except Exception as err:
            raise Exception(err)

    @authentication_required
    def place_market_order(self, symbol, side, amount, reduce=False):
        try:
            params = {
                "reduceOnly": reduce,
                "holdSide": self.get_hold_side(side, reduce),
            }
            order = self._session.create_order(
                symbol,
                'market',
                side,
                amount,
                None,
                params=params
            )
            return order
        except Exception as err:
            raise Exception(err)

    @authentication_required
    def place_market_stop_loss(self, symbol, side, amount, trigger_price, reduce=False):
        try:
            params = {
                'stopPrice': self.convert_price_to_precision(symbol, trigger_price),
                "triggerType": "market_price",
                "reduceOnly": reduce,
                'stop': True,
                "holdSide": self.get_hold_side(side, reduce),
            }
            order = self._session.create_order(
                symbol,
                'market',
                side,
                amount,
                None,
                params=params
            )
            return order
        except Exception as err:
            raise Exception(err)

    @authentication_required
    def get_balance_of_one_coin(self, coin):
        try:
            balance_info = self._session.fetch_balance()
            return balance_info['total'].get(coin, 0.0)
        except Exception as err:
            raise Exception("Une erreur s'est produite", err)

    @authentication_required
    def get_all_balance(self):
        try:
            balance_info = self._session.fetch_balance()
            return balance_info
        except Exception as err:
            raise Exception("Une erreur s'est produite", err)

    @authentication_required
    def get_usdt_equity(self):
        try:
            balance_info = self._session.fetch_balance()
            usdt_equity = balance_info['total'].get('USDT', 0.0)
            return usdt_equity
        except Exception as err:
            raise Exception("Une erreur s'est produite dans get_usdt_equity", err)

    @authentication_required
    def get_open_order(self, symbol, conditional=False):
        try:
            params = {'stop': conditional}
            orders = self._session.fetch_open_orders(symbol, params=params)
            return orders
        except Exception as err:
            raise Exception("Une erreur s'est produite", err)

    @authentication_required
    def get_my_orders(self, symbol):
        try:
            orders = self._session.fetch_orders(symbol)
            return orders
        except Exception as err:
            raise Exception("Une erreur s'est produite", err)

    @authentication_required
    def get_open_position(self, symbol=None):
        try:
            params = {
                "type": "swap",  # Pour les contrats perpétuels
            }
            positions = self._session.fetch_positions(symbols=[symbol] if symbol else None, params=params)
            true_positions = []
            for position in positions:
                if float(position['contracts']) > 0:
                    true_positions.append(position)
            return true_positions
        except Exception as err:
            raise Exception("Une erreur s'est produite dans get_open_position", err)

    @authentication_required
    def cancel_order_by_id(self, id, symbol, conditional=False):
        try:
            params = {'stop': conditional} if conditional else {}
            result = self._session.cancel_order(id, symbol, params=params)
            return result
        except Exception as err:
            raise Exception("Une erreur s'est produite dans cancel_order_by_id", err)

    @authentication_required
    def cancel_all_open_order(self, symbol=None):
        try:
            result = self._session.cancel_all_orders(symbol=symbol)
            return result
        except Exception as err:
            raise Exception("Une erreur s'est produite dans cancel_all_open_order", err)

    @authentication_required
    def cancel_order_ids(self, ids=[], symbol=None):
        try:
            result = self._session.cancel_orders(
                ids=ids,
                symbol=symbol,
            )
            return result
        except Exception as err:
            raise Exception("Une erreur s'est produite dans cancel_order_ids", err)

