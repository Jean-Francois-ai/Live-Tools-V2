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
                    'defaultType': 'swap',  # Nous travaillons avec des contrats perpétuels
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

    @authentication_required
    def place_market_order(self, symbol, side, amount, reduce=False):
        try:
            params = {
                "reduceOnly": reduce,
                "holdSide": self.get_hold_side(side, reduce),
                "positionMode": "single_side",  # Utilisez "single_side" ou "double_side" selon votre compte
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
    def place_limit_order(self, symbol, side, amount, price, reduce=False):
        try:
            params = {
                "reduceOnly": reduce,
                "holdSide": self.get_hold_side(side, reduce),
                "positionMode": "single_side",
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
                "positionMode": "single_side",
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
    def place_market_stop_loss(self, symbol, side, amount, trigger_price, reduce=False):
        try:
            params = {
                'stopPrice': self.convert_price_to_precision(symbol, trigger_price),
                "triggerType": "market_price",
                "reduceOnly": reduce,
                'stop': True,
                "holdSide": self.get_hold_side(side, reduce),
                "positionMode": "single_side",
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

    # Les autres méthodes restent inchangées
    # ...

    def convert_amount_to_precision(self, symbol, amount):
        return self._session.amount_to_precision(symbol, amount)

    def convert_price_to_precision(self, symbol, price):
        return self._session.price_to_precision(symbol, price)

