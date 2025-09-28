import typing, requests
from lib.Types import BINANCE_MARKET
from lib.utils.time import Cursor, timestamp_to_cursor
from lib.utils.exceptions import generate_invalid_arg_exception

class Binance: 
    @staticmethod
    def get_all_symbols(market: BINANCE_MARKET) -> typing.List[str]:
        if market == "um":
            res = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        elif market == "cm":
            res = requests.get("https://dapi.binance.com/dapi/v1/exchangeInfo")
        elif market == "spot":
            res = requests.get("https://api.binance.com/api/v3/exchangeInfo")
        else: raise generate_invalid_arg_exception("market", market)

        res.raise_for_status()
        return [symbol['symbol'] for symbol in res.json()['symbols']]
    

    @staticmethod
    def get_start_cursor(symbol: str, market: BINANCE_MARKET) -> Cursor: 
        if market == "um":
            res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=1&startTime=0")
            res.raise_for_status()
            return timestamp_to_cursor(res.json()[0][0])

        elif market == "cm":
            exchange_info = requests.get("https://dapi.binance.com/dapi/v1/exchangeInfo")
            exchange_info.raise_for_status()

            onboard_date = None
            for s in exchange_info.json()["symbols"]: 
                if s["symbol"] == symbol: 
                    onboard_date = s["onboardDate"]
                    break
            
            if onboard_date == None:
                raise ValueError(f"symbol {symbol} (cm) not found in exchange info endpoint")
            return timestamp_to_cursor(onboard_date)
            
        elif market == "spot":
            res = requests.get(f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval=1m&limit=1&startTime=0")
            res.raise_for_status()
            return timestamp_to_cursor(res.json()[0][0])
        
        else: raise generate_invalid_arg_exception("market", market)
