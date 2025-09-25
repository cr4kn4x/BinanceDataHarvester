import typing, logging
from pydantic import BaseModel


class OHLCV(BaseModel):
    open_time: int 
    open: float
    high: float 
    low: float 
    close: float 
    volume: float 
    close_time: int 
    quote_asset_volume: float 
    number_of_trades: int 
    taker_buy_base_asset_volume: float 
    taker_buy_quote_asset_volume: float 
    ignore: typing.Any

    @staticmethod
    def init(raw_data: typing.List): 
        logging.debug(f"Initializing OHLCV with raw_data: {raw_data}")
        return OHLCV(
            open_time = raw_data[0],
            open = raw_data[1],
            high = raw_data[2],
            low = raw_data[3], 
            close = raw_data[4], 
            volume = raw_data[5], 
            close_time = raw_data[6], 
            quote_asset_volume = raw_data[7], 
            number_of_trades = raw_data[8], 
            taker_buy_base_asset_volume = raw_data[9],
            taker_buy_quote_asset_volume = raw_data[10], 
            ignore = raw_data[11], 
        )