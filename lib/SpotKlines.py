import logging, os
from datetime import datetime, UTC, timedelta
from lib.DAO import DAO

from lib.DataStructures import OHLCV
from lib.utility import *
from lib.Types import *


def build_url(symbol: str, interval: t_interval, market: t_market, cursor: Cursor):
    if market == "cm" or market == "um":
        market = f"futures/{market}"
    
    return f"https://data.binance.vision/data/{market}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{cursor.year}-{cursor.get_month()}-{cursor.get_day()}.zip"
    



def fetch_and_store_klines(symbol: str, interval: t_interval, market: t_market, 
                           base_dir: str,
                           dao: DAO,
                           use_zip_cache: bool):
    
    
    # 
    daily_kline_dir = os.path.join(base_dir, market, "daily", "klines", symbol, interval)
    ensure_dir(daily_kline_dir)

    now = datetime.now(tz=UTC)    
    cursor, is_initial_binance_cursor = dao.get_kline_cursor(symbol=symbol, market=market, interval=interval)

    
    total_days = (now - cursor).days
    i = 0
    while   (cursor.year < now.year) or \
            (cursor.year == now.year and cursor.month < now.month) or \
            (cursor.year == now.year and cursor.month == now.month and cursor.day < now.day):
        
        logging.info(f"fetching klines... iteration: {i}")
        logging.info(f"symbol: {symbol} interval: {interval} market: {market} daily_kline_dir: {daily_kline_dir} zip cache: {use_zip_cache}")
        logging.info(f"now: {now} cursor: {cursor} total days left: {(now-cursor).days}")

        # build url  
        url = build_url(symbol=symbol, interval=interval, market=market, cursor=cursor)

        # try to download
        try:
            unzipped_dir = download_zip(url=url, base_dir=daily_kline_dir, use_zip_cache=use_zip_cache)
            is_initial_binance_cursor = False
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                if is_initial_binance_cursor:
                    logging.warning(f"start lag detected. url not found. url: {url} symbol: {symbol} market: {market} interval: {interval}")
                else: 
                    logging.error(f"url not found. url: {url} symbol: {symbol} market: {market} interval: {interval}")
                continue
            else: 
                raise e
        finally:
            logging.info(f"incrementing cursor by 1 day...")
            cursor += timedelta(days=1)
            i += 1
            logging.info(f"new cursor: {cursor}")


        # parse the data 
        raw_klines = csv_to_list(unzipped_dir=unzipped_dir)
        parsed_klines = OHLCV.init_from_list(raw_data=raw_klines)

        dao.insert_klines_error_resistant(market=market, symbol=symbol, interval=interval, klines=parsed_klines)
        logging.info(f"{len(parsed_klines)} klines written in database..")
        logging.info(f"progress: {i}/{total_days}")
