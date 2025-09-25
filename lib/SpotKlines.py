import logging
from datetime import datetime, UTC
from lib.utils import establish_directory, prepare_daily_urls, download_zip, parse_klines, csv_to_list
from lib.DAO import DAO
from concurrent.futures import ThreadPoolExecutor



def gather_spot_klines(symbol: str,
                            dao: DAO, database_name: str, 
                            base_dir: str, use_zip_cache: bool,
                            max_download_workers: int,
                            ): 
    
    MONGO_DATABASE = database_name
    MONGO_COLLECTION = symbol
    BASE_DIR = f"{base_dir}/spot/daily/klines/{symbol}"

    establish_directory(path=BASE_DIR)

    cursor, is_initial_binance_cursor = dao.get_latest_open_time_cursor(MONGO_DATABASE, MONGO_COLLECTION, symbol=symbol)
    now = datetime.now(tz=UTC)

    # prepare url list
    urls = prepare_daily_urls(now=now, cursor=cursor, symbol=symbol)
    logging.info(f"Prepared {len(urls)} daily URLs for symbol {symbol}")


    # download zips in parallel (max_download_workers)
    with ThreadPoolExecutor(max_workers=max_download_workers) as executor:
        unzipped_dirs = list(executor.map(lambda url: download_zip(url=url, base_dir=BASE_DIR, use_zip_cache=use_zip_cache), urls))

    # parse klines and insert in collection
    for i, unzipped_dir in enumerate(unzipped_dirs):
        logging.info(f"parsing and inserting klines {unzipped_dir} ...")
        raw_klines = csv_to_list(unzipped_dir=unzipped_dir)
        parsed_klines = parse_klines(raw_data=raw_klines)

        dao.insert_klines(database_name=MONGO_DATABASE, collection_name=MONGO_COLLECTION, klines=parsed_klines, allow_overwrite=bool(i==0))
        logging.info(f"successfully inserted {len(raw_klines)}")