import logging, coloredlogs, os, argparse
from typing import List, Optional
from lib.DAO import DAO
from lib.utils import check_symbols
from lib.SpotKlines import gather_spot_klines
from dotenv import load_dotenv



def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Binance Spot Kline Harvester by cr4k4nx"
    )

    parser.add_argument(
        "--symbols",
        required=True,
        dest="symbols",
        nargs="+",
        help="provide all symbols to fetch separated by single whitespace. example usage: `--symbols BTCUSDT ETHUSDT SOLUSDT`"
    )

    parser.add_argument(
        "--max-download-workers",
        dest="max_download_workers",
        type=int,
        default=5,
        metavar="5",
        help="*OPTIONAL* number of parallel workers for downloading daily zip files. Adjust carefully to avoid rate limit ban."
    )
   
    parser.add_argument(
        "--env-file",
        dest="env_file",
        default="./.env",
        metavar="./.env",
        help="*OPTIONAL* path to environment file (.env) that contains `MONGO_URI='{your_mongo_db_connection_string}'`. example usage: `--env-file ./env`", 
    )

    parser.add_argument(
        "--mongo-db-name",
        dest="mongo_db_name",
        default="binance_spot_klines",
        metavar="binance_spot_klines",
        help="*OPTIONAL* desired name of the mongo database to store kline data. example usage: `--mongo-db-name binance_spot_klines`" 
    )

    parser.add_argument(
        "--base-dir",
        dest="base_dir",
        required=False,
        metavar="./crypto/binance",
        default="./crypto/binance",
        help="*OPTIONAL* base path to directory to store and unzip the downloaded files. The program will create a subdirectory according to the format: {base-dir}/spot/daily/klines  example usage: `--base-dir E:/crypto/binance`"
    )

    parser.add_argument(
        "--log-level", 
        dest="log_level",
        default="info",
        choices=["debug", "info", "warning"],
        help="*OPTIONAL* set a log level. `--log-level debug` provides verbose logging mainly suitable for debug purposes only.  `--log-level info` provides information about the current progress and actions. `--log-level warning` provides only messages that are related to occuring warnings and errors" 
    )

    parser.add_argument(
        "--use-zip-cache", 
        dest="use_zip_cache",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="consider already downloaded .zip files as cached (usage: `--use-zip-cache`)"
    )

    return parser.parse_args(argv)



def main(argv: Optional[List[str]] = None):
    # parse args using argpase
    args = parse_args(argv)

    symbols = list(set(args.symbols)) 
    env_file = args.env_file
    mongo_db_name = args.mongo_db_name
    base_dir = args.base_dir
    use_zip_cache = args.use_zip_cache
    log_level = args.log_level
    max_download_workers = args.max_download_workers
    
    load_dotenv(env_file)
    coloredlogs.install(level=log_level)

    logging.info(
        ("\n"
        "<configuration>" "\n"
        f"total_symbols = {len(symbols)}" "\n"
        f"symbols = {symbols}" "\n"
        f"base_dir = {base_dir}" "\n"
        f".env file = {env_file}" "\n"
        f"use_zip_cache = {use_zip_cache}" "\n"
        f"log_level = {log_level}" "\n"
        f"max_download_workers = {max_download_workers}" "\n"
        "</configuration>"
    ))

    # validate user provided symbols
    try:
        check_symbols(symbols=symbols)
    except Exception as e: 
        return

    # initialize dao 
    dao = DAO(os.environ.get("MONGO_URI"))

    # schedule 
    for symbol in symbols:
        gather_spot_klines(symbol=symbol, 
                              dao=dao, database_name=mongo_db_name,
                              base_dir=base_dir, use_zip_cache=use_zip_cache,
                              max_download_workers=max_download_workers
                            )
    return



if __name__ == "__main__":
    main()