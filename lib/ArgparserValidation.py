import os, argparse, typing
from pydantic import BaseModel
from dotenv import dotenv_values
from lib.utility import *
from lib.Types import *


class EnvConfiguration(BaseModel):
    MONGO_URI: str

    binance_spot_klines_db: str
    binance_um_klines_db: str
    binance_cm_klines_db: str

    BASE_DIR: str


class Configuration(BaseModel): 
    env_config: EnvConfiguration

    
    symbols: typing.List[str]
    market: str
    interval: str

    log_level: t_log_level
    use_zip_cache: bool



def check_env_config(path: str):
    if not os.path.isfile(path): 
        raise argparse.ArgumentTypeError(f"environment file {path} does not exist")

    try:
        config = dotenv_values(path)
    except Exception as e:
        raise argparse.ArgumentTypeError(f"error loading the environment file {path}. error: {e}")
    
    missing = [env_key for env_key in list(EnvConfiguration.model_fields.keys()) if not config.get(env_key)]
    if missing: 
        raise argparse.ArgumentTypeError(f"error loading the environment file {path}. missing keys: {', '.join(missing)}")
    
    # final validation using pydantic
    try:
        config = EnvConfiguration(**config)
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Invalid configuration file {path}: {e}")
    
    #
    if not os.path.exists(config.BASE_DIR):
        raise argparse.ArgumentTypeError(f"The base directory {config.BASE_DIR} provided in the environment file {path} does not exists. Please create the directory {config.BASE_DIR}")

    return config


def check_symbols(symbols: typing.List[str], market: t_market) -> typing.List[str]:
    if symbols == None:
        return []

    binance_symbols = Binance.get_all_symbols(market=market)

    invalid = [symbol for symbol in symbols if symbol not in binance_symbols]
    if invalid: 
        raise argparse.ArgumentTypeError(f"invalid symbols found: {', '.join(invalid)}")
    return symbols


def check_market_compatibility(market: t_market, interval: t_interval): 
    if market == "spot": 
        if interval not in typing.get_args(t_spot_interval): 
            raise argparse.ArgumentTypeError(f"invalid interval for market type {market}. valid intervals are: {', '.join(typing.get_args(t_spot_interval))}")
    elif market == "cm":
        if interval not in typing.get_args(t_cm_interval):
            raise argparse.ArgumentTypeError(f"invalid interval for market type {market}. valid intervals are: {', '.join(typing.get_args(t_cm_interval))}")
    elif market == "um": 
        if interval not in typing.get_args(t_um_interval):
            raise argparse.ArgumentTypeError(f"invalid interval for market type {market}. valid intervals are: {', '.join(typing.get_args(t_um_interval))}")
    else: 
        raise argparse.ArgumentTypeError(f"invalid market type {market}. valid market types are {', '.join(typing.get_args(t_market))}")
    return market


def parse_args(argv = None) -> Configuration:
    parser = argparse.ArgumentParser(
        description="Binance Kline Data Harvester by cr4k4nx",
        usage="python main.py -h"
    )


    parser.add_argument(
        "--market",
        required=True,
        dest="market",
        choices=["um", "cm", "spot", "futures"],
        help="binance markets: [um, cm, spot]  kucoin markets: [spot, futures]"
    )


    parser.add_argument(
        "--symbols",
        required=True,
        dest="symbols",
        metavar="BTCUSDT",
        nargs="+",
    )


    parser.add_argument(
        "--interval",
        required=True,
        dest="interval",
        choices=set(typing.get_args(t_spot_interval) + typing.get_args(t_um_interval) + typing.get_args(t_cm_interval)),
    )


    parser.add_argument(
        "--env-file",
        dest="env_file",
        default="./.env",
        metavar="./.env",
        help=f"Path to environment file containing your desired database and storage configuration. required keys: {', '.join(list(EnvConfiguration.model_fields.keys()))}"
    )


    parser.add_argument(
        "--log-level", 
        dest="log_level",
        default="info",
        choices=list(typing.get_args(t_log_level)),
        help="*OPTIONAL* set a log level. `--log-level debug` provides verbose logging mainly suitable for debug purposes only.  `--log-level info` provides information about the current progress and actions. `--log-level warning` provides only messages that are related to occuring warnings and errors" 
    )


    parser.add_argument(
        "--use-zip-cache", 
        dest="use_zip_cache",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="use already downloaded .zip files instead of downloading them again"
    )

    # parse the args... 
    args = parser.parse_args(argv)

    # validate the args...
    return Configuration(
        env_config=check_env_config(args.env_file),
        market=check_market_compatibility(args.market, args.interval), 
        interval=args.interval,
        symbols = check_symbols(args.symbols, args.market),
        log_level = args.log_level,
        use_zip_cache = args.use_zip_cache,
    )