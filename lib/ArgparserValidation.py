import os, argparse, typing
from pydantic import BaseModel
from dotenv import dotenv_values
from lib.utils import get_all_symbols
from lib.Types import BINANCE_MARKET, LOG_LEVEL, SPOT_INTERVALL, FUTURES_CM_INTERVALL, FUTURES_UM_INTERVALL


class EnvConfiguration(BaseModel):
    MONGO_URI: str
    SPOT_KLINE_DB: str
    UM_KLINE_DB: str
    CM_KLINE_DB: str
    BASE_DIR: str


class Configuration(BaseModel): 
    env_config: EnvConfiguration

    symbols: typing.List[str]
    market: BINANCE_MARKET
    interval: typing.Union[SPOT_INTERVALL, FUTURES_CM_INTERVALL, FUTURES_UM_INTERVALL]

    log_level: LOG_LEVEL
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

    return config


def check_symbols(symbols: typing.List[str], market: BINANCE_MARKET) -> typing.List[str]:
    if symbols == None:
        return []

    binance_symbols = get_all_symbols(market=market)

    invalid = [symbol for symbol in symbols if symbol not in binance_symbols]
    if invalid: 
        raise argparse.ArgumentTypeError(f"the following {market}-symbols cannot be found on binance: {', '.join(invalid)}")

    return symbols


def check_market_compatibility(market: BINANCE_MARKET, interval: typing.Union[SPOT_INTERVALL, FUTURES_CM_INTERVALL, FUTURES_UM_INTERVALL]): 
    if market == "spot": 
        if interval not in typing.get_args(SPOT_INTERVALL): 
            raise argparse.ArgumentTypeError(f"Invalid interval for market type {market}. valid intervals are: {', '.join(typing.get_args(SPOT_INTERVALL))}")
    elif market == "cm":
        if interval not in typing.get_args(FUTURES_CM_INTERVALL):
            raise argparse.ArgumentTypeError(f"Invalid interval for market type {market}. valid intervals are: {', '.join(typing.get_args(FUTURES_CM_INTERVALL))}")
    elif market == "um": 
        if interval not in typing.get_args(FUTURES_UM_INTERVALL):
            raise argparse.ArgumentTypeError(f"Invalid interval for market type {market}. valid intervals are: {', '.join(typing.get_args(FUTURES_UM_INTERVALL))}")
    else:
        raise argparse.ArgumentTypeError(f"invalid market type {market}. valid market types are {', '.join(typing.get_args(BINANCE_MARKET))}")

    return market


def parse_args(argv = None) -> Configuration:
    parser = argparse.ArgumentParser(
        description="Binance Data Harvester by cr4k4nx",
        usage="python main.py --spot-symbols BTCUSDT, ETHUSDT --env-file ./.env --log-level info --use-zip-cache"
    )


    parser.add_argument(
        "--symbols",
        required=True,
        dest="symbols",
        metavar="BTCUSDT",
        nargs="+",
    )


    parser.add_argument(
        "--market",
        required=True,
        dest="market",
        choices=[o for o in typing.get_args(BINANCE_MARKET)]
    )


    parser.add_argument(
        "--interval",
        required=True,
        dest="interval",
        choices=list(set(typing.get_args(SPOT_INTERVALL) + typing.get_args(FUTURES_UM_INTERVALL) + typing.get_args(FUTURES_CM_INTERVALL))),
        help=f"the following intervals can be fetched depending on the selected market. || *spot*: {', '.join(typing.get_args(SPOT_INTERVALL))} || *um*: {', '.join(typing.get_args(FUTURES_UM_INTERVALL))} || *cm*: {', '.join(typing.get_args(FUTURES_CM_INTERVALL))}"
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
        choices=[list(typing.get_args(LOG_LEVEL))],
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