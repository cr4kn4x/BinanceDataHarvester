import coloredlogs
from typing import List, Optional
from lib.DAO import DAO
from lib.ArgparserValidation import parse_args
from pprint import pprint
from lib.SpotKlines import fetch_and_store_klines





def main(argv: Optional[List[str]] = None):
    # parse args
    args = parse_args(argv)
    env_config = args.env_config

    coloredlogs.install(level=args.log_level)

    pprint("running with following configuration:")
    pprint(args.model_dump(exclude=["env_config.MONGO_URI"]))


    dao = DAO(uri = env_config.MONGO_URI, 
              SPOT_KLINE_DB = env_config.binance_spot_klines_db, 
              UM_KLINE_DB = env_config.binance_um_klines_db, 
              CM_KLINE_DB = env_config.binance_cm_klines_db)

    

    for symbol in args.symbols:
        fetch_and_store_klines(symbol = symbol, 
                               interval = args.interval, 
                               market = args.market,
                               base_dir = env_config.BASE_DIR, 
                               dao = dao,
                               use_zip_cache = args.use_zip_cache)

    return
    

   

if __name__ == "__main__":
    main()