import logging, os, requests, csv, zipfile
from datetime import datetime, UTC
from lib.Types import *


class Cursor(datetime):
    def get_month(self):
        month = str(self.month)
        if len(month) == 1:
            month = f"0{month}"
        return month
    
    def get_day(self): 
        day = str(self.day)
        if len(day) == 1: 
            day = f"0{day}"
        return day
    

def unix_ts_to_seconds(ts):
    ts = int(float(ts))

    if ts > 1e18:   # ns
        return ts / 1e9
    elif ts > 1e15: # us
        return ts / 1e6
    elif ts > 1e12: # ms
        return ts / 1e3
    else:           # s
        return ts



def timestamp_to_cursor(ts):
    return Cursor.fromtimestamp(unix_ts_to_seconds(ts), tz=UTC)


def timestamp_to_datetime(ts):
    return datetime.fromtimestamp(unix_ts_to_seconds(ts), tz=UTC)


def generate_invalid_arg_exception(arg: str, value): 
    return RuntimeError(f"unexpected value {value} provided for arg {arg}")

def is_valid_zip(path: str):
    with zipfile.ZipFile(path) as zip: 
        bad_zip = zip.testzip()

    if bad_zip: 
        logging.error(f"bad zip detected {path}")
        return False
    return True


def ensure_dir(path: str): 
    return os.makedirs(path, exist_ok=True)


def is_dir_empty(path: str): 
    return len(os.listdir(path=path)) == 0


def download_zip(url: str, base_dir: str, use_zip_cache: bool):
        def download(url: str, file_path: str): 
            logging.info(f"downloading {url} to destionation path {file_path}...")
            with requests.get(url) as req: 
                req.raise_for_status()
                with open(file_path, "wb") as fp:
                    for chunk in req.iter_content(chunk_size=8192): 
                        fp.write(chunk)

        full_filepath_zip = f"{base_dir}/{url.split('/')[-1]}"
        full_path_unzipped_dir = full_filepath_zip[:-4]

        if use_zip_cache and os.path.exists(path=full_filepath_zip) and is_valid_zip(path=full_filepath_zip):
            logging.info(f"using cached zip: {full_filepath_zip}")
        else:
            download(url=url, file_path=full_filepath_zip)
        
        logging.info(f"extracting zip to destination path {full_path_unzipped_dir}")
        with zipfile.ZipFile(file=full_filepath_zip) as zip:
            zip.extractall(path=full_path_unzipped_dir)

        return full_path_unzipped_dir


def csv_to_list(unzipped_dir: str):
    logging.info("Converting CSV to list...")
    rows = []

    dir_content = os.listdir(unzipped_dir)
    logging.debug(f"Directory content: {dir_content}")

    if len(dir_content) != 1 and not dir_content[0].endswith(".csv"):
        raise Exception(f"Unexpected directory structure: {unzipped_dir}")

    csv_path = f"{unzipped_dir}/{dir_content[0]}"
    with open(csv_path, mode="r") as fp:
        csv_reader = csv.reader(fp, delimiter=",")
        rows = [row for row in csv_reader]
    
    return rows




class Binance: 
    @staticmethod
    def get_all_symbols(market: t_market) -> typing.List[str]:
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
    def get_start_cursor(symbol: str, market: t_market) -> Cursor: 
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
