import typing, logging, requests, zipfile, os, csv
from lib.DatetimeCursor import Cursor
from lib.DataStructures import OHLCV
from datetime import UTC, datetime, timedelta


def establish_directory(path: str):
    os.makedirs(path , exist_ok=True)
    

def ts_to_seconds(ts):
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
    return Cursor.fromtimestamp(ts_to_seconds(ts), tz=UTC)

def timestamp_to_datetime(ts):
    return datetime.fromtimestamp(ts_to_seconds(ts), tz=UTC)


def get_all_spot_symbols() -> typing.List[str]:
    res = requests.get("https://api.binance.com/api/v3/exchangeInfo")
    res.raise_for_status()

    symbols = [symbol['symbol'] for symbol in res.json()['symbols']]
    return symbols


def get_binance_spot_start_cursor(symbol: str) -> Cursor:
    res = requests.get(f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval=1s&limit={1}&startTime={0}")
    res.raise_for_status()

    start_date = timestamp_to_cursor(res.json()[0][0])
    return start_date


def is_valid_zip(path: str):
    with zipfile.ZipFile(path) as zip: 
        bad_zip = zip.testzip()

    if bad_zip: 
        logging.error(f"bad zip detected {path}")
        return False

    return True


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
    
    # extract zip
    with zipfile.ZipFile(file=full_filepath_zip) as zip:
        zip.extractall(path=full_path_unzipped_dir)

    return full_path_unzipped_dir


def csv_to_list(unzipped_dir: str):
    logging.info("Converting CSV to list")
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


def parse_klines(raw_data: typing.List[typing.List]):
    logging.info(f"Parsing {len(raw_data)} klines...")
    return [OHLCV.init(r) for r in raw_data]


def prepare_daily_urls(now: datetime, cursor: Cursor, symbol: str):
    urls = []

    while (cursor.year < now.year) or (cursor.year == now.year and cursor.month < now.month) or (cursor.year == now.year and cursor.month == now.month and cursor.day < now.day):
        urls.append(f"https://data.binance.vision/data/spot/daily/klines/{symbol}/1s/{symbol}-1s-{cursor.year}-{cursor.get_month()}-{cursor.get_day()}.zip")

        cursor += timedelta(days=1)
    return urls



def check_symbols(symbols: typing.List[str]):
    # check symbols 
    invalid_symbols = []

    binance_symbols = get_all_spot_symbols()
    for symbol in symbols: 
        if symbol not in binance_symbols: 
            invalid_symbols.append(symbol)

    if len(invalid_symbols):
        logging.error(f"You provided {len(invalid_symbols)} symbols that are not available on binance spot market: {invalid_symbols}")

        raise ValueError("Invalid Symbols provided")