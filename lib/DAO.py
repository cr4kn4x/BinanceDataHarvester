import logging, pymongo, typing, uuid
from pymongo.errors import DuplicateKeyError, BulkWriteError
from lib.DataStructures import OHLCV
from lib.Types import *
from lib.utility import *

class DAO:
    def __init__(self, uri, SPOT_KLINE_DB: str, UM_KLINE_DB: str, CM_KLINE_DB: str):
        logging.info("initializing MongoDB client using the provided connection string...")
        self.client = pymongo.MongoClient(uri)

        self.spot_kline_db = SPOT_KLINE_DB
        self.um_kline_db = UM_KLINE_DB
        self.cm_kline_db = CM_KLINE_DB
        logging.info("MongoDB client successfully established connection")

    def _generate_kline_collection_name(self, market: t_market, symbol: str, interval: t_interval):
        if market == "spot":
            db = self.spot_kline_db
        elif market == "cm":
            db = self.cm_kline_db
        elif market == "um":
            db = self.um_kline_db
        else: raise generate_invalid_arg_exception("market", market)

        coll = f"{symbol}_{interval}"
        return db, coll
    

    def _get_collection(self, database_name: str, collection_name: str):
        logging.info(f"accessing collection {collection_name} in database {database_name}...")
        db = self.client.get_database(database_name)

        if collection_name not in db.list_collection_names():
            logging.info(f"collection {collection_name} does not exists in database {database_name}")
            logging.info("creating collection...")
            
            collection = db.create_collection(name=collection_name)
            collection.create_index([("open_time", pymongo.ASCENDING)], unique=True)
            
            logging.info("collection created")


        collection = db.get_collection(collection_name)
        return collection
    

    def _get_kline_collection(self, market: t_market, symbol: str, interval: t_interval): 
        db_name, coll_name = self._generate_kline_collection_name(market=market, symbol=symbol, interval=interval)
        return self._get_collection(database_name=db_name, collection_name=coll_name)




    def _get_start_cursor(self, collection: pymongo.database.Collection) -> Cursor | None:
        obj = collection.find_one(sort=[("open_time", pymongo.DESCENDING)])
        
        if isinstance(obj, dict): 
            next_open_unix = obj["close_time"] + 1  # adding 1 sec / ms / us / ns .. this should make no difference
            return timestamp_to_cursor(next_open_unix) 
        
        return None


    def get_kline_cursor(self, symbol: str, market: t_market, interval: t_interval) -> typing.Tuple[Cursor, bool]:  
        collection = self._get_kline_collection(market=market, symbol=symbol, interval=interval)
        
        logging.info("try to fetch kline cursor from database... ")
        cursor, is_initial_binance_cursor = self._get_start_cursor(collection), False

        if cursor == None:
            logging.info("no cursor found in database. try to initialize from Binance first tick of symbol...")
            cursor, is_initial_binance_cursor = Binance.get_start_cursor(symbol=symbol, market=market), True
            logging.info(f"first tick on Binance found. cursor: {cursor}")

        assert isinstance(cursor, Cursor)
        return cursor, is_initial_binance_cursor


    def insert_klines_error_resistant(self, market: t_market, symbol: str, interval: t_interval, klines: typing.List[OHLCV]):
        collection = self._get_kline_collection(market=market, symbol=symbol, interval=interval)

        klines = [k.model_dump() for k in klines]

        error = False
        try:
            return collection.insert_many(klines, ordered=True)
        except DuplicateKeyError as e:
            logging.warning(f"database write attempt raised DuplicateKeyError: {e}")
            error = True
        except BulkWriteError as e: 
            logging.warning(f"database write attempt raised BulkWriteError: {e.details}")
            error = True
        
        if error:
            logging.warning("retry to insert data using the more error tolerant update and upsert logic...")
            ops = []
            for k in klines:
                k.pop("_id", None)
                ops.append(pymongo.UpdateOne({"open_time": k["open_time"]}, {"$set": k}, upsert=True))
            return collection.bulk_write(ops, ordered=True)