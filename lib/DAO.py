import logging, pymongo, typing
from pymongo.errors import DuplicateKeyError, BulkWriteError
from lib.DatetimeCursor import Cursor
from lib.DataStructures import OHLCV
from lib.utils import get_binance_spot_start_cursor, timestamp_to_cursor


class DAO:
    def __init__(self, uri):
        logging.info("initializing MongoDB client using the provided connection string...")
        self.client = pymongo.MongoClient(uri)
        logging.info("MongoDB client successfully established connection")


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


    def _get_start_cursor(self, database_name: str, collection_name: str) -> Cursor | None:
        collection = self._get_collection(database_name=database_name, collection_name=collection_name)

        obj = collection.find_one(sort=[("open_time", pymongo.DESCENDING)])
        
        if isinstance(obj, dict): 
            next_open_unix = obj["close_time"] + 1  # adding 1 sec / ms / us / ns .. this should make no difference
            return timestamp_to_cursor(next_open_unix) 
        
        return None


    def get_latest_open_time_cursor(self, database_name: str, collection_name: str, symbol: str) -> typing.Tuple[Cursor, bool]: 
        logging.info(f"trying to initialize cursor from database: {database_name} collection: {collection_name} for symbol {symbol}")
        cursor, is_initial_binance_cursor = self._get_start_cursor(database_name, collection_name), False

        if cursor == None:
            logging.info("no cursor found in database")
            logging.info("cursor will be initalized from binance first tick of symbol...")
            cursor, is_initial_binance_cursor = get_binance_spot_start_cursor(symbol=symbol), True
            logging.info(f"first tick on Binance found. cursor: {cursor}")

        assert isinstance(cursor, Cursor)
        return cursor, is_initial_binance_cursor


    def insert_klines_error_resistant(self, database_name: str, collection_name: str, klines: typing.List[OHLCV]):
        try: 
            return self.insert_klines(database_name=database_name, collection_name=collection_name, klines=klines)
        
        except DuplicateKeyError as e: 
            logging.warning(f"database write attempt raised DuplicateKeyError: {e}")
            logging.warning("retry to insert data using the DuplicateKeyError tolerant update and upsert logic...")
            
            return self.insert_klines(database_name=database_name, collection_name=collection_name, klines=klines, allow_overwrite=True)
        except BulkWriteError as e:
            logging.warning(f"database write attempt raised BulkWriteError: {e.details}")
            logging.warning("retry to insert data using the DuplicateKeyError tolerant update and upsert logic...")
            
            return self.insert_klines(database_name=database_name, collection_name=collection_name, klines=klines, allow_overwrite=True)
        

    def insert_klines(self, database_name: str, collection_name: str, klines: typing.List[OHLCV], allow_overwrite: bool = False): 
        collection = self._get_collection(database_name=database_name, collection_name=collection_name)
        
        logging.info(f"attempting to bulk write (allow_overwrite={allow_overwrite}) total {len(klines)} klines in database {database_name} collection {collection_name}...")        
        
        # dump klines
        klines = [k.model_dump(mode="python") for k in klines]
        
        if allow_overwrite:
            ops = [pymongo.UpdateOne({"open_time": k["open_time"]}, {"$set": k}, upsert=True) for k in klines]
            return collection.bulk_write(ops, ordered=True)

        else: 
            return collection.insert_many(klines, ordered=True)