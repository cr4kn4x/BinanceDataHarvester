import pymongo, typing, requests
from datetime import datetime
from lib.utility import unix_ts_to_seconds
from lib.Types import t_interval
from lib.DataStructures import OHLCV


def main(interval_seconds: int) -> typing.List[typing.List[int]]:
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/?authSource=admin")
    collection = mongo_client.get_database("binance_spot_klines").get_collection("BTCUSDT_1m")


    cursor = collection.find({}, {"_id": 0, "open_time": 1}).sort("open_time", pymongo.ASCENDING).limit(2)
    open_0, open_1 = unix_ts_to_seconds(cursor.next()["open_time"]), unix_ts_to_seconds(cursor.next()["open_time"])

    if open_1 - open_0 != interval_seconds: 
        print("double check the provided interval")
        return
    
    gaps = []

    cursor = collection.find({}, {"_id": 0, "open_time": 1}).sort("open_time", pymongo.ASCENDING)
    prev_ts = unix_ts_to_seconds(cursor.next()["open_time"])

    active_gap = False

    for ts in cursor:
        ts = unix_ts_to_seconds(ts["open_time"])

        next_ts_calculated = prev_ts + interval_seconds
        if ts != next_ts_calculated: 
            if not active_gap:
                gaps.append([next_ts_calculated]) 
                active_gap = True
            else:
                gaps[-1].append(next_ts_calculated)

            while next_ts_calculated < ts:
                next_ts_calculated += interval_seconds
                if next_ts_calculated < ts:
                    gaps[-1].append(next_ts_calculated)
            assert next_ts_calculated == ts # assert that all missing open_times are present 
        else:
            active_gap = False

        prev_ts = ts
    return gaps




def api(symbol: str, interval: t_interval, limit: int, startTime: int) -> typing.List[OHLCV]:
    url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}&startTime={startTime}"

    res = requests.get(url)
    res.raise_for_status()

    res_json = res.json()
    return OHLCV.init_from_list(raw_data=res_json)


if __name__ == "__main__":
    interval_in_seconds = 60
    max_api_batch_size = 1000
    
    gaps = main(interval_in_seconds)
    for gap in gaps:
        while len(gap) > 0:
            batch_timestamps = gap[:max_api_batch_size]
            gap = gap[max_api_batch_size:]

            add_data = api(symbol="BTCUSDT", interval="1m", limit=len(batch_timestamps), startTime=int(batch_timestamps[0]*1000 - 1))


            print("gap-start:", batch_timestamps[0],  "api-data-start: ", add_data[0].open_time)
            print("gap-end:  ", batch_timestamps[-1], "api-data-end:   ", add_data[-1].open_time)

            print("gap-start (inclusive):", datetime.fromtimestamp(batch_timestamps[0]),  "api-data-start: ", datetime.fromtimestamp(unix_ts_to_seconds(add_data[0].open_time)))
            print("gap-end (inclusive):  ", datetime.fromtimestamp(batch_timestamps[-1]), "api-data-end:   ", datetime.fromtimestamp(unix_ts_to_seconds(add_data[-1].open_time)))

            print(len(batch_timestamps), len(add_data))
            break
        break







