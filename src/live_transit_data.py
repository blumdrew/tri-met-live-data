"""live data from trimet"""
#TODO - very long range, convert data to SQL database
import os
from typing import Optional, Sequence, Union
import requests
import json

import pandas as pd
import uuid

# TODO: move constants to a dedicated file
DATA_PATH = os.path.join(
    os.path.dirname(__file__),
    "data"
)
os.makedirs(DATA_PATH, exist_ok=True)

class TriMetData(object):
    """For fetching TriMet live data from their API"""
    base_url_routes = "https://developer.trimet.org/ws/V1/routeConfig"
    base_url_arrivals = "https://developer.trimet.org/ws/v2/arrivals"

    def __init__(
        self,
        route_id: Optional[int] = None,
    ) -> None:
        if not route_id:
            raise ValueError("Must pass route_id for performance reasons")
        env_path = os.path.join(
            os.path.dirname(__file__),
            ".env"
        )
        if not os.path.isfile(env_path):
            # jank
            raise FileNotFoundError(f"Please save an .env file to base location to continue")
        with open(env_path, "r") as a:
            d = json.load(a)
        self.app_id = d["appID"]
        self.route_id = route_id
        self.route_data = self.get_route_data()
        self.route_color = self.route_data.get("routeColor")
        self.frequent_service = self.route_data.get("frequentService")
        self.route_number = self.route_data.get("route")
        self.type = self.route_data.get("type")
        self.stop_ids = self.route_data.get("dir")
        self.desc = self.route_data.get("desc")

    def _get_all_routes(
        self,
        cache: bool = True
    ) -> pd.DataFrame:
        """get route metadata from webservice, or load from cache"""
        cache_path = os.path.join(
            DATA_PATH,
            "route_data.json"
        )
        if os.path.isfile(cache_path):
            dt = pd.read_json(cache_path)
            return dt

        routes = requests.get(
            f"{self.base_url_routes}/appID/{self.app_id}/json/true/dir/true/stops/true/"
        )
        data = routes.json()["resultSet"]["route"]
        # parse loc id out of the return set
        replacement_data = []
        for idx, item in enumerate(data):
            old_dir = item["dir"]
            new_dirs = set()
            for dir in old_dir:
                try:
                    new_dir = set(x["locid"] for x in dir["stop"])
                except KeyError:
                    new_dir = set()
                new_dirs = new_dirs.union(new_dir)
            replacement_data.append((idx, new_dirs))
        for dt in replacement_data:
            idx, new_dirs = dt
            data[idx]["dir"] = list(new_dirs)
        print(data)
        with open(cache_path, "w") as f:
            json.dump(data, f)
        return pd.read_json(cache_path)

    def get_route_data(
        self
    ) -> dict:
        if self.route_id:
            df = self._get_all_routes()
            ds = df[df["id"] == self.route_id]
            return ds.to_dict(orient="records")[0]
        else:
            return {}

    #TODO clean up
    def fetch_arrival_data_by_route(
        self,
        stop_ids: Optional[Union[int, Sequence[int]]] = None,
        cache: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """Fetch arrival data from TriMet API for all stops on route, or a subset"""
        raise NotImplementedError("Not ready for looks yet")
        if not stop_ids:
            stop_ids = self.stop_ids
        elif isinstance(stop_ids, int):
            stop_ids = [stop_ids]
        else:
            # check they are legit
            stp_id_check = set(self.stop_ids)
            if any(stop_id not in stp_id_check for stop_id in stop_ids):
                raise ValueError("Some stop ids passed not in self.stop_ids, please revise")
        stop_ids = sorted(stop_ids)
        # for now, truncate stop_ids. add a for loop here for splices of 10
        stop_ids = [f"{si}" for si in stop_ids[-9:]]
        loc_ids = ",".join(stop_ids)
        #TODO - add a timestamp here
        cache_path = os.path.join(
            DATA_PATH,
            f"route {self.route_id} data",
            f"{'_'.join(stop_ids)}.json"
        )
        os.makedirs(os.path.dirname(cache_path),exist_ok=True)
        if not os.path.isfile(cache_path):
            print("Fetching data")
            res = requests.get(
                f"{self.base_url_arrivals}/appID/{self.app_id}/locIDs/{loc_ids}/"
            )
            data = res.json()
            with open(cache_path, "w") as f:
                json.dump(data, f)
        else:
            print("Reading data")
            with open(cache_path, 'r') as f:
                res = json.load(f)
            data = res
        # parse a bit
        # data frame
        print(data.keys())
        print(data["resultSet"].keys())
        print(data["resultSet"]["arrival"][0].keys())
        data = pd.DataFrame(data["resultSet"]["arrival"])
        
        return data

    def fetch_arrival_data_by_stop(
        self,
        stop_ids: Union[int, Sequence[int]],
        run_id: str = None
    ) -> pd.DataFrame:
        """arrivals by stop ids"""
        if not run_id:
            unique_run_id = str(uuid.uuid4())
        else:
            unique_run_id = run_id
        if isinstance(stop_ids, int):
            stop_ids = [stop_ids]

        dataset_cache_path = os.path.join(
            DATA_PATH,
            "stops",
            "raw-data"
        )
        os.makedirs(dataset_cache_path, exist_ok=True)
        # TODO - change to iterate by group of tens
        if len(stop_ids) > 10:
            raise NotImplementedError("Too many stop ids passed")
        loc_ids = ",".join(f"{s}" for s in stop_ids)
        dataset_cache_path = os.path.join(
            dataset_cache_path,
            f"{unique_run_id}.json"
        )

        if not os.path.isfile(dataset_cache_path):
            res = requests.get(
                f"{self.base_url_arrivals}/appID/{self.app_id}/locIDs/{loc_ids}/"
            )
            data = res.json()
            with open(dataset_cache_path, "w") as f:
                json.dump(data, f)
        else:
            with open(dataset_cache_path, "r") as f:
                res = json.load(f)
            data = res
        # parse it up
        data = pd.DataFrame(data["resultSet"]["arrival"])
        # dates to real dates
        data["estimated"] = pd.to_datetime(data["estimated"],origin="unix",unit="ms")
        data["estimated"] = data["estimated"].dt.tz_localize(tz="UTC").dt.tz_convert(tz="US/Pacific")
        data["scheduled"] = pd.to_datetime(data["scheduled"],origin="unix",unit="ms")
        data["scheduled"] = data["scheduled"].dt.tz_localize(tz="UTC").dt.tz_convert(tz="US/Pacific")

        # separate by locid, and route and save
        data["unique_run_id"] = unique_run_id
        # sort columns
        data.columns = sorted(data.columns)
        loc_path = os.path.join(
            DATA_PATH,
            "stops",
            "by-location"
        )
        route_path = os.path.join(
            DATA_PATH,
            "stops",
            "by-route-and-location"
        )
        os.makedirs(loc_path,exist_ok=True)
        os.makedirs(route_path,exist_ok=True)

        for locid in data["locid"].unique():
            df = data[data["locid"] == locid]
            for route in df["route"].unique():
                rf = df[df["route"] == route]
                if rf.empty:
                    continue
                route_path_specific = os.path.join(
                    route_path,
                    f"{route}",
                    f"{locid}.csv"
                )
                os.makedirs(os.path.dirname(route_path_specific),exist_ok=True)
                if os.path.isfile(route_path_specific):
                    rf.to_csv(
                        route_path_specific,
                        index=False,
                        header=False,
                        mode='a'
                    )
                else:
                    rf.to_csv(
                        route_path_specific,
                        index=False,
                        mode='a'
                    )
            loc_path_specific = os.path.join(
                loc_path,
                f"{locid}.csv"
            )
            if os.path.isfile(loc_path_specific):
                df.to_csv(
                    loc_path_specific,
                    index=False,
                    header=False,
                    mode="a"
                )
            else:
                df.to_csv(
                    loc_path_specific,
                    index=False,
                    mode="a"
                )

        return data


tmd = TriMetData(route_id=9)
df = tmd.fetch_arrival_data_by_stop(stop_ids=[13791,4539,13778,13825,13772,13773,14244,6545,6578,1375])
print(df[["vehicleID","tripID","locid","estimated","scheduled","departed","status","route"]])
print(df["estimated"] - df["scheduled"])