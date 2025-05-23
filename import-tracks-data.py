import numpy as np
import pandas as pd

from playandgo.pg_engine import PlayAndGoEngine
from valhalla.valhalla_engine import ValhallaEngine
from storage.storage_engine import FileStorage


def find_nearest_edges_by_locate(territory_id, start_time):
    nearest_edges = []
    for track in playandgo_engine.get_tracks(territory_id, start_time):
        track_id = track["_id"]
        for edge_info in valhalla_engine.find_nearest_edges_by_locate(track, track_id):
            if not edge_info in nearest_edges:
                nearest_edges.append(edge_info)
    print(f"Found {len(nearest_edges)} unique edges.")


def find_nearest_edges_by_trace(territory_id, start_time):
    nearest_edges = []
    for track in playandgo_engine.get_tracks(territory_id, start_time):
        track_id = track["_id"]
        trace_route = valhalla_engine.find_nearest_edges_by_trace(track, track_id) 
        for trace_info in trace_route.trace_infos:
            if not trace_info.way_id in nearest_edges:
                nearest_edges.append(trace_info.way_id)
    print(f"Found {len(nearest_edges)} unique edges.")


if __name__ == "__main__":
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()
    file_storage = FileStorage("./files")

    # Ottieni i dati da PlayAndGo
    start_time = "2025-03-01T00:00:00+00:00"

    df = pd.DataFrame(columns=['territory_id', 'player_id', 'track_id', 'campaign_id', 'campaign_type', 
                               'start_time', 'end_time', 'mode', 'validation_result', 'distance', 'duration'])
    for c_track in playandgo_engine.get_campaign_tracks("TAA", start_time):
        print(f"Campaign Track: {c_track}")
        if df.empty:
            df.loc[0] = vars(c_track)
        else:
            df.loc[df.index.max() + 1] = vars(c_track)
    file_storage.merge_campaign_tracks("TAA", df)

    #find_nearest_edges_by_locate("TN", start_time)
    #find_nearest_edges_by_trace("TN", start_time)

"""     df = pd.DataFrame(

        {

            "a": list("abc"),

            "b": list(range(1, 4)),

            "c": np.arange(3, 6).astype("u1"),

            "d": np.arange(4.0, 7.0, dtype="float64"),

            "e": [True, False, True]

        }

    )
 """


    #file_storage.save_campaign_tracks(territory_id, df)
    #df2 = file_storage.load_campaign_tracks(territory_id)
    #print(df2.head())
