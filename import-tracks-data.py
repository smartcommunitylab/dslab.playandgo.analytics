import numpy as np
import pandas as pd

from playandgo.pg_engine import PlayAndGoEngine
from valhalla.valhalla_engine import ValhallaEngine
from storage.storage_engine import FileStorage


def import_nearest_edges_by_locate(territory_id, start_time):
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()

    nearest_edges = []
    for track in playandgo_engine.get_tracks(territory_id, start_time):
        track_id = str(track["_id"])
        for edge_info in valhalla_engine.find_nearest_edges_by_locate(track, track_id):
            if not edge_info in nearest_edges:
                nearest_edges.append(edge_info)
    print(f"Found {len(nearest_edges)} unique edges.")


def import_nearest_edges_by_trace(territory_id, start_time):
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()
    file_storage = FileStorage()

    df_tracks = pd.DataFrame(columns=['track_id', 'shape'])
    for track in playandgo_engine.get_tracks(territory_id, start_time):
        track_id = str(track["_id"])
        trace_route = valhalla_engine.find_nearest_edges_by_trace(track, track_id) 
        
        if df_tracks.empty:
            df_tracks.loc[0] = [track_id, trace_route.shape]
        else:
            df_tracks.loc[df_tracks.index.max() + 1] = [track_id, trace_route.shape]

    rows, columns = df_tracks.shape
    print(f"Imported Rows: {rows}, Columns: {columns}")
    file_storage.merge_tracks(territory_id, df_tracks)

"""         for trace_info in trace_route.trace_infos:
            if not trace_info.way_id in nearest_edges:
                nearest_edges.append(trace_info.way_id)
        print(f"Found {len(nearest_edges)} unique edges.")
 """

def import_campaign_tracks_data(territory_id, start_time):
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()

    # Ottieni i dati da PlayAndGo
    df = pd.DataFrame(columns=['territory_id', 'player_id', 'track_id', 'campaign_id', 'campaign_type', 
                               'start_time', 'end_time', 'mode', 'validation_result', 'distance', 'duration'])
    for c_track in playandgo_engine.get_campaign_tracks(territory_id, start_time):
        print(f"Campaign Track: {c_track}")
        if df.empty:
            df.loc[0] = vars(c_track)
        else:
            df.loc[df.index.max() + 1] = vars(c_track)
    
    rows, columns = df.shape
    print(f"Imported Rows: {rows}, Columns: {columns}")
    file_storage.merge_campaign_tracks(territory_id, df)


if __name__ == "__main__":
    start_time = "2025-03-01T00:00:00+00:00"

    #import_campaign_tracks_data("TAA", start_time)
    #import_nearest_edges_by_locate("TN", start_time)
    import_nearest_edges_by_trace("TN", start_time)
