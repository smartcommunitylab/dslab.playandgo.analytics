import numpy as np
import pandas as pd

from playandgo.pg_engine import PlayAndGoEngine
from valhalla.valhalla_engine import ValhallaEngine
from storage.storage_engine import FileStorage


def import_nearest_edges_by_locate(territory_id, start_time, end_time=None):
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()

    nearest_edges = []
    for track in playandgo_engine.get_tracks(territory_id, start_time, end_time):
        track_id = str(track["_id"])
        for edge_info in valhalla_engine.find_nearest_edges_by_locate(track, track_id):
            if not edge_info in nearest_edges:
                nearest_edges.append(edge_info)
    print(f"Found {len(nearest_edges)} unique edges.")


def import_nearest_edges_by_trace(territory_id, start_time, end_time=None):
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()
    file_storage = FileStorage()

    way_shape_map = {}

    df_tracks = pd.DataFrame(columns=['track_id', 'shape'])
    df_nearest_edges = pd.DataFrame(columns=['track_id', 'way_id', 'travel_mode', 'lon', 'lat',
                                             'distance_from_trace_point', 'distance_along_edge', 'shape'])
    for track in playandgo_engine.get_tracks(territory_id, start_time, end_time):
        track_id = str(track["_id"])
        trace_route = valhalla_engine.find_nearest_edges_by_trace(track, track_id) 
        
        if df_tracks.empty:
            df_tracks.loc[0] = [track_id, trace_route.shape]
        else:
            df_tracks.loc[df_tracks.index.max() + 1] = [track_id, trace_route.shape]
        
        if len(trace_route.trace_infos) > 0:
            for trace_info in trace_route.trace_infos:
                # check way shape
                if trace_info.way_id not in way_shape_map:
                    edge_info = valhalla_engine.find_nearest_edges_by_osm_way(track, trace_info.way_id, 
                                                                              trace_info.lon, trace_info.lat)
                    if edge_info is not None:
                        way_shape_map[trace_info.way_id] = edge_info.shape
                    else:
                        way_shape_map[trace_info.way_id] = None

                trace_info.shape = way_shape_map[trace_info.way_id]        
                if df_nearest_edges.empty:
                    df_nearest_edges.loc[0] = [track_id, trace_info.way_id, trace_info.travel_mode, 
                                                trace_info.lon, trace_info.lat, trace_info.distance_from_trace_point, 
                                                trace_info.distance_along_edge, trace_info.shape]
                else:
                    df_nearest_edges.loc[df_nearest_edges.index.max() + 1] = [track_id, trace_info.way_id, 
                                                trace_info.travel_mode, trace_info.lon, trace_info.lat, 
                                                trace_info.distance_from_trace_point, trace_info.distance_along_edge, 
                                                trace_info.shape]
                    
    rows, columns = df_tracks.shape
    print(f"Imported Tracks Rows: {rows}, Columns: {columns}")
    file_storage.merge_tracks(territory_id, df_tracks)

    rows, columns = df_nearest_edges.shape
    print(f"Imported Nearest Edges Rows: {rows}, Columns: {columns}")
    file_storage.merge_nearest_edges(territory_id, df_nearest_edges)


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
    end_time = "2025-05-30T23:59:59+00:00"

    #import_campaign_tracks_data("TAA", start_time)
    #import_nearest_edges_by_locate("TN", start_time)
    import_nearest_edges_by_trace("TN", start_time, end_time)
