import os

import numpy as np
import pandas as pd

from flask import Flask, request, Response

from datetime import datetime
from datetime import timezone

from playandgo.pg_engine import PlayAndGoEngine
from valhalla.valhalla_engine import ValhallaEngine
from storage.storage_engine import FileStorage

def get_utc_datetime(dt):
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    return dt


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


def import_nearest_edges_by_trace(territory_id, start_time, end_time=None, save_csv=False):
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()
    file_storage = FileStorage()

    try:
        df_way_shapes = file_storage.load_dataframe(territory_id, file_storage.way_shapes)
    except FileNotFoundError:
        df_way_shapes = pd.DataFrame(columns=['way_id', 'shape'])
    df_tracks = pd.DataFrame(columns=['track_id', 'shape'])
    df_nearest_edges = pd.DataFrame(columns=['track_id', 'way_id', 'timestamp', 'ordinal'])

    for track in playandgo_engine.get_tracks(territory_id, start_time, end_time):
        start = datetime.now()

        track_id = str(track["_id"])
        trace_route = valhalla_engine.find_nearest_edges_by_trace(track, track_id) 
        
        if df_tracks.empty:
            df_tracks.loc[0] = [track_id, trace_route.shape]
        else:
            df_tracks.loc[df_tracks.index.max() + 1] = [track_id, trace_route.shape]
        
        if len(trace_route.trace_infos) > 0:
            index = 0
            last_way_id = -1
            for trace_info in trace_route.trace_infos:
                # check way shape
                if not trace_info.way_id in df_way_shapes['way_id'].values:
                    edge_info = valhalla_engine.find_nearest_edges_by_osm_way(track, trace_info.way_id, 
                                                                              trace_info.lon, trace_info.lat)
                    if edge_info is not None:
                        if df_way_shapes.empty:
                            df_way_shapes.loc[0] = [edge_info.way_id, edge_info.shape]
                        else:
                            df_way_shapes.loc[df_way_shapes.index.max() + 1] = [edge_info.way_id, edge_info.shape]

                if trace_info.way_id != last_way_id:
                    last_way_id = trace_info.way_id
                    if df_nearest_edges.empty:
                        df_nearest_edges.loc[0] = [track_id, trace_info.way_id, 
                                                    trace_info.timestamp, index]
                    else:
                        df_nearest_edges.loc[df_nearest_edges.index.max() + 1] = [track_id, trace_info.way_id, 
                                                    trace_info.timestamp, index]
                    index += 1

        stop = datetime.now()
        print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Time:{(stop - start).total_seconds()} seconds")

    start_time_dt = datetime.fromisoformat(start_time)
    year = start_time_dt.strftime("%Y")

    rows, columns = df_tracks.shape
    print(f"Imported Tracks Rows: {rows}, Columns: {columns}")
    file_storage.merge_tracks(territory_id, year, df_tracks, save_csv)

    rows, columns = df_way_shapes.shape
    print(f"Imported Way Shapes Rows: {rows}, Columns: {columns}")
    file_storage.merge_way_shapes(territory_id, df_way_shapes, save_csv)

    rows, columns = df_nearest_edges.shape
    print(f"Imported Nearest Edges Rows: {rows}, Columns: {columns}")
    file_storage.merge_nearest_edges(territory_id, year, df_nearest_edges, save_csv)


def import_campaign_tracks_data(territory_id, start_time, end_time=None, save_csv=False):
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()

    # Ottieni i dati da PlayAndGo
    df = pd.DataFrame(columns=['territory_id', 'player_id', 'track_id', 'campaign_id', 'campaign_type', 
                               'start_time', 'end_time', 'mode', 'validation_result', 'distance', 'duration'])
    for c_track in playandgo_engine.get_campaign_tracks(territory_id, start_time, end_time):
        #print(f"Campaign Track: {c_track}")
        c_track.start_time = get_utc_datetime(c_track.start_time)
        c_track.end_time = get_utc_datetime(c_track.end_time)
        if df.empty:
            df.loc[0] = vars(c_track)
        else:
            df.loc[df.index.max() + 1] = vars(c_track)

    start_time_dt = datetime.fromisoformat(start_time)
    year = start_time_dt.strftime("%Y")

    rows, columns = df.shape
    print(f"Imported Campaign Tracks Rows: {rows}, Columns: {columns}")
    file_storage.merge_campaign_tracks(territory_id, year, df, save_csv)


def import_campaign_subscriptions_data(territory_id, start_time, end_time=None, save_csv=False):
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()

    # Ottieni i dati da PlayAndGo
    df = pd.DataFrame(columns=['territory_id', 'player_id', 'campaign_id', 'campaign_type', 
                               'registration_date', 'group_id'])
    for c_subscription in playandgo_engine.get_campaign_subscriptions(territory_id, start_time, end_time):
        #print(f"Campaign Subscription: {c_subscription}")
        c_subscription.registration_date = get_utc_datetime(c_subscription.registration_date)
        if df.empty:
            df.loc[0] = vars(c_subscription)
        else:
            df.loc[df.index.max() + 1] = vars(c_subscription)

    start_time_dt = datetime.fromisoformat(start_time)
    year = start_time_dt.strftime("%Y")

    rows, columns = df.shape
    print(f"Imported Campaign Sybscriptions Rows: {rows}, Columns: {columns}")
    file_storage.merge_campaign_subscriptions(territory_id, year, df, save_csv)


def get_df_info(file_storage, territory_id:str, df_file:str, year:str=None):
    size, df = file_storage.load_dataframe(territory_id, df_file, year)
    rows, columns = df.shape
    df_info = {
        "df_name": df_file,
        "rows": rows,
        "columns": columns,
        "memory": int(df.memory_usage(deep=True).sum() / 1024),  # Convert bytes to kilobytes
        "file_size": int(size / 1024)
    }
    return df_info


def get_df_info_map(territory_id:str, year:str):
    file_storage = FileStorage()
    info_map = {}

    df_info = get_df_info(file_storage, territory_id, "campaign_subscriptions", year)
    info_map["campaign_subscriptions"] = df_info

    df_info = get_df_info(file_storage, territory_id, "campaign_tracks", year)
    info_map["campaign_tracks"] = df_info

    df_info = get_df_info(file_storage, territory_id, "tracks", year)
    info_map["tracks"] = df_info

    df_info = get_df_info(file_storage, territory_id, "nearest_edges", year)
    info_map["nearest_edges"] = df_info

    df_info = get_df_info(file_storage, territory_id, "way_shapes", year)
    info_map["way_shapes"] = df_info

    return info_map


app = Flask(__name__)
server_port = os.getenv("SERVER_PORT", 8078)

@app.route('/api/import/campaign-tracks', methods=['GET'])
def api_import_campaign_tracks_data():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    start_time = request.args.get('start_time', type=str)
    end_time = request.args.get('end_time', default=None, type=str)
    save_csv = request.args.get('save_csv', default=False, type=bool)
    import_campaign_tracks_data(territory_id, start_time, end_time, save_csv)
    stop = datetime.now()
    print(f"api_import_campaign_tracks_data Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return Response(status=200)


@app.route('/api/import/campaign-subs', methods=['GET'])
def api_import_campaign_subscriptions_data():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    start_time = request.args.get('start_time', type=str)
    end_time = request.args.get('end_time', default=None, type=str)
    save_csv = request.args.get('save_csv', default=False, type=bool)
    import_campaign_subscriptions_data(territory_id, start_time, end_time, save_csv)
    stop = datetime.now()
    print(f"api_import_campaign_subscriptions_data Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return Response(status=200)


@app.route('/api/import/nearest-edges', methods=['GET'])
def api_import_nearest_edges_by_trace():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    start_time = request.args.get('start_time', type=str)
    end_time = request.args.get('end_time', default=None, type=str)
    save_csv = request.args.get('save_csv', default=False, type=bool)
    import_nearest_edges_by_trace(territory_id, start_time, end_time, save_csv)
    stop = datetime.now()
    print(f"api_import_nearest_edges_by_trace Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return Response(status=200)


@app.route('/api/import/info', methods=['GET'])
def api_info_df():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    year = request.args.get('year', type=str)
    info_map = get_df_info(territory_id, year)
    stop = datetime.now()
    print(f"api_info_df Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return info_map


if __name__ == "__main__":    
    app.run(host='0.0.0.0', port=server_port)

    #start_time = "2025-05-01T00:00:00+00:00"
    #end_time = "2025-05-30T23:59:59+00:00"

    #import_campaign_tracks_data("L", start_time)
    #import_campaign_subscriptions_data("L", start_time)
    #import_nearest_edges_by_locate("TN", start_time)
    #import_nearest_edges_by_trace("L", start_time, end_time)
