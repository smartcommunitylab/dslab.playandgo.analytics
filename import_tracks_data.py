import pandas as pd

import h3

from datetime import datetime
from datetime import timezone

from playandgo.pg_engine import PlayAndGoEngine
from valhalla.valhalla_engine import ValhallaEngine, convert_tracked_instance_to_points
from storage.storage_engine import FileStorage
from graph.graphmap import GraphMap

h3_res = 13  # H3 resolution level

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


def extract_track_data_osm(territory_id, track, df_tracks, df_tracks_info, df_way_shapes, df_nearest_edges, valhalla_engine, graph_map):
    start = datetime.now()

    bbox = graph_map.get_bbox(territory_id)

    track_id = str(track["_id"])

    if df_tracks_info.empty:
        df_tracks_info.loc[0] = [track['userId'], track_id, track['multimodalId'], track['freeTrackingTransport'], get_utc_datetime(track['startTime'])]
    else:
        df_tracks_info.loc[df_tracks_info.index.max() + 1] = [track['userId'], track_id, track['multimodalId'], track['freeTrackingTransport'], get_utc_datetime(track['startTime'])]

    trace_route = valhalla_engine.find_nearest_edges_by_trace(track, track_id) 

    if df_tracks.empty:
        df_tracks.loc[0] = [track_id, trace_route.shape]
    else:
        df_tracks.loc[df_tracks.index.max() + 1] = [track_id, trace_route.shape]

    if len(trace_route.trace_infos) > 0:
        #lon_array =[]
        #lat_array =[]
        for index, trace_info in enumerate(trace_route.trace_infos):
            try:
                # check way shape
                if not trace_info.way_id in df_way_shapes['way_id'].values:
                    try:
                        edge_info = valhalla_engine.find_nearest_edges_by_osm_way(track, trace_info.way_id, 
                                                                                trace_info.lon, trace_info.lat)
                        if edge_info is not None:
                            if df_way_shapes.empty:
                                df_way_shapes.loc[0] = [edge_info.way_id, edge_info.shape]
                            else:
                                df_way_shapes.loc[df_way_shapes.index.max() + 1] = [edge_info.way_id, edge_info.shape]
                    except Exception as e1:
                        print(f"Error processing edge_info: {trace_info}, Error: {e1}")

                node_id = None
                # check if point is in bbox
                if graph_map.is_point_in_bbox(trace_info.lon, trace_info.lat, bbox):
                    node_id = graph_map.find_nearest_nodes(trace_info.lon, trace_info.lat)
                cell = h3.latlng_to_cell(trace_info.lat, trace_info.lon, h3_res)
                #df_nearest_edges = ['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
                if df_nearest_edges.empty:
                    df_nearest_edges.loc[0] = [track_id, str(cell), trace_info.timestamp, node_id, trace_info.way_id, index]
                else:
                    df_nearest_edges.loc[df_nearest_edges.index.max() + 1] = [track_id, str(cell), trace_info.timestamp, node_id, trace_info.way_id, index]
            except Exception as e2:
                print(f"Error processing trace_info: {trace_info}, Error: {e2}")

    stop = datetime.now()
    print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Time:{(stop - start).total_seconds()} seconds")


def extract_track_data_h3(track, df_tracks_info, df_nearest_edges):
    start = datetime.now()

    track_id = str(track["_id"])

    if df_tracks_info.empty:
        df_tracks_info.loc[0] = [track['userId'], track_id, track['multimodalId'], track['freeTrackingTransport'], get_utc_datetime(track['startTime'])]
    else:
        df_tracks_info.loc[df_tracks_info.index.max() + 1] = [track['userId'], track_id, track['multimodalId'], track['freeTrackingTransport'], get_utc_datetime(track['startTime'])]

    points = convert_tracked_instance_to_points(track)    
    sorted_points = sorted(points, key=lambda x: x["time"])
    for index, point in enumerate(sorted_points):
        cell = h3.latlng_to_cell(point['latitude'], point['longitude'], h3_res)
        #df_nearest_edges = ['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
        if df_nearest_edges.empty:
            df_nearest_edges.loc[0] = [track_id, str(cell), point['time'], None, None, index]
        else:
            df_nearest_edges.loc[df_nearest_edges.index.max() + 1] = [track_id, str(cell), point['time'], None, None, index]

    stop = datetime.now()
    print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Time:{(stop - start).total_seconds()} seconds")


def import_nearest_edges_by_trace(territory_id, start_time, track_modes, end_time=None, save_csv=False):
    print(f"import_nearest_edges_by_trace")
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()
    file_storage = FileStorage()
    graph_map = GraphMap()
    print(f"import_nearest_edges_by_trace init done")

    dim = 0
    try:
        size, df_way_shapes = file_storage.load_dataframe(territory_id, file_storage.way_shapes)
        dim =  df_way_shapes.shape[0]
    except FileNotFoundError:
        df_way_shapes = pd.DataFrame(columns=['way_id', 'shape'])
    df_tracks = pd.DataFrame(columns=['track_id', 'shape'])
    df_tracks_info = pd.DataFrame(columns=['player_id', 'track_id', 'multimodal_id', 'mode', 'start_time'])
    df_nearest_edges = pd.DataFrame(columns=['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal'])
    #df_h3_info = pd.DataFrame(columns=['track_id', 'h3', 'timestamp', 'ordinal'])

    #track_modes = ["walk", "bike", "bus", "train", "car"]

    for track_mode in track_modes:
        print(f"Processing mode: {track_mode}")
        if track_mode != "train":
            try:
                graph_map.load_graph_from_bbox(territory_id, track_mode)
            except ValueError as e:
                print(f"Error loading graph for territory {territory_id} with mode {track_mode}: {e}")
                continue

            count = 0
            for track in playandgo_engine.get_tracks(territory_id, start_time, end_time, track_mode):
                try:
                    extract_track_data_osm(territory_id, track, df_tracks, df_tracks_info, df_way_shapes, df_nearest_edges, valhalla_engine, graph_map)
                    print(f"Track {track_mode} {count} processed.")
                except Exception as e:
                    print(f"Error processing track {track_mode} {count}: {e}")
                count += 1

        else:
            count = 0
            for track in playandgo_engine.get_tracks(territory_id, start_time, end_time, track_mode):
                extract_track_data_h3(track, df_tracks_info, df_nearest_edges)
                print(f"Track {track_mode} {count} processed.")
                count += 1


    start_time_dt = datetime.fromisoformat(start_time)
    year = start_time_dt.strftime("%Y")

    infos = []

    rows, columns = df_tracks.shape
    print(f"Imported Tracks Rows: {rows}, Columns: {columns}")
    file_storage.merge_tracks(territory_id, year, df_tracks, save_csv)
    info_map = {"name": file_storage.tracks, "rows": rows}
    infos.append(info_map)

    rows, columns = df_tracks_info.shape
    print(f"Imported Tracks Info Rows: {rows}, Columns: {columns}")
    file_storage.merge_tracks_info(territory_id, year, df_tracks_info, save_csv)
    info_map = {"name": file_storage.tracks_info, "rows": rows}
    infos.append(info_map)

    rows, columns = df_way_shapes.shape
    print(f"Imported Way Shapes Rows: {rows}, Columns: {columns}")
    file_storage.merge_way_shapes(territory_id, df_way_shapes, save_csv)
    info_map = {"name": file_storage.way_shapes, "rows": (rows - dim)}
    infos.append(info_map)

    rows, columns = df_nearest_edges.shape
    print(f"Imported Nearest Edges Rows: {rows}, Columns: {columns}")
    file_storage.merge_nearest_edges(territory_id, year, df_nearest_edges, save_csv)
    info_map = {"name": file_storage.nearest_edges, "rows": rows}
    infos.append(info_map)

    #rows, columns = df_h3_info.shape
    #print(f"Imported H3 Rows: {rows}, Columns: {columns}")
    #file_storage.merge_h3_info(territory_id, year, df_h3_info, save_csv)
    #info_map = {"name": file_storage.h3_info, "rows": rows}
    #infos.append(info_map)

    return infos


def import_campaign_tracks_data(territory_id, start_time, end_time=None, save_csv=False):
    print(f"import_campaign_tracks_data")
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()

    # Ottieni i dati da PlayAndGo
    df = pd.DataFrame(columns=['territory_id', 'player_id', 'track_id', 'campaign_id', 'campaign_type', 
                               'start_time', 'end_time', 'mode', 'validation_result', 'distance', 'duration'])
    for c_track in playandgo_engine.get_campaign_tracks(territory_id, start_time, end_time):
        try:
            c_track.start_time = get_utc_datetime(c_track.start_time)
            c_track.end_time = get_utc_datetime(c_track.end_time)
            if df.empty:
                df.loc[0] = vars(c_track)
            else:
                df.loc[df.index.max() + 1] = vars(c_track)
            print(f"Processed campaign track: {c_track.track_id}")
        except Exception as e:
            print(f"Error processing campaign track: {c_track.track_id}, Error: {e}")
            continue

    start_time_dt = datetime.fromisoformat(start_time)
    year = start_time_dt.strftime("%Y")

    rows, columns = df.shape
    print(f"Imported Campaign Tracks Rows: {rows}, Columns: {columns}")
    file_storage.merge_campaign_tracks(territory_id, year, df, save_csv)
    info_map = {"name": file_storage.campaign_tracks, "rows": rows}
    return info_map


def import_campaign_subscriptions_data(territory_id, start_time, end_time=None, save_csv=False):
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()

    # Ottieni i dati da PlayAndGo
    df = pd.DataFrame(columns=['territory_id', 'player_id', 'campaign_id', 'campaign_type', 
                               'registration_date', 'group_id'])
    for c_subscription in playandgo_engine.get_campaign_subscriptions(territory_id, start_time, end_time):
        try:
            c_subscription.registration_date = get_utc_datetime(c_subscription.registration_date)
            if df.empty:
                df.loc[0] = vars(c_subscription)
            else:
                df.loc[df.index.max() + 1] = vars(c_subscription)
            print(f"Processed campaign subscription: {c_subscription.player_id}, {c_subscription.campaign_id}")
        except Exception as e:
            print(f"Error processing campaign subscription: {c_subscription.campaign_id}, Error: {e}")
            continue
        
    start_time_dt = datetime.fromisoformat(start_time)
    year = start_time_dt.strftime("%Y")

    rows, columns = df.shape
    print(f"Imported Campaign Sybscriptions Rows: {rows}, Columns: {columns}")
    file_storage.merge_campaign_subscriptions(territory_id, year, df, save_csv)
    info_map = {"name": file_storage.campaign_subscriptions, "rows": rows}
    return info_map


def get_df_info(file_storage, territory_id:str, df_file:str, year:str=None):
    size, df = file_storage.load_dataframe(territory_id, df_file, year)
    rows, columns = df.shape
    df_info = {
        "df_name": df_file,
        "rows": rows,
        "columns": columns,
        "memory_KB": int(df.memory_usage(deep=True).sum() / 1024),  # Convert bytes to kilobytes
        "file_size_KB": int(size / 1024)
    }
    return df_info


def get_df_info_list(territory_id:str, year:str):
    file_storage = FileStorage()
    info_list = []

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.campaign_subscriptions, year)
        info_list.append(df_info)
    except FileNotFoundError:
        print(f"File not found for campaign_subscriptions in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.campaign_tracks, year)
        info_list.append(df_info)
    except FileNotFoundError:
        print(f"File not found for campaign_tracks in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.tracks, year)
        info_list.append(df_info)
    except FileNotFoundError:
        print(f"File not found for tracks in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.tracks_info, year)
        info_list.append(df_info)
    except FileNotFoundError:
        print(f"File not found for tracks_info in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.nearest_edges, year)
        info_list.append(df_info)
    except FileNotFoundError:
        print(f"File not found for nearest_edges in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.h3_info, year)
        info_list.append(df_info)
    except FileNotFoundError:
        print(f"File not found for h3_info in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.way_shapes)
        info_list.append(df_info)
    except FileNotFoundError:
        print(f"File not found for way_shapes in territory {territory_id}")

    return info_list


def merge_edges(territory_id:str, year:str, save_csv=False):
    file_storage = FileStorage()
    try:
        file_storage.merge_mapped_edges(territory_id, year, save_csv)
        df_info = get_df_info(file_storage, territory_id, file_storage.mapped_edges, year)
        return df_info
    except FileNotFoundError:
        print(f"File not found for campaign_subscriptions in territory {territory_id} for year {year}")


def merge_campaign_tracks(territory_id:str, year:str, campaign_id:str, save_csv=False):
    file_storage = FileStorage()
    try:
        file_storage.merge_campaign_tracks(territory_id, year, campaign_id, save_csv)
        df_info = get_df_info(file_storage, territory_id, file_storage.mapped_campaign_tracks, year)
        return df_info
    except FileNotFoundError:
        print(f"File not found for campaign_subscriptions in territory {territory_id} for year {year}")
