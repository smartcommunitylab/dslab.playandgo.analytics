import logging

import pandas as pd

import h3

from datetime import datetime
from datetime import timezone

from playandgo.pg_engine import PlayAndGoEngine
from valhalla.valhalla_engine import ValhallaEngine, convert_tracked_instance_to_points
from storage.storage_engine import FileStorage
from graph.graphmap import GraphMap

logger = logging.getLogger(__name__)

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
    logger.info(f"Found {len(nearest_edges)} unique edges.")


def extract_track_data_osm(territory_id, track, ls_tracks, ls_tracks_info, df_way_shapes, ls_nearest_edges, valhalla_engine, graph_map):
    start = datetime.now()

    bbox = graph_map.get_bbox(territory_id)

    track_id = str(track["_id"])

    # columns=['player_id', 'track_id', 'multimodal_id', 'mode', 'start_time']
    track_info = {'player_id': track['userId'], 'track_id': track_id, 'multimodal_id': track['multimodalId'], 
                  'mode': track['freeTrackingTransport'], 'start_time': get_utc_datetime(track['startTime'])}
    ls_tracks_info.append(track_info)

    trace_route = valhalla_engine.find_nearest_edges_by_trace(track, track_id) 

    # columns=['track_id', 'shape']
    track_shape = {'track_id': track_id, 'shape': trace_route.shape}
    ls_tracks.append(track_shape)

    if len(trace_route.trace_infos) > 0:
        lon_array =[]
        lat_array =[]
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
                        logger.warning(f"Error processing edge_info: {trace_info}, Error: {e1}")
            except Exception as e2:
                logger.warning(f"Error processing trace_info: {trace_info}, Error: {e2}")

            lon_array.append(trace_info.lon)
            lat_array.append(trace_info.lat)

        # check if trace is in bbox
        if graph_map.are_points_in_bbox(lon_array, lat_array, bbox):
            nearest_node = graph_map.find_nearest_nodes(lon_array, lat_array, track_id)
            for index, node_id in enumerate(nearest_node):
                trace_info = trace_route.trace_infos[index]
                cell = h3.latlng_to_cell(trace_info.lat, trace_info.lon, h3_res)
                #df_nearest_edges = ['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
                nearest_edge = {'track_id': track_id, 'h3': str(cell), 'timestamp': trace_info.timestamp, 
                                'node_id': node_id, 'way_id': trace_info.way_id, 'ordinal': index}
                ls_nearest_edges.append(nearest_edge)
        else:
            for index, trace_info in enumerate(trace_route.trace_infos):
                cell = h3.latlng_to_cell(trace_info.lat, trace_info.lon, h3_res)
                #df_nearest_edges = ['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
                nearest_edge = {'track_id': track_id, 'h3': str(cell), 'timestamp': trace_info.timestamp, 
                                'node_id': None, 'way_id': trace_info.way_id, 'ordinal': index}
                ls_nearest_edges.append(nearest_edge)

    stop = datetime.now()
    logger.info(f"Track ID: {track_id}, Time:{(stop - start).total_seconds()} seconds")


def extract_track_data_h3(track, ls_tracks_info, ls_nearest_edges):
    start = datetime.now()

    track_id = str(track["_id"])

    # columns=['player_id', 'track_id', 'multimodal_id', 'mode', 'start_time']
    track_info = {'player_id': track['userId'], 'track_id': track_id, 'multimodal_id': track['multimodalId'], 
                  'mode': track['freeTrackingTransport'], 'start_time': get_utc_datetime(track['startTime'])}
    ls_tracks_info.append(track_info)

    points = convert_tracked_instance_to_points(track)    
    sorted_points = sorted(points, key=lambda x: x["time"])
    for index, point in enumerate(sorted_points):
        cell = h3.latlng_to_cell(point['latitude'], point['longitude'], h3_res)
        #df_nearest_edges = ['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
        nearest_edge = {'track_id': track_id, 'h3': str(cell), 'timestamp': point['time'], 
                        'node_id': None, 'way_id': None, 'ordinal': index}
        ls_nearest_edges.append(nearest_edge)

    stop = datetime.now()
    logger.info(f"Track ID: {track_id}, Time:{(stop - start).total_seconds()} seconds")


def import_nearest_edges_by_trace(territory_id, start_time, track_modes, end_time=None, save_csv=False):
    logger.info(f"import_nearest_edges_by_trace")
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()
    file_storage = FileStorage()
    graph_map = GraphMap()

    dim = 0
    try:
        size, df_way_shapes = file_storage.load_dataframe(territory_id, file_storage.way_shapes)
        dim =  df_way_shapes.shape[0]
    except FileNotFoundError:
        df_way_shapes = pd.DataFrame(columns=['way_id', 'shape'])
    
    # columns=['track_id', 'shape']
    ls_tracks = [] 
    
    # columns=['player_id', 'track_id', 'multimodal_id', 'mode', 'start_time']
    ls_tracks_info = []

    # columns=['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
    ls_nearest_edges = []

    #track_modes = ["walk", "bike", "bus", "train", "car"]

    for track_mode in track_modes:
        logger.info(f"Processing mode: {track_mode}")
        if track_mode != "train":
            try:
                graph_map.load_graph_from_bbox(territory_id, track_mode)
            except ValueError as e:
                logger.info(f"Error loading graph for territory {territory_id} with mode {track_mode}: {e}")
                continue

            count = 0
            for track in playandgo_engine.get_tracks(territory_id, start_time, end_time, track_mode):
                try:
                    extract_track_data_osm(territory_id, track, ls_tracks, ls_tracks_info, df_way_shapes, ls_nearest_edges, valhalla_engine, graph_map)
                    logger.info(f"Track {track_mode} {count} processed.")
                except Exception as e:
                    logger.warning(f"Error processing track {track_mode} {count}: {e}")
                count += 1

        else:
            count = 0
            for track in playandgo_engine.get_tracks(territory_id, start_time, end_time, track_mode):
                extract_track_data_h3(track, ls_tracks_info, ls_nearest_edges)
                logger.info(f"Track {track_mode} {count} processed.")
                count += 1


    start_time_dt = datetime.fromisoformat(start_time)
    year = start_time_dt.strftime("%Y")

    infos = []

    df_tracks = pd.DataFrame(ls_tracks, columns=['track_id', 'shape'])
    rows, columns = df_tracks.shape
    logger.info(f"Imported Tracks Rows: {rows}, Columns: {columns}")
    file_storage.merge_tracks(territory_id, year, df_tracks, save_csv)
    info_map = {"name": file_storage.tracks, "rows": rows}
    infos.append(info_map)

    df_tracks_info = pd.DataFrame(ls_tracks_info, columns=['player_id', 'track_id', 'multimodal_id', 'mode', 'start_time'])
    rows, columns = df_tracks_info.shape
    logger.info(f"Imported Tracks Info Rows: {rows}, Columns: {columns}")
    file_storage.merge_tracks_info(territory_id, year, df_tracks_info, save_csv)
    info_map = {"name": file_storage.tracks_info, "rows": rows}
    infos.append(info_map)

    rows, columns = df_way_shapes.shape
    logger.info(f"Imported Way Shapes Rows: {rows}, Columns: {columns}")
    file_storage.merge_way_shapes(territory_id, df_way_shapes, save_csv)
    info_map = {"name": file_storage.way_shapes, "rows": (rows - dim)}
    infos.append(info_map)

    df_nearest_edges = pd.DataFrame(ls_nearest_edges, columns=['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal'])
    rows, columns = df_nearest_edges.shape
    logger.info(f"Imported Nearest Edges Rows: {rows}, Columns: {columns}")
    file_storage.merge_nearest_edges(territory_id, year, df_nearest_edges, save_csv)
    info_map = {"name": file_storage.nearest_edges, "rows": rows}
    infos.append(info_map)

    #rows, columns = df_h3_info.shape
    #logger.info(f"Imported H3 Rows: {rows}, Columns: {columns}")
    #file_storage.merge_h3_info(territory_id, year, df_h3_info, save_csv)
    #info_map = {"name": file_storage.h3_info, "rows": rows}
    #infos.append(info_map)

    return infos


def import_campaign_tracks_data(territory_id, start_time, end_time=None, save_csv=False):
    logger.info(f"import_campaign_tracks_data")
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()

    # Ottieni i dati da PlayAndGo
    # columns=['territory_id', 'player_id', 'track_id', 'campaign_id', 'campaign_type', 
    #                           'start_time', 'end_time', 'mode', 'validation_result', 'distance', 'duration']
    ls_tracks = []
    for c_track in playandgo_engine.get_campaign_tracks(territory_id, start_time, end_time):
        try:
            c_track.start_time = get_utc_datetime(c_track.start_time)
            c_track.end_time = get_utc_datetime(c_track.end_time)
            ls_tracks.append(c_track.__dict__)
            logger.info(f"Processed campaign track: {c_track.track_id}")
        except Exception as e:
            logger.warning(f"Error processing campaign track: {c_track.track_id}, Error: {e}")
            continue

    start_time_dt = datetime.fromisoformat(start_time)
    year = start_time_dt.strftime("%Y")

    df = pd.DataFrame(ls_tracks, columns=['territory_id', 'player_id', 'track_id', 'campaign_id', 'campaign_type', 
                                         'start_time', 'end_time', 'mode', 'validation_result', 'distance', 'duration'])
    rows, columns = df.shape
    logger.info(f"Imported Campaign Tracks Rows: {rows}, Columns: {columns}")
    file_storage.merge_campaign_tracks(territory_id, year, df, save_csv)
    info_map = {"name": file_storage.campaign_tracks, "rows": rows}
    return info_map


def import_campaign_groups_data(territory_id:str, year:str, save_csv=False):
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()

    # Ottieni i dati da PlayAndGo
    # columns=['territory_id', 'player_id', 'campaign_id', 'group_id']
    ls_groups = []
    for c_group in playandgo_engine.get_campaign_groups(territory_id, year):
        try:
            ls_groups.append(c_group.__dict__)
            logger.info(f"Processed campaign group: {c_group.player_id}, {c_group.campaign_id}")
        except Exception as e:
            logger.warning(f"Error processing campaign group: {c_group.player_id}, {c_group.campaign_id}, Error: {e}")
            continue
        
    #start_time_dt = datetime.fromisoformat(start_time)
    #year = start_time_dt.strftime("%Y")

    df = pd.DataFrame(ls_groups, columns=['territory_id', 'player_id', 'campaign_id', 'group_id'])
    rows, columns = df.shape
    logger.info(f"Imported Campaign Groups Rows: {rows}, Columns: {columns}")
    file_storage.merge_campaign_groups(territory_id, year, df, save_csv)
    info_map = {"name": file_storage.campaign_groups, "rows": rows}
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
        df_info = get_df_info(file_storage, territory_id, file_storage.campaign_groups, year)
        info_list.append(df_info)
    except FileNotFoundError:
        logger.error(f"File not found for campaign_groups in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.campaign_tracks, year)
        info_list.append(df_info)
    except FileNotFoundError:
        logger.error(f"File not found for campaign_tracks in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.tracks, year)
        info_list.append(df_info)
    except FileNotFoundError:
        logger.error(f"File not found for tracks in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.nearest_edges, year)
        info_list.append(df_info)
    except FileNotFoundError:
        logger.error(f"File not found for nearest_edges in territory {territory_id} for year {year}")

    try:
        df_info = get_df_info(file_storage, territory_id, file_storage.way_shapes)
        info_list.append(df_info)
    except FileNotFoundError:
        logger.error(f"File not found for way_shapes in territory {territory_id}")

    return info_list


def merge_campaign_tracks_groups(territory_id:str, year:str, campaign_id:str, save_csv=False):
    file_storage = FileStorage()
    try:
        df = file_storage.merge_df_campaign_tracks_groups_by_campaign(territory_id, year, campaign_id)
        file_storage.save_df(territory_id, file_storage.mapped_campaign_groups, df, year, save_csv)
        df_info = get_df_info(file_storage, territory_id, file_storage.mapped_campaign_groups, year)
        return df_info
    except FileNotFoundError:
        logger.error(f"File not found for campaign_subscriptions in territory {territory_id} for year {year}")


def import_campaign_tracks_info_data(territory_id:str, year:str, save_csv=False):
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()

    # columns=['territory_id', 'player_id', 'campaign_id', 'track_id', 'multimodal_id', 'way_back', 'location_id']
    ls_tracks = []
    for c_track in playandgo_engine.get_campaign_tracks_info(territory_id, year):
        try:
            ls_tracks.append(c_track.__dict__)
            logger.info(f"Processed campaign info track: {c_track.player_id}, {c_track.track_id}, {c_track.campaign_id}")
        except Exception as e:
            logger.warning(f"Error processing campaign info track: {c_track.player_id}, {c_track.track_id}, {c_track.campaign_id}, Error: {e}")
            continue
        
    #start_time_dt = datetime.fromisoformat(start_time)
    #year = start_time_dt.strftime("%Y")

    df = pd.DataFrame(ls_tracks, columns=['territory_id', 'player_id', 'campaign_id', 'track_id', 'way_back', 'location_id'])
    rows, columns = df.shape
    logger.info(f"Imported Campaign Info Track Rows: {rows}, Columns: {columns}")
    file_storage.merge_campaign_tracks_info(territory_id, year, df, save_csv)
    info_map = {"name": file_storage.campaign_tracks_info, "rows": rows}
    return info_map

