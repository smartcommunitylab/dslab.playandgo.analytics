import os
import duckdb
import pandas as pd
import h3
from pathlib import Path
from enum import Enum
from datetime import datetime
from datetime import timezone
from zoneinfo import ZoneInfo

class TimeSlot(Enum):
    S_05_10 = "05-10"
    S_10_15 = "10-15"
    S_15_20 = "15-20"
    S_20_05 = "20-05"


class DuckEngine:
    
    def __init__(self, territory_id:str, campaign_id:str=None, read_only:bool=True, tz:str="Europe/Rome"):
        self.territory_id = territory_id
        self.tz = tz
        self.store_path = os.getenv("STORAGE_PATH", "./files/")
        if self.store_path.endswith("/") or self.store_path.endswith("\\"):
            self.store_path = self.store_path[:-1]
        if campaign_id is not None:
            self.database_path = Path(f"{self.store_path}/{territory_id}/campaigns/{campaign_id}_database.duckdb").absolute()
        else:
            self.database_path = Path(f"{self.store_path}/{territory_id}/duckdb_database.duckdb").absolute()
        self.conn = duckdb.connect(self.database_path, read_only=read_only)

        self.search_path = Path(f"{self.store_path}/{territory_id}").absolute()

        self.conn.execute(f"SET file_search_path ='{self.search_path}'")
        #self.conn.execute("CALL start_ui();")
        self.table_nearest_edges = "nearest_edges"
        self.table_track_info = "track_info"


    def execute_query(self, query):
        return self.conn.execute(query).fetchall()


    def close(self):
        self.conn.close()


    def import_parquet(self, table_name:str, file_pattern:str):
        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{file_pattern}')")

    
    def import_dataframe(self, table_name:str, df:pd.DataFrame):
        if df is not None and not df.empty:
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")


    def convert_nearest_edges(self, df_nearest_edges, h3_level:int) -> pd.DataFrame: 
        # df_nearest_edges columns=['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
        # Crea una nuova colonna con l'H3 parent
        df_nearest_edges['h3_parent'] = df_nearest_edges['h3'].apply(lambda x: h3.cell_to_parent(x, h3_level))
        edges = []
        last_track_id = ""
        for track_id, timestamp, h3_id in zip(df_nearest_edges['track_id'], df_nearest_edges['timestamp'], df_nearest_edges['h3_parent']):
            if track_id != last_track_id:
                last_track_id = track_id
                last_h3_id = ""
                index = 0
            if h3_id != last_h3_id:
                last_h3_id = h3_id
                edges.append((track_id, h3_id, timestamp, index))
                index += 1
        
        df_duck_nearest_edges = pd.DataFrame(edges, columns=['track_id', 'h3', 'timestamp', 'ordinal'])
        return df_duck_nearest_edges
    


    def get_time_slot(self, start_time:datetime) -> TimeSlot:
        dt = start_time.astimezone(ZoneInfo(self.tz))
        slot = dt.strftime("%H:%M")
        if slot >= "05:00" and slot <= "09:59":
            return TimeSlot.S_05_10
        if slot >= "10:00" and slot <= "14:59":
            return TimeSlot.S_10_15
        if slot >= "15:00" and slot <= "19:59":
            return TimeSlot.S_15_20
        return TimeSlot.S_20_05


    def convert_campaign_track_group_info(self, df_tracks_info:pd.DataFrame, df_duck_nearest_edges:pd.DataFrame) -> pd.DataFrame:
        tracks = []

        # Precalcola h3 di inizio (ordinal=0) e h3 di fine (ultimo ordinal) per ciascun track_id
        if df_duck_nearest_edges is not None and not df_duck_nearest_edges.empty:
            df_edges_sorted = df_duck_nearest_edges.sort_values(['track_id', 'ordinal'])
            h3_bounds = df_edges_sorted.groupby('track_id', as_index=False).agg(
                h3_start=('h3', 'first'),
                h3_end=('h3', 'last')
            )
            h3_start_map = dict(zip(h3_bounds['track_id'], h3_bounds['h3_start']))
            h3_end_map = dict(zip(h3_bounds['track_id'], h3_bounds['h3_end']))
        else:
            h3_start_map = {}
            h3_end_map = {}

        for _, row in df_tracks_info.iterrows():
            track_id = row['track_id']
            player_id = row['player_id']
            campaign_id = row['campaign_id']
            campaign_type = row['campaign_type']
            start_time = row['start_time']
            end_time = row['end_time']
            mode = row['mode']
            duration = row['duration']
            distance = row['distance']
            group_id = row['group_id']
            multimodal_id = row['multimodal_id']
            time_slot = self.get_time_slot(start_time)

            # ottieni h3 iniziale e finale per questa traccia (se presenti)
            h3_start = h3_start_map.get(track_id)
            h3_end = h3_end_map.get(track_id)

            tracks.append((
                campaign_id, campaign_type, player_id, group_id, track_id, multimodal_id, mode,
                start_time, end_time, duration, distance, time_slot.value, h3_start, h3_end
            ))

        df_duck_tracks_info = pd.DataFrame(
            tracks,
            columns=[
                'campaign_id', 'campaign_type', 'player_id', 'group_id', 'track_id',
                'multimodal_id', 'mode', 'start_time', 'end_time', 'duration', 'distance',
                'time_slot', 'h3_start', 'h3_end'
            ]
        )
        return df_duck_tracks_info


    def convert_campaign_data(self, campaign_id:str, h3_level:int, df_tracks_info:pd.DataFrame, df_nearest_edges:pd.DataFrame) -> pd.DataFrame:
        if (df_tracks_info is None) or (df_nearest_edges is None):
            return None, None
        # from df_tracks_info get all track_id that belong to the campaign_id
        df_tracks_info = df_tracks_info[df_tracks_info['campaign_id'] == campaign_id]
        # from df_duck_nearest_edges get all rows that belong to the track_id in df_tracks_info
        track_ids = df_tracks_info['track_id'].unique().tolist()
        df_nearest_edges = df_nearest_edges[df_nearest_edges['track_id'].isin(track_ids)]

        df_duck_nearest_edges = self.convert_nearest_edges(df_nearest_edges.copy(), h3_level)

        # Precalcola h3 di inizio (ordinal=0) e h3 di fine (ultimo ordinal) per ciascun track_id
        df_edges_sorted = df_duck_nearest_edges.sort_values(['track_id', 'ordinal'])
        h3_bounds = df_edges_sorted.groupby('track_id', as_index=False).agg(
            h3_start=('h3', 'first'),
            h3_end=('h3', 'last')
        )
        h3_start_map = dict(zip(h3_bounds['track_id'], h3_bounds['h3_start']))
        h3_end_map = dict(zip(h3_bounds['track_id'], h3_bounds['h3_end']))

        tracks = []
        for _, row in df_tracks_info.iterrows():
            track_id = row['track_id']
            player_id = row['player_id']
            campaign_id = row['campaign_id']
            campaign_type = row['campaign_type']
            start_time = row['start_time']
            end_time = row['end_time']
            mode = row['mode']
            duration = row['duration']
            distance = row['distance']
            group_id = row['group_id']
            multimodal_id = row['multimodal_id']
            time_slot = self.get_time_slot(start_time)

            # ottieni h3 iniziale e finale per questa traccia (se presenti)
            h3_start = h3_start_map.get(track_id)
            h3_end = h3_end_map.get(track_id)

            tracks.append((
                campaign_id, campaign_type, player_id, group_id, track_id, multimodal_id, mode,
                start_time, end_time, duration, distance, time_slot.value, h3_start, h3_end
            ))

        df_duck_tracks_info = pd.DataFrame(
            tracks,
            columns=[
                'campaign_id', 'campaign_type', 'player_id', 'group_id', 'track_id',
                'multimodal_id', 'mode', 'start_time', 'end_time', 'duration', 'distance',
                'time_slot', 'h3_start', 'h3_end'
            ]
        )

        return df_duck_nearest_edges, df_duck_tracks_info





