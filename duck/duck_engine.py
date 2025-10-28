from typing import List
import os
import duckdb
import pandas as pd
import h3
from pathlib import Path

class DuckEngine:
    def __init__(self, territory_ids:List[str]):
        self.store_path = os.getenv("STORAGE_PATH", "./files/")
        if self.store_path.endswith("/") or self.store_path.endswith("\\"):
            self.store_path = self.store_path[:-1]
        self.database_path = Path(f"{self.store_path}/duckdb_database.duckdb").absolute()
        self.conn = duckdb.connect(self.database_path, read_only=True)

        # Crea una lista di Path assoluti per ogni territory_id
        self.search_paths = [Path(f"{self.store_path}/{tid}").absolute() for tid in territory_ids]
        # Stringa concatenata separata da virgola come richiesto
        self.search_path = ",".join(str(p) for p in self.search_paths)

        self.conn.execute(f"SET file_search_path ='{self.search_path}'")
        self.table_nearest_edges = "nearest_edges"
        self.table_track_info = "track_info"


    def execute_query(self, query):
        return self.conn.execute(query).fetchall()


    def close(self):
        self.conn.close()


    def import_parquet(self, table_name:str, file_pattern:str):
        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{file_pattern}')")


    def convert_nearest_edges(self, territory_id, df_nearest_edges, h3_level:int) -> pd.DataFrame: 
        # df_nearest_edges columns=['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
        # Crea una nuova colonna con l'H3 parent
        df_nearest_edges['h3_parent'] = df_nearest_edges['h3'].apply(lambda x: h3.cell_to_parent(x, h3_level))
        edges = []
        last_track_id = ""
        for row in df_nearest_edges.itertuples():
            track_id = row.track_id
            timestamp = row.timestamp
            h3_id = row.h3_parent
            if track_id != last_track_id:
                last_track_id = track_id
                last_h3_id = 0
                index = 0
            if h3_id != last_h3_id:
                last_h3_id = h3_id
                edges.append((territory_id, track_id, h3_id, timestamp, index))
                index += 1
        
        df_duck_nearest_edges = pd.DataFrame(edges, columns=['territory_id', 'track_id', 'h3', 'timestamp', 'ordinal'])
        return df_duck_nearest_edges
    

    def convert_campaign_track_group_info(self, territory_id, df_tracks_info) -> pd.DataFrame:
        tracks = []
        for row in df_tracks_info.itertuples():
            track_id = row.track_id
            player_id = row.player_id
            campaign_id = row.campaign_id
            campaign_type = row.campaign_type
            start_time = row.start_time
            end_time = row.end_time
            mode = row.mode
            duration = row.duration
            distance = row.distance
            group_id = row.group_id
            multimodal_id = row.multimodal_id
            tracks.append((territory_id, campaign_id, campaign_type, player_id, group_id, track_id, multimodal_id, mode, start_time, end_time, duration, distance))

        df_duck_tracks_info = pd.DataFrame(tracks, columns=['territory_id', 'campaign_id', 'campaign_type', 'player_id', 'group_id', 'track_id', 'multimodal_id', 'mode', 'start_time', 'end_time', 'duration', 'distance'])
        return df_duck_tracks_info


