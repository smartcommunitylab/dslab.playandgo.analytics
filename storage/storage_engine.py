import os
import pandas as pd
import csv
import logging

logger = logging.getLogger(__name__)

class FileStorage:
    def __init__(self):
        self.store_path = os.getenv("STORAGE_PATH", "./files/")
        if self.store_path.endswith("/") or self.store_path.endswith("\\"):
            self.store_path = self.store_path[:-1]
        self.campaign_tracks = "campaign_tracks"
        self.campaign_groups = "campaign_groups"
        self.campaign_tracks_info = "campaign_tracks_info"
        self.tracks = "tracks"
        self.tracks_info = "tracks_info"
        self.nearest_edges = "nearest_edges"
        self.way_shapes = "way_shapes"
        self.h3_info = "h3_info"
        self.mapped_campaign_groups = "mapped_campaign_groups"
        self.duck_nearest_edges = "duck_nearest_edges"
        self.duck_tracks_info = "duck_tracks_info"


    def check_directory(self, territory_id:str):
        """Check if the directory exists."""
        directory_path = self.store_path + "/" + territory_id
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"Created directory: {directory_path}")


    def get_filename(self, territory_id:str, df_file:str, year:str=None) -> str: 
        """Get the filename for a dataframe."""
        if not year is None:
            return f"{self.store_path}/{territory_id}/{year}_{df_file}.parquet"         
        else:
            return f"{self.store_path}/{territory_id}/{df_file}.parquet"


    def get_campaign_analysis_filename(self, territory_id:str, campaign_id:str, group_id:str=None) -> str:
        """Get the filename for a campaign analysis."""
        if group_id is None:
            return f"{self.store_path}/{territory_id}/analysis_{campaign_id}.parquet"         
        else:
            return f"{self.store_path}/{territory_id}/analysis_{campaign_id}_{group_id}.parquet"


    def merge_campaign_tracks(self, territory_id:str, year:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.campaign_tracks, year)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['territory_id','player_id','track_id', 'campaign_id'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            logger.info(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_tracks(self, territory_id:str, year:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.tracks, year)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['track_id'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            logger.info(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_tracks_info(self, territory_id:str, year:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.tracks_info, year)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['player_id', 'track_id'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            logger.info(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_nearest_edges(self, territory_id:str, year:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.nearest_edges, year)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['track_id', 'ordinal'])
            combined_df.to_parquet(file_path, engine="pyarrow")
            if save_csv:
                self.save_csv(file_path, combined_df) 
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            logger.info(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_way_shapes(self, territory_id:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.way_shapes)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['way_id'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            logger.info(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_campaign_groups(self, territory_id:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.campaign_groups)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['territory_id', 'player_id', 'campaign_id'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            logger.info(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_campaign_tracks_info(self, territory_id:str, year:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.campaign_tracks_info, year)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['territory_id','player_id','track_id', 'campaign_id'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            logger.info(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)

    def merge_mapped_campaign_groups(self, territory_id:str, year:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.mapped_campaign_groups, year)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['territory_id','player_id','track_id', 'campaign_id'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            logger.info(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_df_campaign_tracks_groups_by_campaign(self, territory_id:str, year:str, campaign_id:str):
        """Merge data to a file."""
        self.check_directory(territory_id)
        try:
            s, df_campaign_tracks = self.load_dataframe(territory_id, self.campaign_tracks, year)
            logger.info(f"Campaign Tracks Rows: {df_campaign_tracks.shape[0]}")
            s, df_campaign_groups = self.load_dataframe(territory_id, self.campaign_groups)
            logger.info(f"Campaign Groups Rows: {df_campaign_groups.shape[0]}")
            #s, df_campaign_tracks_info = self.load_dataframe(territory_id, self.campaign_tracks_info, year)
            #logger.info(f"Campaign Tracks Info Rows: {df_campaign_tracks_info.shape[0]}")
            s, df_tracks_info = self.load_dataframe(territory_id, self.tracks_info, year)
            logger.info(f"Tracks Info Rows: {df_tracks_info.shape[0]}")
            # Rimuove le colonne mode e start_time da df_tracks_info
            df_tracks_info = df_tracks_info.drop(columns=['mode', 'start_time'], errors='ignore')

            # Filtra per campaign_id
            df_campaign_tracks = df_campaign_tracks[df_campaign_tracks['campaign_id'] == campaign_id]
            logger.info(f"Filtered Campaign Tracks Rows: {df_campaign_tracks.shape[0]}")
            df_campaign_groups = df_campaign_groups[df_campaign_groups['campaign_id'] == campaign_id]
            logger.info(f"Filtered Campaign Groups Rows: {df_campaign_groups.shape[0]}")
            #df_campaign_tracks_info = df_campaign_tracks_info[df_campaign_tracks_info['campaign_id'] == campaign_id]
            #logger.info(f"Filtered Campaign Tracks Info Rows: {df_campaign_tracks_info.shape[0]}")

            # Trova i player_id presenti in df_campaign_subscriptions
            #player_ids_subs = df_campaign_subscriptions['player_id'].unique()
            # Seleziona le righe di df_campaign_tracks con player_id non presente in df_campaign_subscriptions
            #tracks_not_in_subs = df_campaign_tracks[~df_campaign_tracks['player_id'].isin(player_ids_subs)]
            #logger.info(tracks_not_in_subs)

            # Aggiunge group_id con merge sulle colonne territory_id, player_id, campaign_id
            df_merged = pd.merge(
                df_campaign_tracks,
                df_campaign_groups,
                on=['territory_id', 'player_id', 'campaign_id'],
                how='left'
            )  
            # Aggiunge multimodal_id con merge sulle colonne player_id e track_id
            df_merged = pd.merge(
                df_merged,
                df_tracks_info,
                on=['player_id', 'track_id'],
                how='left'
            )
            # Aggiunge way_back e location_id con merge sulle colonne territory_id, track_id, player_id, campaign_id
            #df_merged = pd.merge(
            #    df_merged,
            #    df_campaign_tracks_info,
            #    on=['territory_id', 'track_id', 'player_id', 'campaign_id'],
            #    how='left'
            #)
            return df_merged
        except FileNotFoundError:
            raise FileNotFoundError(f"Required files for merging mapped edges not found for territory {territory_id} and year {year}.")


    def merge_dataframes(self, df_old:pd.DataFrame, df_new:pd.DataFrame, columns_sub) -> pd.DataFrame:
        """Merge two dataframes."""
        combined_df = pd.concat([df_old, df_new], ignore_index=True) \
            .drop_duplicates(subset=columns_sub, keep='last') \
            .reset_index(drop=True)
        rows, columns = combined_df.shape
        logger.info(f"Storage Rows: {rows}, Columns: {columns}")
        return combined_df
    

    def save_df(self, territory_id:str, df_file:str, df:pd.DataFrame, year:str=None, save_csv:bool=False):
        file_path = self.get_filename(territory_id, df_file, year)
        df.to_parquet(file_path, engine="pyarrow") 
        if save_csv:
            self.save_csv(file_path, df)


    def save_csv(self, orig_path:str, df:pd.DataFrame):
        """Save data to a file."""
        file_path = orig_path.replace(".parquet", ".csv")
        df.to_csv(file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)                   


    def load_dataframe(self, territory_id:str, df_file:str, year:str=None) -> tuple:
        """Load a dataframe from a file."""
        file_path = self.get_filename(territory_id, df_file, year)
        if os.path.exists(file_path):
            file_stats = os.stat(file_path)
            return file_stats.st_size, pd.read_parquet(file_path, engine="pyarrow")
        else:
            raise FileNotFoundError(f"File {file_path} does not exist.")


    def load_multiple_dataframes(self, territory_id:str, df_file:str, years:list) -> pd.DataFrame:
        """Load multiple dataframes from files."""
        df_result = None
        for year in years:
            s, df = self.load_dataframe(territory_id, df_file, year)
            # append dataframe to df_result
            if df_result is None:
                df_result = df
            else:
                df_result = pd.concat([df_result, df], ignore_index=True)
        return df_result
    

    def concat_dfs(self, df_list:list) -> pd.DataFrame:
        """Concatenate a list of dataframes."""
        df_result = None
        for df in df_list:
            if df_result is None:
                df_result = df
            else:
                df_result = pd.concat([df_result, df], ignore_index=True)
        return df_result