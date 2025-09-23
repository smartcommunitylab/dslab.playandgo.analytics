import os
import pandas as pd

class FileStorage:
    def __init__(self):
        self.store_path = os.getenv("STORAGE_PATH", "./files/")
        if self.store_path.endswith("/") or self.store_path.endswith("\\"):
            self.store_path = self.store_path[:-1]
        self.campaign_tracks = "campaign_tracks"
        self.campaign_subscriptions = "campaign_subscriptions"
        self.tracks = "tracks"
        self.tracks_info = "tracks_info"
        self.nearest_edges = "nearest_edges"
        self.way_shapes = "way_shapes"
        self.h3_info = "h3_info"
        self.mapped_edges = "mapped_edges"
        self.mapped_campaign_tracks = "mapped_campaign_tracks"


    def check_directory(self, territory_id:str):
        """Check if the directory exists."""
        directory_path = self.store_path + "/" + territory_id
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"Created directory: {directory_path}")


    def get_filename(self, territory_id:str, df_file:str, year:str=None) -> str: 
        """Get the filename for a dataframe."""
        if not year is None:
            return f"{self.store_path}/{territory_id}/{year}_{df_file}.parquet"         
        else:
            return f"{self.store_path}/{territory_id}/{df_file}.parquet"


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
            print(f"Storage Rows: {rows}, Columns: {columns}")
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
            print(f"Storage Rows: {rows}, Columns: {columns}")
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
            print(f"Storage Rows: {rows}, Columns: {columns}")
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
            print(f"Storage Rows: {rows}, Columns: {columns}")
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
            print(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_campaign_subscriptions(self, territory_id:str, year:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.campaign_subscriptions, year)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['territory_id', 'player_id', 'campaign_id'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            print(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_h3_info(self, territory_id:str, year:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.get_filename(territory_id, self.h3_info, year)
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['track_id', 'ordinal'])
            combined_df.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, combined_df)
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            print(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_mapped_edges(self, territory_id:str, year:str, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        try:
            s, df_nearest_edges = self.load_dataframe(territory_id, self.nearest_edges, year)
            s, df_h3_info = self.load_dataframe(territory_id, self.h3_info, year)
            # Merge dei due dataframe sulle colonne 'track_id' e 'ordinal'
            df_merged = pd.merge(df_h3_info, df_nearest_edges, on=['track_id', 'ordinal'], how='inner')
            file_path = self.get_filename(territory_id, self.mapped_edges, year)
            df_merged.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, df_merged)
        except FileNotFoundError:
            raise FileNotFoundError(f"Required files for merging mapped edges not found for territory {territory_id} and year {year}.")


    def merge_campaign_tracks(self, territory_id:str, year:str, campaign_id:str, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        try:
            s, df_campaign_tracks = self.load_dataframe(territory_id, self.campaign_tracks, year)
            print(f"Campaign Tracks Rows: {df_campaign_tracks.shape[0]}")
            s, df_campaign_subscriptions = self.load_dataframe(territory_id, self.campaign_subscriptions, year)
            print(f"Campaign Subscriptions Rows: {df_campaign_subscriptions.shape[0]}")

            # Filtra per campaign_id
            df_campaign_tracks = df_campaign_tracks[df_campaign_tracks['campaign_id'] == campaign_id]
            print(f"Filtered Campaign Tracks Rows: {df_campaign_tracks.shape[0]}")
            df_campaign_subscriptions = df_campaign_subscriptions[df_campaign_subscriptions['campaign_id'] == campaign_id]
            print(f"Filtered Campaign Subscriptions Rows: {df_campaign_subscriptions.shape[0]}")

            # Trova i player_id presenti in df_campaign_subscriptions
            #player_ids_subs = df_campaign_subscriptions['player_id'].unique()
            # Seleziona le righe di df_campaign_tracks con player_id non presente in df_campaign_subscriptions
            #tracks_not_in_subs = df_campaign_tracks[~df_campaign_tracks['player_id'].isin(player_ids_subs)]
            #print(tracks_not_in_subs)

            # Seleziona solo le colonne chiave + group_id dal secondo dataframe
            df_campaign_subscriptions = df_campaign_subscriptions[['territory_id', 'player_id', 'campaign_id', 'group_id']]

            # Merge sulle colonne territory_id, player_id, campaign_id
            df_merged = pd.merge(
                df_campaign_tracks,
                df_campaign_subscriptions,
                on=['territory_id', 'player_id', 'campaign_id'],
                how='inner'
            )  

            file_path = self.get_filename(territory_id, self.mapped_campaign_tracks, year)
            df_merged.to_parquet(file_path, engine="pyarrow") 
            if save_csv:
                self.save_csv(file_path, df_merged)
        except FileNotFoundError:
            raise FileNotFoundError(f"Required files for merging mapped edges not found for territory {territory_id} and year {year}.")


    def merge_dataframes(self, df_old:pd.DataFrame, df_new:pd.DataFrame, columns_sub) -> pd.DataFrame:
        """Merge two dataframes."""
        combined_df = pd.concat([df_old, df_new], ignore_index=True) \
            .drop_duplicates(subset=columns_sub, keep='last') \
            .reset_index(drop=True)
        rows, columns = combined_df.shape
        print(f"Storage Rows: {rows}, Columns: {columns}")
        return combined_df
    

    def save_csv(self, orig_path:str, df:pd.DataFrame):
        """Save data to a file."""
        file_path = orig_path.replace(".parquet", ".csv")
        df.to_csv(file_path, index=False)                   


    def load_dataframe(self, territory_id:str, df_file:str, year:str=None) -> tuple:
        """Load a dataframe from a file."""
        file_path = self.get_filename(territory_id, df_file, year)
        if os.path.exists(file_path):
            file_stats = os.stat(file_path)
            return file_stats.st_size, pd.read_parquet(file_path, engine="pyarrow")
        else:
            raise FileNotFoundError(f"File {file_path} does not exist.")
