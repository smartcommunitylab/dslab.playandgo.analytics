import os
import pandas as pd

class FileStorage:
    def __init__(self):
        self.store_path = os.getenv("STORAGE_PATH", "./files/").strip("/")


    def check_directory(self, territory_id:str):
        """Check if the directory exists."""
        directory_path = self.store_path + "/" + territory_id
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            

    def merge_campaign_tracks(self, territory_id:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.store_path + "/" + territory_id + "/campaign_tracks.parquet"
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


    def merge_tracks(self, territory_id:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.store_path + "/" + territory_id + "/tracks.parquet"
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


    def merge_nearest_edges(self, territory_id:str, df:pd.DataFrame, save_csv:bool=False):
        """Merge data to a file."""
        self.check_directory(territory_id)
        file_path = self.store_path + "/" + territory_id + "/nearest_edges.parquet"
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path, engine="pyarrow")
            combined_df = self.merge_dataframes(existing_df, df, ['track_id', 'way_id', 'lat', 'lon'])
            combined_df.to_parquet(file_path, engine="pyarrow")
            if save_csv:
                self.save_csv(file_path, combined_df) 
        else:   
            df.to_parquet(file_path, engine="pyarrow")                   
            rows, columns = df.shape
            print(f"Storage Rows: {rows}, Columns: {columns}")
            if save_csv:
                self.save_csv(file_path, df)


    def merge_dataframes(self, df1:pd.DataFrame, df2:pd.DataFrame, columns_sub) -> pd.DataFrame:
        """Merge two dataframes."""
        combined_df = pd.concat([df1, df2], ignore_index=True) \
            .drop_duplicates(subset=columns_sub) \
            .reset_index(drop=True)
        rows, columns = combined_df.shape
        print(f"Storage Rows: {rows}, Columns: {columns}")
        return combined_df
    

    def save_csv(self, orig_path:str, df:pd.DataFrame):
        """Save data to a file."""
        file_path = orig_path.replace(".parquet", ".csv")
        df.to_csv(file_path, index=False)                   
