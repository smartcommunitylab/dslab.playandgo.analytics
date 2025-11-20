from storage.storage_engine import FileStorage

if __name__ == "__main__":    
    storage_engine = FileStorage()
    # nearest edges
    #size, df = storage_engine.load_dataframe("Ferrara", storage_engine.nearest_edges, "2025")
    #file_path = storage_engine.get_filename("Ferrara", storage_engine.nearest_edges, "2025")
    #storage_engine.save_csv(file_path, df)
    # tracks info
    size, df = storage_engine.load_dataframe("Ferrara", storage_engine.tracks_info, "2025")
    file_path = storage_engine.get_filename("Ferrara", storage_engine.tracks_info, "2025")
    storage_engine.save_csv(file_path, df)
    # campaign tracks
    #size, df = storage_engine.load_dataframe("Ferrara", storage_engine.campaign_tracks, "2025")
    #file_path = storage_engine.get_filename("Ferrara", storage_engine.campaign_tracks, "2025")
    #storage_engine.save_csv(file_path, df)
    # campaign groups
    #size, df = storage_engine.load_dataframe("Ferrara", storage_engine.campaign_groups, "2025")
    #file_path = storage_engine.get_filename("Ferrara", storage_engine.campaign_groups, "2025")
    #storage_engine.save_csv(file_path, df)
