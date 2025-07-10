from psycopg.psyco_engine import PsycoEngine
from storage.storage_engine import FileStorage

if __name__ == "__main__":    
    engine = PsycoEngine()
    
    # Initialize the database tables
    engine.init_tables()
    print("Database tables initialized successfully.")

    territory_id = "L"
    file_storage = FileStorage()
    
    #size, df_tracks_info = file_storage.load_dataframe(territory_id, file_storage.tracks_info, "2025")  
    # Import track information into the database
    #engine.import_track_info(territory_id, df_tracks_info)
    #print("Track information imported successfully.")

    # Import nearest edges into the database
    #size, df_nearest_edges = file_storage.load_dataframe(territory_id, file_storage.nearest_edges, "2025")
    #size, df_h3_info = file_storage.load_dataframe(territory_id, file_storage.h3_info, "2025")
    #engine.import_nearest_edges(territory_id, df_nearest_edges, df_h3_info, 8)
    #print("Track Edges imported successfully.")

    size, df_campaign_tracks = file_storage.load_dataframe(territory_id, file_storage.campaign_tracks, "2025")
    engine.import_campaign_tracks(territory_id, df_campaign_tracks)
    print("Campaign Tracks imported successfully.")    