from psycopg.psyco_engine import PsycoEngine
from storage.storage_engine import FileStorage

if __name__ == "__main__":    
    engine = PsycoEngine()
    
    # Initialize the database tables
    engine.init_tables()
    print("Database tables initialized successfully.")

    territory_id = "Ferrara"
    file_storage = FileStorage()
    size, df_tracks_info = file_storage.load_dataframe(territory_id, file_storage.tracks_info, "2025")
    rows, columns = df_tracks_info.shape
    if rows > 0:
        print(f"Loaded {rows} rows of track information for territory {territory_id}.")
        
        # Import track information into the database
        engine.import_track_info(territory_id, df_tracks_info)
        print("Track information imported successfully.")
    else:
        print(f"No track information found for territory {territory_id}.")