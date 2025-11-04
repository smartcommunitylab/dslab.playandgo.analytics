import logging

from duck.duck_engine import DuckEngine
from storage.storage_engine import FileStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')
logger = logging.getLogger(__name__)

h3_level = 9
years = ["2023","2024","2025"]

if __name__ == "__main__": 
    territory_ids = ["L"]
    # create a map with <territory_id, DuckEngine>
    engine_map = {}
    for territory_id in territory_ids:
        duck_engine = DuckEngine(territory_id, False)
        engine_map[territory_id] = duck_engine

    storage_engine = FileStorage()
    
    for year in years:
        # nearest edges
        for territory_id in territory_ids:
            size, df = storage_engine.load_dataframe(territory_id, storage_engine.nearest_edges, year)
            duck_df = duck_engine.convert_nearest_edges(df, h3_level)
            storage_engine.save_df(territory_id, storage_engine.duck_nearest_edges, duck_df, year, False)
            rows, columns = duck_df.shape
            logger.info(f"Imported {territory_id} Nearest Edges Rows: {rows}, Columns: {columns}")    
            duck_engine = engine_map[territory_id]
            duck_engine.import_parquet(duck_engine.table_nearest_edges, f"*{storage_engine.duck_nearest_edges}.parquet")
            logger.info(f"Imported Nearest Edges to DuckDB table: {duck_engine.table_nearest_edges}")

        # campaign track group info
        for territory_id in territory_ids:
            size, df = storage_engine.load_dataframe(territory_id, storage_engine.mapped_campaign_groups, year)
            duck_df = duck_engine.convert_campaign_track_group_info(df)
            storage_engine.save_df(territory_id, storage_engine.duck_tracks_info, duck_df, year, False)
            rows, columns = duck_df.shape
            logger.info(f"Imported {territory_id} Tracks Rows: {rows}, Columns: {columns}")  
            duck_engine = engine_map[territory_id]
            duck_engine.import_parquet(duck_engine.table_track_info, f"*{storage_engine.duck_tracks_info}.parquet")
            logger.info(f"Imported Tracks Info to DuckDB table: {duck_engine.table_track_info}")
    
    for territory_id, duck_engine in engine_map.items():
        duck_engine.close()




