import logging

from duck.duck_engine import DuckEngine
from storage.storage_engine import FileStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
        territory_id = 'L'
        years = ['2023', '2024', '2025']
        campaign_id = 'L.personal'
        h3_level = 10
        storage_engine = FileStorage()
        df_total_edges = storage_engine.load_multiple_dataframes(territory_id, storage_engine.nearest_edges, years)
        df_campaign_tracks = storage_engine.load_multiple_dataframes(territory_id, storage_engine.mapped_campaign_groups, years)
        duck_engine = DuckEngine(territory_id, campaign_id, False)
        df_duck_nearest_edges, df_duck_tracks_info, df_duck_multimodal = duck_engine.convert_campaign_data(
            campaign_id, h3_level, df_campaign_tracks, df_total_edges)
        duck_engine.import_dataframe(duck_engine.table_nearest_edges, df_duck_nearest_edges)
        duck_engine.import_dataframe(duck_engine.table_track_info, df_duck_tracks_info)
        duck_engine.import_dataframe(duck_engine.table_trip_info, df_duck_multimodal)
        duck_engine.close()
