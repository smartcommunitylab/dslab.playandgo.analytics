import logging

from duck.duck_engine import DuckEngine
from storage.storage_engine import FileStorage

logger = logging.getLogger(__name__)

h3_level = 10

def import_duckdb_data(territory_id:str, filter_campaign_id:str=None):
    storage_engine = FileStorage()
    size, df_campaigns = storage_engine.load_dataframe(territory_id, storage_engine.campaigns)
    for _, row in df_campaigns.iterrows():
        campaign_id = row['campaign_id']
        if filter_campaign_id is not None:
            if campaign_id != filter_campaign_id:
                continue
        if(row['type'] == "personal"):
            continue
        years = []
        start_year = row['date_from'].year
        end_year = row['date_to'].year
        # get a list for years to process
        for year in range(start_year, end_year + 1):
            year_str = str(year)
            if year_str not in years:
                years.append(year_str)

        df_total_edges = storage_engine.load_multiple_dataframes(territory_id, storage_engine.nearest_edges, years)
        df_campaign_tracks = storage_engine.load_multiple_dataframes(territory_id, storage_engine.mapped_campaign_groups, years)
        duck_engine = DuckEngine(territory_id, campaign_id, False)
        df_duck_nearest_edges, df_duck_tracks_info, df_duck_multimodal = duck_engine.convert_campaign_data(
            campaign_id, h3_level, df_campaign_tracks, df_total_edges)
        duck_engine.import_dataframe(duck_engine.table_nearest_edges, df_duck_nearest_edges)
        duck_engine.import_dataframe(duck_engine.table_track_info, df_duck_tracks_info)
        duck_engine.import_dataframe(duck_engine.table_trip_info, df_duck_multimodal)
        duck_engine.close()
