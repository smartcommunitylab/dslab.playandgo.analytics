import logging

from duck.duck_engine import DuckEngine
from storage.storage_engine import FileStorage
from playandgo.pg_engine import PlayAndGoEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')
logger = logging.getLogger(__name__)

h3_level = 10
default_years = ["2022","2023", "2025"]

if __name__ == "__main__": 
    territory_ids = ["Ferrara"]

    storage_engine = FileStorage()
    playandgo_engine = PlayAndGoEngine()
    
    for territory_id in territory_ids:
        # get campaigns from PlayAndGo
        campaigns = playandgo_engine.get_campaigns(territory_id)
        # for each campaign 
        for campaign in campaigns:
            campaign_id = str(campaign['_id'])
            # get starting and endig years
            years = []
            if(campaign['type'] == "personal"):
                years = default_years
            else:
                start_year = campaign['dateFrom'].year
                end_year = campaign['dateTo'].year
                # get a list for years to process
                for year in range(start_year, end_year + 1):
                    year_str = str(year)
                    if year_str not in years:
                        years.append(year_str)

            # nearest edges
            df_total_edges = storage_engine.load_multiple_dataframes(territory_id, storage_engine.nearest_edges, years)
            df_campaign_tracks = storage_engine.load_multiple_dataframes(territory_id, storage_engine.mapped_campaign_groups, years)
            duck_engine = DuckEngine(territory_id, campaign_id, False)
            df_duck_nearest_edges, df_duck_tracks_info = duck_engine.convert_campaign_data(
                campaign_id, h3_level, df_campaign_tracks, df_total_edges
            )
            duck_engine.import_dataframe(duck_engine.table_nearest_edges, df_duck_nearest_edges)
            duck_engine.import_dataframe(duck_engine.table_track_info, df_duck_tracks_info)
            duck_engine.close()





