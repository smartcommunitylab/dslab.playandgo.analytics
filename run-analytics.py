import logging

from datetime import datetime

from playandgo.pg_engine import PlayAndGoEngine
from storage.storage_engine import FileStorage
from analytics.analytics import generate_report

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')
logger = logging.getLogger(__name__)


def analyze_territory(playandgo_engine: PlayAndGoEngine, file_storage: FileStorage, territory_id):
    logger.info(f"Starting analytics {territory_id}")
    # get today date
    today = datetime.now()
    # get play&go campaigns
    campaigns = playandgo_engine.get_campaigns(territory_id)
    for campaign in campaigns:
        campaign_id = str(campaign["_id"])
        # skip personal campaigns
        if campaign["type"] == "personal":
            logger.info(f"Skipping personal campaign {campaign_id}")
            continue
        # check if campaign ends at least one week before today 
        if (today - campaign["dateTo"]).days >= 7:
            logger.info(f"Skipping campaign {campaign_id} that ends on {campaign['dateTo']}")
            continue
        yearFrom = campaign["dateFrom"].strftime("%Y")
        yearTo = campaign["dateTo"].strftime("%Y")
        # make a list of unique years
        years = list(set([yearFrom, yearTo]))
        # order years
        years.sort()
        df_campaign_info_total = None
        df_edges_total = None
        for year in years:
            logger.info(f"Processing campaign {campaign_id} year {year}")
            try:
                # merge campaign tracks groups
                df_campaign_info = file_storage.merge_df_campaign_tracks_groups_by_campaign(territory_id, year, campaign_id)
                df_campaign_info_total = file_storage.concat_dfs([df_campaign_info_total, df_campaign_info])
                s, df_edges = file_storage.load_dataframe(territory_id, file_storage.nearest_edges, year)
                df_edges_total = file_storage.concat_dfs([df_edges_total, df_edges])
            except Exception as e:
                logger.warning(f"Error processing data {campaign_id}: {e}")        
        rows, columns = df_campaign_info_total.shape
        logger.info(f"Merged Campaign Groups Rows: {rows}, Columns: {columns}")
        rows, columns = df_edges_total.shape
        logger.info(f"Loaded Nearest Edges Rows: {rows}, Columns: {columns}")
        # TODO perform analytics
        generate_report(territory_id, campaign_id, df_campaign_info_total, df_edges_total, file_storage)


if __name__ == "__main__":
    playandgo_engine = PlayAndGoEngine()
    file_storage = FileStorage()
    analyze_territory(playandgo_engine, file_storage, "Ferrara")

