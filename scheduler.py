from datetime import datetime, timedelta
import pytz
import logging
import argparse 

from import_tracks_data import import_campaign_tracks_data, import_campaign_groups_data, import_nearest_edges_by_trace 
from import_tracks_data import merge_campaign_tracks_groups, import_campaigns_data
from import_tracks_data import get_territories, get_campaigns, get_utc_datetime
from import_duckdb_data import import_duckdb_data
from config_manager import get_time_range_for_territory, add_or_update_time_range

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')

# Valori di default qualora non trovati nel file di configurazione
DEFAULT_START_TIME = "2026-01-01T00:00:00+00:00"
DEFAULT_END_TIME = "2026-05-31T23:59:59+00:00"


def import_campaign_data_for_territory(territory, period: int):
    territory_id = str(territory['_id'])

    track_modes = []
    if territory['territoryData'] is not None and 'means' in territory['territoryData']:
        track_modes = territory['territoryData']['means']

    """
    Importa i dati delle campagne per un territorio.
    Usa i range temporali dal file di configurazione se disponibili.
    """
    # Ottieni i range temporali per il territorio
    time_range = get_time_range_for_territory(territory_id)
    if time_range:
        start_time = time_range['start_time']
        end_time = time_range['end_time']
    else:
        # Usa i valori di default se non trovati
        start_time = DEFAULT_START_TIME
        end_time = DEFAULT_END_TIME
        logging.warning(f"Range temporale non trovato per il territorio '{territory_id}'. Uso valori di default.")
    
    logging.info(f"Importa dati per il territorio '{territory_id}' dal {start_time} al {end_time}")
    
    # Importa i dati delle campagne
    import_campaigns_data(territory_id, save_csv=False)

    import_campaign_tracks_data(territory_id, start_time=start_time, end_time=end_time, save_csv=False)
    
    import_campaign_groups_data(territory_id, save_csv=False)
    
    import_nearest_edges_by_trace(territory_id, start_time=start_time, end_time=end_time, track_modes=track_modes, save_csv=False)

    start_time_dt = datetime.fromisoformat(start_time)
    end_time_df = datetime.fromisoformat(end_time)
    year = start_time_dt.strftime("%Y")

    for campaign in get_campaigns(territory_id):
        campaign_id = str(campaign['_id'])
        
        if campaign["type"] == "personal":
            logging.info(f"Skipping personal campaign '{campaign_id}'")
            continue
        
        # if campaign is not personal, check if start_time is between the campaign date
        end_campaign = get_utc_datetime(campaign['dateTo'])
        if start_time_dt > end_campaign:
            logging.info(f"Campaign '{campaign_id}' is outside the time range.")
            continue

        # check campaign type
        set_group_id = False
        if (campaign["type"] == "company")  or (campaign["type"] == "school") or (campaign["type"] == "group"):
            set_group_id = True

        merge_campaign_tracks_groups(territory_id, year, campaign_id, set_group_id=set_group_id, set_campaign_info=False)

        import_duckdb_data(territory_id, campaign_id)

    # incrementa il time range del territorio con il periodo
    new_start_time = end_time_df.astimezone(pytz.utc).isoformat()
    # add period in seconds to ned_time_df
    new_end_time = (end_time_df + timedelta(seconds=period)).astimezone(pytz.utc).isoformat()
    add_or_update_time_range(territory_id, new_start_time, new_end_time)
     

if __name__ == "__main__":
    # legge da linea di comando il periodo di scheduling in secondi, se non specificato usa 30 giorni
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", type=int, default=60 * 60 * 24 * 30, help="Periodo di scheduling in secondi")
    # legge da linea di comando i territori da processare, se non specificati processa tutti i territori
    parser.add_argument("--territories", nargs="*", help="Lista dei territori da processare")
    args = parser.parse_args()
    period = args.period
    territories_db =[]
    if args.territories:
        for territory_id in args.territories:
            territory = next((t for t in get_territories() if str(t['_id']) == territory_id), None)
            if territory:
                territories_db.append(territory)
    else: 
        territories_db = get_territories()
    for territory in territories_db:
        import_campaign_data_for_territory(territory, period)
