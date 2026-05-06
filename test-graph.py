import logging
from datetime import datetime, timedelta
import pytz
from graph.graphmap import GraphMap

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')

if __name__ == "__main__":
    territory_id = "TN"
    track_mode = "car"
    graph_map = GraphMap()
    graph_map.load_graph_from_bbox(territory_id, track_mode)
    print(graph_map.G)

    DEFAULT_START_TIME = "2026-05-05T00:00:00+00:00"
    DEFAULT_END_TIME = "2026-05-31T23:59:59+00:00"
    period = 7 * 24 * 60 * 60 # 7 giorni in secondi
    start_time_dt = datetime.fromisoformat(DEFAULT_START_TIME)
    end_time_df = datetime.fromisoformat(DEFAULT_END_TIME)
    new_start_time = end_time_df.astimezone(pytz.utc).isoformat()
    print(new_start_time)
    new_end_time = (end_time_df + timedelta(seconds=period)).astimezone(pytz.utc).isoformat()
    print(new_end_time)
