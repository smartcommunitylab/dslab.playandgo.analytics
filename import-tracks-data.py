from playandgo.pg_engine import PlayAndGoEngine
from valhalla.valhalla_engine import ValhallaEngine


def find_nearest_edges_by_locate(territory_id, start_time):
    nearest_edges = []
    for track in playandgo_engine.get_tracks(territory_id, start_time):
        track_id = track["_id"]
        for edge_info in valhalla_engine.find_nearest_edges_by_locate(track, track_id):
            if not edge_info in nearest_edges:
                nearest_edges.append(edge_info)
    print(f"Found {len(nearest_edges)} unique edges.")


def find_nearest_edges_by_trace(territory_id, start_time):
    nearest_edges = []
    for track in playandgo_engine.get_tracks(territory_id, start_time):
        track_id = track["_id"]
        trace_route = valhalla_engine.find_nearest_edges_by_trace(track, track_id) 
        for trace_info in trace_route.trace_infos:
            if not trace_info.way_id in nearest_edges:
                nearest_edges.append(trace_info.way_id)
    print(f"Found {len(nearest_edges)} unique edges.")


if __name__ == "__main__":
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()

    # Ottieni i dati da PlayAndGo
    territory_id = "TN"
    start_time = "2025-03-01T00:00:00+00:00"

    #for c_track in playandgo_engine.get_campaign_tracks(territory_id, start_time):
    #    print(f"Campaign Track: {c_track}")

    #find_nearest_edges_by_locate(territory_id, start_time)
    find_nearest_edges_by_trace(territory_id, start_time)
