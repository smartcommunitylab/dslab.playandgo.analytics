from playandgo.pg_engine import PlayAndGoEngine
from valhalla.valhalla_engine import ValhallaEngine

if __name__ == "__main__":
    # Inizializza gli engine
    playandgo_engine = PlayAndGoEngine()
    valhalla_engine = ValhallaEngine()

    # Ottieni i dati da PlayAndGo
    territoryId = "TAA"
    startTime = "2025-03-01T00:00:00+00:00"

    # Itera sui dati e processali con Valhalla
    nearest_edges = []
    for track in playandgo_engine.getTracks(territoryId, startTime):
        track_id = track["_id"]
        for edge_info in valhalla_engine.find_nearest_edges(track, track_id):
            if not edge_info in nearest_edges:
                nearest_edges.append(edge_info)
    print(f"Found {len(nearest_edges)} unique edges.")
