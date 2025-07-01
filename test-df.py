import json

from valhalla.valhalla_engine import ValhallaEngine
from playandgo.pg_engine import PlayAndGoEngine

if __name__ == "__main__":    
    valhalla_engine = ValhallaEngine()
    playandgo_engine = PlayAndGoEngine()

    file_path = "./files/tracks/ferrara.json"
    with open(file_path, "r", encoding="utf-8") as file:
        nearest_edges = []
        for line in file:
            track_json = json.loads(line.strip())  # Converte la riga in un oggetto Python
            track_id = str(track_json["_id"])
            terriory_id = track_json["territoryId"]
            track = playandgo_engine.get_track(terriory_id, track_id)
            if not track is None:
                trace_route = valhalla_engine.find_nearest_edges_by_trace(track, track_id) 
                nearest_edges.append(trace_route.to_dict())
        
        # Salva tutto l'array su file JSON
        with open("./files/tracks/nearest_edges.json", "w", encoding="utf-8") as out_file:
            json.dump(nearest_edges, out_file, indent=2, ensure_ascii=False)        
            
