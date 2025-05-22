import os
import json
from datetime import datetime
import requests
from jinja2 import Template

class EndgeInfo:    
    """
    EdgeInfo class to handle edge information.
    """

    def __init__(self, way_id, shape):
        self.way_id = way_id
        self.shape = shape
    
    def __eq__(self, other):
        return other.way_id == self.way_id
    
    def __contains__(self, item):
        return item == self.way_id
    
    def __repr__(self):
        return f"EdgeInfo(way_id={self.way_id}, shape={self.shape})"


def get_transit_mode(track):
    """
    Get the transit mode from the track.
    """
    if "validationResult" in track and "valid" in track["validationResult"] and track["validationResult"]["valid"] is True:
        if "freeTrackingTransport" in track:
            mode = track["freeTrackingTransport"]
            match mode:
                case "bus":
                    return "multimodal"
                case "train":
                    return "multimodal"
                case "bike":
                    return "bicycle"
                case "walk":
                    return "pedestrian"
                case "car":
                    return "auto"
                case _:
                    return None
    return None


def convert_tracked_instance_to_points(track):
    """
    Convert the tracked instance to a sequence of points.
    """
    points = []
    if "validationResult" in track and "valid" in track["validationResult"] and track["validationResult"]["valid"] is True:
        for point in track["geolocationEvents"]:
            point_new = {
                "longitude": point["longitude"],
                "latitude": point["latitude"],
                "recorded_at": point["recorded_at"]
            }
            points.append(point_new)
    return points


class ValhallaEngine:
    """
    ValhallaEngine class to handle Valhalla engine operations.
    """

    def __init__(self):
        # Leggi il template da file
        with open("data/valhalla_locate_template.txt", "r", encoding="utf-8") as f:
            template_content = f.read()
        # Crea il template Jinja2
        self.template = Template(template_content)
        self.valhalla_uri = os.getenv("VALHALLA_LOCATE_URI", "http://localhost:8002/locate")




    def find_nearest_edges(self, track, track_id) -> list[EndgeInfo]:
        """
        Find the nearest edges in the graph for the given points.
        """
        nearest_edges = []
        start = datetime.now()
        points = convert_tracked_instance_to_points(track)
        print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Points: {len(points)}")
        if len(points) > 0:
            # Sostituisci i valori
            rendered = self.template.render(costing=get_transit_mode(track), points=points)
            # Convertilo in oggetto Python se serve
            data_points = json.loads(rendered)
            # Invio della richiesta POST con il body in JSON
            response = requests.post(self.valhalla_uri, json=data_points)
            if response.status_code == 200:
                # Parsare la risposta JSON in un dizionario Python
                data_locate = response.json()
                for track_element in data_locate:
                    if "edges" in track_element and track_element["edges"] is not None:
                        for edge in track_element["edges"]:
                            if "edge_info" in edge:
                                edge_info = edge["edge_info"]
                                endge_info_obj = EndgeInfo(edge_info["way_id"], edge_info["shape"])
                                if not endge_info_obj in nearest_edges:
                                    nearest_edges.append(endge_info_obj)
            else:
                print(f"Errore[{track_id}]: {response.status_code}")
        stop = datetime.now()
        print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Edges: {len(nearest_edges)}, Time:{(stop - start).total_seconds()} seconds")
        return nearest_edges
