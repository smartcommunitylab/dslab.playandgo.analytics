import os
import json
from datetime import datetime
from datetime import timezone
import requests
from jinja2 import Template
import traceback

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


class TraceInfo:
    def __init__(self, edge_index, distance_from_trace_point, distance_along_edge, 
                 lon, lat, way_id=None, travel_mode=None, timestamp=None):
        self.edge_index = edge_index
        self.distance_from_trace_point = distance_from_trace_point
        self.distance_along_edge = distance_along_edge
        self.lon = lon
        self.lat = lat
        self.way_id = way_id
        self.travel_mode = travel_mode
        self.timestamp = timestamp

    def __repr__(self):
        return f"TraceInfo(edge_index={self.edge_index}, way_id={self.way_id}, travel_mode={self.travel_mode})"


class TraceRoute:
    def __init__(self, track_id:str, shape:str=None, trace_infos:list[TraceInfo]=[]):
        self.track_id = track_id
        self.shape = shape
        self.trace_infos = trace_infos

    def __repr__(self):
        return f"TraceRoute(track_id={self.track_id})"
    

def get_transit_mode(track):
    """
    Get the transit mode from the track.
    """
    if "validationResult" in track and "valid" in track["validationResult"] and track["validationResult"]["valid"] is True:
        if "freeTrackingTransport" in track:
            mode = track["freeTrackingTransport"]
            match mode:
                case "bus":
                    return "auto"
                case "train":
                    return "auto"
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
            dt = point["recorded_at"]
            dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
            point_new = {
                "longitude": point["longitude"],
                "latitude": point["latitude"],
                "recorded_at": point["recorded_at"],
                "time": int(dt.timestamp())
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
        self.template_locate = Template(template_content)
        with open("data/valhalla_trace_attributes_template.txt", "r", encoding="utf-8") as f:
            template_content = f.read()
        self.template_trace_attribuutes = Template(template_content)
        
        self.valhalla_uri = os.getenv("VALHALLA_URI", "http://localhost:8002/locate").rstrip("/")
        

    def find_nearest_edges_by_osm_way(self, track, way_id, lon, lat) -> EndgeInfo:
        """
        Find the nearest edges in the graph for the given points.
        """
        try:
            #start = datetime.now()
            point_new = {
                "longitude": lon,
                "latitude": lat
            }   
            rendered = self.template_locate.render(costing=get_transit_mode(track), points=[point_new])
            # Convertilo in oggetto Python se serve
            data_points = json.loads(rendered)
            # Invio della richiesta POST con il body in JSON
            response = requests.post(self.valhalla_uri + "/locate", json=data_points)
            if response.status_code == 200:
                # Parsare la risposta JSON in un dizionario Python
                data_locate = response.json()
                for track_element in data_locate:
                    if "edges" in track_element and track_element["edges"] is not None:
                        for edge in track_element["edges"]:
                            if "edge_info" in edge:
                                edge_info = edge["edge_info"]
                                if edge_info["way_id"] == way_id:
                                    endge_info_obj = EndgeInfo(edge_info["way_id"], edge_info["shape"])
                                    #stop = datetime.now()
                                    #print(f"{datetime.isoformat(datetime.now())} Way ID: {way_id}, Time:{(stop - start).total_seconds()} seconds")
                                    return endge_info_obj
            else:
                print(f"Errore: {response.status_code} - {response.text}")
            return None
        except Exception as e:
            print(f"Exception[{way_id}]: {e}")
            
        return None
    

    def find_nearest_edges_by_locate(self, track, track_id) -> list[EndgeInfo]:
        """
        Find the nearest edges in the graph for the given points.
        """
        nearest_edges = []
        try:
            start = datetime.now()
            points = convert_tracked_instance_to_points(track)
            print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Points: {len(points)}")
            if len(points) > 0:
                # Sostituisci i valori
                rendered = self.template_locate.render(costing=get_transit_mode(track), points=points)
                # Convertilo in oggetto Python se serve
                data_points = json.loads(rendered)
                # Invio della richiesta POST con il body in JSON
                response = requests.post(self.valhalla_uri + "/locate", json=data_points)
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
                    print(f"Errore[{track_id}]: {response.status_code} - {response.text}")
            stop = datetime.now()
            print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Edges: {len(nearest_edges)}, Time:{(stop - start).total_seconds()} seconds")
            return nearest_edges
        except Exception as e:
            print(f"Exception[{track_id}]: {e}")
        
        return []

    def find_nearest_edges_by_trace(self, track, track_id) -> TraceRoute:
        """
        Find the nearest edges in the graph for the given points.
        """
        trace_route = TraceRoute(track_id=track_id)
        try:
            #start = datetime.now()
            points = convert_tracked_instance_to_points(track)
            sorted_points = sorted(points, key=lambda x: x["time"])
            print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Points: {len(points)}")
            if len(points) > 0:
                # Sostituisci i valori
                rendered = self.template_trace_attribuutes.render(costing=get_transit_mode(track), points=sorted_points)
                # Convertilo in oggetto Python se serve
                data_points = json.loads(rendered)
                # Invio della richiesta POST con il body in JSON
                response = requests.post(self.valhalla_uri + "/trace_attributes", json=data_points)
                if response.status_code == 200:
                    # Parsare la risposta JSON in un dizionario Python
                    data_trace = response.json()
                    trace_route.shape = data_trace["shape"]
                    trace_infos = []
                    data_edges = data_trace["edges"]
                    for index, matched_point in enumerate(data_trace["matched_points"]):
                        if matched_point["type"] == "matched":
                            edge_index = matched_point["edge_index"]
                            if edge_index > len(data_edges):
                                continue
                            trace_info = TraceInfo(
                                edge_index=edge_index,
                                distance_from_trace_point=matched_point["distance_from_trace_point"],
                                distance_along_edge=matched_point["distance_along_edge"],
                                lon=matched_point["lon"],
                                lat=matched_point["lat"],
                                way_id=data_edges[edge_index]["way_id"],
                                travel_mode=data_edges[edge_index]["travel_mode"],
                                timestamp=sorted_points[index]['time']
                            )
                            #TODO some logic about travel_mode or distances 
                            trace_infos.append(trace_info)
                    trace_route.trace_infos = trace_infos
                else:
                    print(f"Errore[{track_id}]: {response.status_code} - {response.text}")
            #stop = datetime.now()
            #print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Edges: {len(trace_route.trace_infos)}, Time:{(stop - start).total_seconds()} seconds")
            return trace_route
        except Exception as e:
            traceback.print_exc()
            print(f"Exception[{track_id}]: {e}")
            
        return TraceRoute(track_id=track_id)
