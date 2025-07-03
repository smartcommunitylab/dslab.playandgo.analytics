import pyrosm 
import osmnx as ox

from datetime import datetime


class GraphMap:
    def __init__(self):
        self.net_walking = 'walking'
        self.net_cycling = 'cycling'
        self.net_driving = 'driving'     
        self.net_service = 'driving+service' 
        self.net_all = 'all'
    

    def get_osm_file(self, territory_id: str):
        match territory_id.upper():
            case "FERRARA":
                return "data/osm/ferrara.osm.pbf"
            case "L":
                return "data/osm/lecco.osm.pbf"
            case _:
                return None


    def get_network_type(self, mode_type: str):
        """
        Get the network type based on the mode type.
        """
        match mode_type:
            case "walk":
                return self.net_walking
            case "bike":
                return self.net_cycling
            case "bus":
                return self.net_service
            case "train":
                return self.net_service
            case "car":
                return self.net_driving
            case _:
                raise ValueError(f"Unknown mode type: {mode_type}")


    def load_graph(self, territory_id: str, mode_type: str):
        network_type = self.get_network_type(mode_type)
        osm_file = self.get_osm_file(territory_id)
        if not osm_file:
            raise ValueError(f"No OSM file found for territory ID: {territory_id}")
        print(f"{datetime.isoformat(datetime.now())} Start loading Graph - Territory ID: {territory_id}, Mode: {mode_type}")
        start = datetime.now()
        osm = pyrosm.OSM(osm_file)
        nodes, edges = osm.get_network(nodes=True, network_type=network_type)
        G = osm.to_graph(nodes, edges, graph_type="networkx")
        stop = datetime.now()
        print(f"{datetime.isoformat(datetime.now())} Graph loaded - Territory ID: {territory_id}, Mode: {mode_type}, Time:{(stop - start).total_seconds()} seconds")
        return G


    def find_nearest_nodes(self, G, lon_array, lat_array, track_id):
        """
        Find the nearest edges in the graph for the given points.
        """
        start = datetime.now()
        nearest_nodes = ox.distance.nearest_nodes(G, lon_array, lat_array, return_dist=False)
        stop = datetime.now()
        print(f"{datetime.isoformat(datetime.now())} Track ID: {track_id}, Nodes: {len(nearest_nodes)}, Time:{(stop - start).total_seconds()} seconds")
        return nearest_nodes
