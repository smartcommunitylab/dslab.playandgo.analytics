import os
import h3
import logging

from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib import colors

from shapely.geometry import shape, Polygon
import geopandas as gpd
import pandas as pd

from storage.storage_engine import FileStorage
from duck.duck_engine import DuckEngine
from h3_analysis import get_duck_avg_duration, get_duck_trips, get_duck_user_departure

from flask import Flask, request, render_template

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')
logger = logging.getLogger(__name__)


def h3_to_geojson(h3_cell):
    """Convert H3 index to GeoJSON Polygon."""
    boundary = h3.cells_to_geo([h3_cell])
    return Polygon(shape(boundary))


def get_duck_avg_duration_geo(territory_id:str, campaign_id:str, mode:str, time_slot:str, group_id:str,
                              target_resolution:int, color_by_avg:bool=True, min_tracks:int=5) -> str:
    duck_engine = DuckEngine(territory_id, campaign_id, True)
    df_agg = get_duck_avg_duration(campaign_id, mode, time_slot, group_id, target_resolution, duck_engine)
    duck_engine.close()
    
    # Toglie le celle H3 che hanno meno di x tracce distinte
    df_agg = df_agg[df_agg['tracks'] >= min_tracks]
    
    # Aggiungo i colori
    cmap = plt.get_cmap('plasma')
    if color_by_avg:
        norm = plt.Normalize(df_agg['avg_duration'].min(), df_agg['avg_duration'].max())
        df_agg['color'] = df_agg['avg_duration'].apply(lambda x: colors.to_hex(cmap(norm(x))))
    else:
        norm = plt.Normalize(df_agg['tracks'].min(), df_agg['tracks'].max())
        df_agg['color'] = df_agg['tracks'].apply(lambda x: colors.to_hex(cmap(norm(x))))
    
    # Crea un GeoDataFrame con le geometrie H3
    h3_geoms = df_agg["h3_parent"].apply(lambda x: h3_to_geojson(x))
    h3_gdf = gpd.GeoDataFrame(data=df_agg, geometry=h3_geoms, crs=4326)
    return h3_gdf.to_json()


def get_duck_trips_geo(territory_id:str, campaign_id:str, mode:str, time_slot:str, group_id:str, 
                       target_resolution:int, min_tracks:int=5) -> str:
    duck_engine = DuckEngine(territory_id, campaign_id, True)
    df_agg = get_duck_trips(campaign_id, mode, time_slot, group_id, target_resolution, duck_engine)
    duck_engine.close()
    
    # Toglie le celle H3 che hanno meno di x tracce distinte
    df_agg = df_agg[df_agg['tracks'] >= min_tracks]
    
    # Aggiungo i colori
    cmap = plt.get_cmap('plasma')
    norm = plt.Normalize(df_agg['tracks'].min(), df_agg['tracks'].max())
    df_agg['color'] = df_agg['tracks'].apply(lambda x: colors.to_hex(cmap(norm(x))))
    
    # Crea un GeoDataFrame con le geometrie H3
    h3_geoms = df_agg["h3_parent"].apply(lambda x: h3_to_geojson(x))
    h3_gdf = gpd.GeoDataFrame(data=df_agg, geometry=h3_geoms, crs=4326)
    return h3_gdf.to_json()


def get_duck_departure_geo(territory_id:str, campaign_id:str, mode:str, time_slot:str, group_id:str, 
                           h3_destination:int, target_resolution:int, min_tracks:int=5) -> str:
    duck_engine = DuckEngine(territory_id, campaign_id, True)
    df_agg = get_duck_user_departure(campaign_id, mode, time_slot, group_id, h3_destination, target_resolution, duck_engine)
    duck_engine.close()
    
    # Toglie le celle H3 che hanno meno di x tracce distinte
    df_agg = df_agg[df_agg['unique_users'] >= min_tracks]
    
    # Aggiungo i colori
    cmap = plt.get_cmap('plasma')
    norm = plt.Normalize(df_agg['unique_users'].min(), df_agg['unique_users'].max())
    df_agg['color'] = df_agg['unique_users'].apply(lambda x: colors.to_hex(cmap(norm(x))))

    # Crea un GeoDataFrame con le geometrie H3
    h3_geoms = df_agg["h3_start_parent"].apply(lambda x: h3_to_geojson(x))
    h3_gdf = gpd.GeoDataFrame(data=df_agg, geometry=h3_geoms, crs=4326)
    return h3_gdf.to_json()


app = Flask(__name__)
server_port = os.getenv("SERVER_PORT", 8078)


@app.route('/api/geo/duck/avg-duration', methods=['GET'])
def api_get_duck_duration_geo():
    territory_id = request.args.get('territory_id', type=str)
    campaign_id = request.args.get('campaign_id', type=str)
    mode = request.args.get('mode', type=str, default=None)
    time_slot = request.args.get('time_slot', type=str, default=None)
    group_id = request.args.get('group_id', type=str, default=None)
    target_resolution = request.args.get('target_resolution', type=int, default=8)
    color_by_avg = request.args.get('color_by_avg', type=str, default='true').lower() == 'true'
    min_tracks = request.args.get('min_tracks', type=int, default=5)
    json = get_duck_avg_duration_geo(territory_id, campaign_id, mode, time_slot, group_id, target_resolution, color_by_avg, min_tracks)
    return json


@app.route('/api/geo/duck/trip', methods=['GET'])
def api_get_duck_trips_geo():
    territory_id = request.args.get('territory_id', type=str)
    campaign_id = request.args.get('campaign_id', type=str)
    mode = request.args.get('mode', type=str, default=None)
    time_slot = request.args.get('time_slot', type=str, default=None)
    group_id = request.args.get('group_id', type=str, default=None)
    target_resolution = request.args.get('target_resolution', type=int, default=8)
    min_tracks = request.args.get('min_tracks', type=int, default=5)
    json = get_duck_trips_geo(territory_id, campaign_id, mode, time_slot, group_id, target_resolution, min_tracks)
    return json


@app.route('/api/geo/duck/departure', methods=['GET'])
def api_get_duck_departures_geo():
    territory_id = request.args.get('territory_id', type=str)
    campaign_id = request.args.get('campaign_id', type=str)
    mode = request.args.get('mode', type=str, default=None)
    time_slot = request.args.get('time_slot', type=str, default=None)
    group_id = request.args.get('group_id', type=str, default=None)
    target_resolution = request.args.get('target_resolution', type=int, default=8)
    h3_destination = request.args.get('h3_destination', type=str)
    min_tracks = request.args.get('min_tracks', type=int, default=5)
    json = get_duck_departure_geo(territory_id, campaign_id, mode, time_slot, group_id, h3_destination, target_resolution, min_tracks)
    return json


@app.route('/')
def home():
    return render_template('index.html')

@app.route('/duck/duration')
def duck_avg_duration_h3():
    return render_template('duck_duration.html')

@app.route('/duck/trip')
def duck_trips_h3():
    return render_template('duck_trips.html')

@app.route('/duck/departure')
def duck_departures_h3():
    return render_template('duck_departure.html')

if __name__ == "__main__":    
    app.run(host='0.0.0.0', port=server_port)
