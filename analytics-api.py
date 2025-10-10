import os
import h3
import logging

from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib import colors

from shapely.geometry import shape, Polygon
import geopandas as gpd

from storage.storage_engine import FileStorage
from psycopg.psyco_engine import PsycoEngine

from flask import Flask, request, render_template

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')
logger = logging.getLogger(__name__)

def h3_to_geojson(h3_cell):
    """Convert H3 index to GeoJSON Polygon."""
    boundary = h3.cells_to_geo([h3_cell])
    return Polygon(shape(boundary))


def get_h3_geo(territory_id, year, mode, target_resolution):
    df_h3_info = psyco_engine.get_h3_info(territory_id, year, mode)
    # Crea una nuova colonna con l'H3 parent
    df_h3_info['h3_parent'] = df_h3_info['h3'].apply(lambda x: h3.cell_to_parent(x, target_resolution))
    # Tieni solo una riga per ogni coppia (track_id, h3)
    df_unique = df_h3_info.drop_duplicates(subset=['track_id', 'h3_parent'])
    # Conta quante tracce distinte hanno attraversato ogni h3
    h3_counts = df_unique['h3_parent'].value_counts().reset_index()
    h3_counts.columns = ['h3_parent', 'distinct_track_count']
    # Toglie le celle H3 che hanno meno di 15 tracce distinte
    h3_counts_filtered = h3_counts[h3_counts['distinct_track_count'] >= 15]
    # aggiungo i colori
    # Scegli una colormap, ad esempio 'viridis'
    cmap = plt.get_cmap('viridis')
    # Normalizza i valori tra 0 e 1
    norm = plt.Normalize(h3_counts_filtered['distinct_track_count'].min(), h3_counts_filtered['distinct_track_count'].max())
    # Applica la colormap e converti in esadecimale
    h3_counts_filtered['color'] = h3_counts_filtered['distinct_track_count'].apply(lambda x: colors.to_hex(cmap(norm(x))))
    # Crea un GeoDataFrame con le geometrie H3
    h3_geoms = h3_counts_filtered["h3_parent"].apply(lambda x: h3_to_geojson(x))
    h3_gdf = gpd.GeoDataFrame(data=h3_counts_filtered, geometry=h3_geoms, crs=4326)
    return h3_gdf.to_json()



app = Flask(__name__)
server_port = os.getenv("SERVER_PORT", 8078)
psyco_engine = PsycoEngine()
psyco_engine.init_tables()


@app.route('/api/geo/h3', methods=['GET'])
def api_get_h3_geo():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    year = request.args.get('year', type=str)
    mode = request.args.get('mode', type=str, default='all')
    target_resolution = request.args.get('target_resolution', type=int, default=8)
    json = get_h3_geo(territory_id, year, mode, target_resolution)
    stop = datetime.now()
    print(f"api_get_h3_geo Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return json


@app.route('/api/geo/campaign', methods=['GET'])
def api_get_campaign_geo():
    territory_id = request.args.get('territory_id', type=str)
    campaign_id = request.args.get('campaign_id', type=str)
    group_id = request.args.get('group_id', type=str, default=None)

    if not territory_id:
        return {"error": "territory_id is required"}, 400

    file_path = FileStorage().get_campaign_analysis_filename(territory_id, campaign_id, group_id)
    if not os.path.exists(file_path):
        return {"error": "File not found"}, 404

    # Leggi il Parquet e restituisci GeoJSON
    gdf = gpd.read_parquet(file_path)
    rows, columns = gdf.shape
    logger.info(f"H3 data {territory_id}, {campaign_id}, {group_id} Rows: {rows}, Columns: {columns}")
    return gdf.to_json()


@app.route('/')
def home():
    return render_template('index.html')

@app.route('/campaign')
def campaign_h3():
    return render_template('campaign_h3.html')

if __name__ == "__main__":    
    app.run(host='0.0.0.0', port=server_port)
