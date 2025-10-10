import pandas as pd

import h3

import matplotlib.pyplot as plt
from matplotlib import colors

from shapely.geometry import shape, Polygon
import geopandas as gpd

from storage.storage_engine import FileStorage

def h3_to_geojson(h3_cell):
    """Convert H3 index to GeoJSON Polygon."""
    boundary = h3.cells_to_geo([h3_cell])
    return Polygon(shape(boundary))


def save_gdf_as_parquet(gdf: gpd.GeoDataFrame, territory_id:str, campaign_id:str, group_id:str, file_storage: FileStorage):
    """Save GeoDataFrame as Parquet file."""
    file_path = file_storage.get_campaign_analysis_filename(territory_id, campaign_id, group_id)
    gdf.to_parquet(file_path)
    return file_path


def generate_report(territory_id:str, campaign_id:str, df_campaign_info:pd.DataFrame, df_edges:pd.DataFrame, file_storage: FileStorage):
    # df_campaign_info colums ['territory_id', 'player_id', 'track_id', 'campaign_id', 'campaign_type', 
    # 'start_time', 'end_time', 'mode', 'validation_result', 'distance', 'duration', 
    # 'group_id', 'multimodal_id', 'way_back', 'location_id']
    # from df_campaign_info remove rows that have 'validation_result' false
    df_campaign_valid = df_campaign_info[df_campaign_info['validation_result'] == True]
    # df_edges colums ['track_id', 'h3', 'timestamp', 'node_id', 'way_id', 'ordinal']
    # Unisci i due dataframe sui campi 'track_id'
    df_merged = pd.merge(df_edges, df_campaign_valid[['track_id', 'group_id']], on='track_id', how='inner')

    # Seleziona solo le colonne di interesse e rimuovi i duplicati
    df_unique = df_merged[['track_id', 'h3', 'group_id']].drop_duplicates()
    # Conta quante tracce distinte hanno attraversato ogni h3
    h3_counts = df_unique.groupby(['h3', 'group_id']).size().reset_index(name='distinct_track_count')
    # Toglie le celle H3 che hanno meno di 15 tracce distinte
    h3_counts_filtered = h3_counts[h3_counts['distinct_track_count'] >= 15]
    # crea diversi dtaframe per ogni group_id
    for group_id, df_group in h3_counts_filtered.groupby('group_id'):
        # aggiungo i colori
        # Scegli una colormap, ad esempio 'viridis'
        cmap = plt.get_cmap('viridis')
        # Normalizza i valori tra 0 e 1
        norm = plt.Normalize(df_group['distinct_track_count'].min(), df_group['distinct_track_count'].max())
        # Applica la colormap e converti in esadecimale
        df_group['color'] = df_group['distinct_track_count'].apply(lambda x: colors.to_hex(cmap(norm(x))))
        # Crea un GeoDataFrame con le geometrie H3
        h3_geoms = df_group["h3"].apply(lambda x: h3_to_geojson(x))
        h3_gdf = gpd.GeoDataFrame(data=df_group, geometry=h3_geoms, crs=4326)
        # salva il file
        save_gdf_as_parquet(h3_gdf, territory_id, campaign_id, group_id, file_storage)
    return True