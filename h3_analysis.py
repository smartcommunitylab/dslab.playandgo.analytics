import logging
import pandas as pd
import h3

from duck.duck_engine import DuckEngine

logger = logging.getLogger(__name__)

h3_base_level = 10

def get_duck_avg_duration(campaign_id:str, mode:str, time_slot:str, group_id:str, target_resolution:int, 
                              duck_engine: DuckEngine) -> pd.DataFrame:
    query = f"""
        SELECT nearest_edges.h3, count(*) as tracks, avg(track_info.duration) as avg_duration
        FROM nearest_edges JOIN track_info
        ON nearest_edges.track_id=track_info.track_id
        WHERE track_info.campaign_id='{campaign_id}' AND nearest_edges.ordinal=0""" 
    if mode is not None:    
        query = query + f" AND track_info.mode='{mode}'"
    if time_slot is not None:
        query = query + f" AND track_info.time_slot='{time_slot}'"
    if group_id is not None:
        query = query + f" AND track_info.group_id='{group_id}'"        
    query = query + f" GROUP BY nearest_edges.h3"
        
    results = duck_engine.execute_query(query)
    # Crea un DataFrame dai risultati
    df_h3 = pd.DataFrame(results, columns=['h3', 'tracks', 'avg_duration'])
    # Raggruppa i valori h3 alla risoluzione target usando il parent H3
    df_h3['h3_parent'] = df_h3['h3'].apply(lambda x: h3.cell_to_parent(x, target_resolution))
    # Calcola la somma pesata delle durate (weighted by tracks) e il totale tracks per ogni parent
    df_h3['duration_weighted'] = df_h3['avg_duration'] * df_h3['tracks']
    df_agg = df_h3.groupby('h3_parent', as_index=False).agg(
        tracks=('tracks', 'sum'),
        duration_weighted_sum=('duration_weighted', 'sum')
    )
    # Calcola la durata media pesata per parent
    df_agg['avg_duration'] = df_agg['duration_weighted_sum'] / df_agg['tracks']
    df_agg = df_agg.drop(columns=['duration_weighted_sum'])
    return df_agg


def get_duck_trips(campaign_id:str, mode:str, time_slot:str, group_id:str, target_resolution:int, 
                       duck_engine: DuckEngine) -> pd.DataFrame:
    query = f"""
        SELECT nearest_edges.h3, count(distinct nearest_edges.track_id) as tracks
        FROM nearest_edges JOIN track_info
        ON nearest_edges.track_id=track_info.track_id
        WHERE track_info.campaign_id='{campaign_id}'""" 
    if mode is not None:    
        query = query + f" AND track_info.mode='{mode}'"
    if time_slot is not None:
        query = query + f" AND track_info.time_slot='{time_slot}'"
    if group_id is not None:
        query = query + f" AND track_info.group_id='{group_id}'"
    query = query + f" GROUP BY nearest_edges.h3"
    
    results = duck_engine.execute_query(query)
    # Crea un DataFrame dai risultati
    df_h3 = pd.DataFrame(results, columns=['h3', 'tracks'])
    # Raggruppa i valori h3 alla risoluzione target usando il parent H3
    df_h3['h3_parent'] = df_h3['h3'].apply(lambda x: h3.cell_to_parent(x, target_resolution))
    # Calcola la somma delle tracce per ogni parent
    df_agg = df_h3.groupby('h3_parent', as_index=False).agg(
        tracks=('tracks', 'sum')
    )
    return df_agg

def get_duck_user_departure(campaign_id:str, mode:str, time_slot:str, group_id:str, h3_destination:str,
                             target_resolution:int, duck_engine: DuckEngine) -> pd.DataFrame:
    query = f"""
        SELECT player_id, track_id, h3_start, h3_end FROM track_info
        WHERE track_info.campaign_id='{campaign_id}'""" 
    if mode is not None:    
        query = query + f" AND track_info.mode='{mode}'"
    if time_slot is not None:
        query = query + f" AND track_info.time_slot='{time_slot}'"
    if group_id is not None:
        query = query + f" AND track_info.group_id='{group_id}'"
    results = duck_engine.execute_query(query)
    # Crea un DataFrame dai risultati
    df_h3 = pd.DataFrame(results, columns=['player_id', 'track_id', 'h3_start', 'h3_end'])
    # Elimina le righe con valori nulli in h3_start o h3_end
    df_h3 = df_h3.dropna(subset=['h3_start', 'h3_end'])
    # Riporta i valori h3 alla risoluzione target usando il parent H3
    df_h3['h3_start_parent'] = df_h3['h3_start'].apply(lambda x: h3.cell_to_parent(x, target_resolution))
    df_h3['h3_end_parent'] = df_h3['h3_end'].apply(lambda x: h3.cell_to_parent(x, target_resolution))
    # Filtra per h3_end_parent uguale a h3_destination
    df_filtered = df_h3[df_h3['h3_end_parent'] == h3_destination]
    # Raggruppa per h3_start_parent e conta gli utenti unici
    df_departure_counts = df_filtered.groupby('h3_start_parent', as_index=False).agg(
        unique_users=('player_id', 'nunique'))
    return df_departure_counts