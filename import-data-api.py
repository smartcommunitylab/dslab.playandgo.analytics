import os
import logging

from flask import Flask, request, Response

from datetime import datetime

from import_tracks_data import import_campaign_tracks_data, import_campaign_groups_data, import_nearest_edges_by_trace 
from import_tracks_data import get_df_info_list, merge_campaign_tracks_groups, import_campaign_tracks_info_data

app = Flask(__name__)
server_port = os.getenv("SERVER_PORT", 8078)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(name)s: %(message)s')

@app.route('/api/import/campaign-tracks', methods=['GET'])
def api_import_campaign_tracks_data():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    start_time = request.args.get('start_time', type=str)
    end_time = request.args.get('end_time', default=None, type=str)
    save_csv = request.args.get('save_csv', default=False, type=bool)
    info_map = import_campaign_tracks_data(territory_id, start_time, end_time, save_csv)
    stop = datetime.now()
    print(f"api_import_campaign_tracks_data Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return info_map


@app.route('/api/import/campaign-groups', methods=['GET'])
def api_import_campaign_groups_data():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    save_csv = request.args.get('save_csv', default=False, type=bool)
    info_map = import_campaign_groups_data(territory_id, save_csv)
    stop = datetime.now()
    print(f"api_import_campaign_groups_data Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return info_map


@app.route('/api/import/campaign-tracks-info', methods=['GET'])
def api_import_campaign_tracks_info_data():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    start_time = request.args.get('start_time', type=str)
    end_time = request.args.get('end_time', default=None, type=str)
    save_csv = request.args.get('save_csv', default=False, type=bool)
    info_map = import_campaign_tracks_info_data(territory_id, start_time, end_time, save_csv)
    stop = datetime.now()
    print(f"api_import_campaign_tracks_info_data Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return info_map


@app.route('/api/import/nearest-edges', methods=['GET'])
def api_import_nearest_edges_by_trace():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    start_time = request.args.get('start_time', type=str)
    end_time = request.args.get('end_time', default=None, type=str)
    track_modes = request.args.getlist('mode', type=str)
    save_csv = request.args.get('save_csv', default=False, type=bool)
    info_map = import_nearest_edges_by_trace(territory_id, start_time, track_modes, end_time, save_csv)
    stop = datetime.now()
    print(f"api_import_nearest_edges_by_trace Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return info_map


@app.route('/api/import/info', methods=['GET'])
def api_info_df():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    year = request.args.get('year', type=str)
    info_map = get_df_info_list(territory_id, year)
    stop = datetime.now()
    print(f"api_info_df Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return info_map


@app.route('/api/import/merge-campaign-tracks-groups', methods=['GET'])
def api_merge_campaign_tracks_group():
    start = datetime.now()
    territory_id = request.args.get('territory_id', type=str)
    year = request.args.get('year', type=str)
    campaign_id = request.args.get('campaign_id', type=str)
    save_csv = request.args.get('save_csv', default=False, type=bool)
    info_map = merge_campaign_tracks_groups(territory_id, year, campaign_id, save_csv)
    stop = datetime.now()
    print(f"api_merge_campaign_tracks_groups Territory ID: {territory_id}, Time:{(stop - start).total_seconds()} seconds")
    return info_map


if __name__ == "__main__":    
    app.run(host='0.0.0.0', port=server_port)

    #start_time = "2025-05-01T00:00:00+00:00"
    #end_time = "2025-05-30T23:59:59+00:00"

    #import_campaign_tracks_data("L", start_time)
    #import_campaign_subscriptions_data("L", start_time)
    #import_nearest_edges_by_locate("TN", start_time)
    #import_nearest_edges_by_trace("L", start_time, end_time)
