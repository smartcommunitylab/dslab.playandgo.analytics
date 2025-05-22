import os
from pymongo import MongoClient
from datetime import datetime


class CampaignTrack:
    def __init__(self, territory_id, player_id, track_id, campaign_id, campaign_type, 
                 start_time, end_time, mode, validation_result, distance, duration):
        self.territory_id = territory_id
        self.player_id = player_id
        self.track_id = track_id
        self.campaign_id = campaign_id
        self.campaign_type = campaign_type
        self.start_time = start_time
        self.end_time = end_time
        self.mode = mode
        self.validation_result = validation_result
        self.distance = distance
        self.duration = duration

    def __repr__(self):
        return f"CampaignTrack(territory_id={self.territory_id}, player_id={self.player_id}, track_id={self.track_id}, \
            campaign_id={self.campaign_id}, start_date={self.start_time}, end_date={self.end_time})"
    

class PlayAndGoEngine:
    
    def __init__(self):
        self.mongo_uri = os.getenv("PG_MONGO_URI", "mongodb://localhost:27017/")
        self.mongo_db = os.getenv("PG_MONGO_DB", "playngo-engine")


    def get_tracks(self, territory_id: str, start_time: str):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri)

        # Seleziona il database
        db = client[self.mongo_db]

        # Seleziona la collection
        collection = db["trackedInstances"]

        # Ottieni un cursore per tutti i documenti della collection
        start_time_dt = datetime.fromisoformat(start_time)
        cursor = collection.find({"territoryId":territory_id, "startTime":{"$gt":start_time_dt}})

        # Itera sui documenti
        for track in cursor:
            #get only valid docuemt
            if "validationResult" in track and "valid" in track["validationResult"] and track["validationResult"]["valid"] is True:
                yield track


    def get_campaign_tracks(self, territory_id: str, start_time: str):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri)

        # Seleziona il database
        db = client[self.mongo_db]

        # estrae le campagne
        campaign_collection = db["campaigns"]
        campaign_cursor = campaign_collection.find({"territoryId":territory_id})
        campaign_map = {}
        for campaign in campaign_cursor:
            campaign_map[campaign["_id"]] = campaign

        # Seleziona la collection
        collection = db["campaignPlayerTracks"]

        # Ottieni un cursore per tutti i documenti della collection
        start_time_dt = datetime.fromisoformat(start_time)
        cursor = collection.find({"territoryId":territory_id, "startTime":{"$gt":start_time_dt}})

        # Itera sui documenti
        for track in cursor:
            campaign_id = track["campaignId"]
            if campaign_id in campaign_map:
                campaign = campaign_map[campaign_id]
                ctrack = CampaignTrack()
                ctrack.territory_id = territory_id
                ctrack.campaign_id = campaign_id
                ctrack.player_id = track["playerId"]
                ctrack.track_id = track["trackedInstanceId"]
                ctrack.start_time = track["startTime"]
                ctrack.end_time = track["endTime"]
                ctrack.mode = track["modeType"]
                ctrack.validation_result = track["valid"]
                ctrack.distance = track["distance"]
                ctrack.duration = track["duration"]
                ctrack.campaign_type = campaign["type"]
                yield ctrack
