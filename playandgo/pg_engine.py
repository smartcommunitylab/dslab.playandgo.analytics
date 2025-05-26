import os
from pymongo import MongoClient
from datetime import datetime


def get_group_id(subscription, campaign_type) -> str:
    """
    Extracts the company group from the subscription.
    """
    if campaign_type == "company":
        if "groupId" in subscription and subscription["groupId"] is not None:
            return subscription["groupId"]
        if "campaignData" in subscription and subscription["campaignData"] is not None:
            if "companyKey" in subscription["campaignData"]:
                return subscription["campaignData"]["companyKey"]
    elif campaign_type == "school":
        if "campaignData" in subscription and subscription["campaignData"] is not None:
            if "teamId" in subscription["campaignData"]:
                return subscription["campaignData"]["teamId"]
    return None


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
    

class CampaignSubscription:
    def __init__(self, territory_id, player_id, campaign_id, campaign_type, registration_date, group_id=None):
        self.territory_id = territory_id
        self.player_id = player_id
        self.campaign_id = campaign_id
        self.campaign_type = campaign_type
        self.registration_date = registration_date
        self.group_id = group_id

    def __repr__(self):
        return f"CampaignSubscription(territory_id={self.territory_id}, player_id={self.player_id}, \
            campaign_id={self.campaign_id}, registration_date={self.registration_date}, group_id={self.group_id})"


class PlayAndGoEngine:
    
    def __init__(self):
        self.mongo_uri = os.getenv("PG_MONGO_URI", "mongodb://localhost:27017/")
        self.mongo_db = os.getenv("PG_MONGO_DB", "playngo-engine")


    def get_tracks(self, territory_id: str, start_time: str, end_time: str = None):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri)

        # Seleziona il database
        db = client[self.mongo_db]

        # Seleziona la collection
        collection = db["trackedInstances"]

        # Ottieni un cursore per tutti i documenti della collection
        start_time_dt = datetime.fromisoformat(start_time)
        if end_time is not None:
            end_time_dt = datetime.fromisoformat(end_time)
            cursor = collection.find({"territoryId":territory_id, "startTime":{"$gt":start_time_dt, "$lt":end_time_dt}})
        else:
            cursor = collection.find({"territoryId":territory_id, "startTime":{"$gt":start_time_dt}})

        # Itera sui documenti
        for track in cursor:
            #get only valid docuemt
            if "validationResult" in track and "valid" in track["validationResult"] and track["validationResult"]["valid"] is True:
                yield track


    def get_campaign_tracks(self, territory_id: str, start_time: str, end_time: str = None):
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
        if end_time is not None:
            end_time_dt = datetime.fromisoformat(end_time)
            cursor = collection.find({"territoryId":territory_id, "startTime":{"$gt":start_time_dt, "$lt":end_time_dt}})
        else:
            cursor = collection.find({"territoryId":territory_id, "startTime":{"$gt":start_time_dt}})

        # Itera sui documenti
        for track in cursor:
            campaign_id = track["campaignId"]
            if campaign_id in campaign_map:
                campaign = campaign_map[campaign_id]
                c_track = CampaignTrack(
                    territory_id=territory_id,
                    player_id=track["playerId"],
                    track_id=track["trackedInstanceId"],
                    campaign_id=campaign_id,
                    campaign_type=campaign["type"],
                    start_time=track["startTime"],
                    end_time=track["endTime"],
                    mode=track["modeType"],
                    validation_result=track["valid"],
                    distance=track["distance"],
                    duration=track["duration"]
                )
                yield c_track

    def get_campaign_subscriptions(self, territory_id: str, start_time: str, end_time: str = None):
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
        collection = db["campaignSubscriptions"]

        # Ottieni un cursore per tutti i documenti della collection
        start_time_dt = datetime.fromisoformat(start_time)
        if end_time is not None:
            end_time_dt = datetime.fromisoformat(end_time)
            cursor = collection.find({"territoryId":territory_id, "startTime":{"$gt":start_time_dt, "$lt":end_time_dt}})
        else:
            cursor = collection.find({"territoryId":territory_id, "startTime":{"$gt":start_time_dt}})

        for subscription in cursor:
            campaign_id = subscription["campaignId"]
            if campaign_id in campaign_map:
                campaign = campaign_map[campaign_id]
                c_subscription = CampaignSubscription(
                    territory_id=territory_id,
                    player_id=subscription["playerId"],
                    campaign_id=campaign_id,
                    campaign_type=campaign["type"],
                    registrationDate=subscription["registrationDate"],
                    groupId=get_group_id(subscription, campaign["type"])
                )
                yield c_subscription