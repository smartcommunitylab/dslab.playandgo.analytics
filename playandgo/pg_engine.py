import os
from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId

def get_group_id(subscription, campaign_type) -> str:
    """
    Extracts the company group from the subscription.
    """
    if campaign_type == "company":
        if "campaignData" in subscription and subscription["campaignData"] is not None:
            if "companyKey" in subscription["campaignData"]:
                return subscription["campaignData"]["companyKey"]
    elif campaign_type == "school":
        if "campaignData" in subscription and subscription["campaignData"] is not None:
            if "teamId" in subscription["campaignData"]:
                return subscription["campaignData"]["teamId"]
    return None


def get_company_subscription(user, campaign_id:str):
    """
    Extracts the subscription for a given campaign from the user document.
    """
    for role in user["roles"]:
        for sub in role["subscriptions"]:
            if "campaign" in sub and sub["campaign"] == campaign_id:
                return sub
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


class CampaignGroup:
    def __init__(self, territory_id, player_id, campaign_id, group_id):
        self.group_id = group_id
        self.territory_id = territory_id
        self.player_id = player_id
        self.campaign_id = campaign_id

    def __repr__(self):
        return f"CampaignGroup(territory_id={self.territory_id}, player_id={self.player_id}, \
            campaign_id={self.campaign_id}, group_id={self.group_id})"


class SpecificCampaingTrackInfo:
    def __init__(self, territory_id, player_id, track_id, multimodal_id, campaign_id, way_back, location_id):
        self.territory_id = territory_id
        self.player_id = player_id
        self.track_id = track_id
        self.campaign_id = campaign_id
        self.way_back = way_back
        self.location_id = location_id 
        self.multimodal_id = multimodal_id 

    def __repr__(self):
        return f"SpecificCampaingTrackInfo(territory_id={self.territory_id}, player_id={self.player_id}, \
            track_id={self.track_id}, multimodal_id={self.multimodal_id}, campaign_id={self.campaign_id}, way_back={self.way_back}, location_id={self.location_id})"


class PlayAndGoEngine:
    
    def __init__(self):
        # P&G MongoDB connection settings
        self.mongo_uri = os.getenv("PG_MONGO_URI", "mongodb://localhost:27017/")
        self.mongo_db = os.getenv("PG_MONGO_DB", "playngo-engine")
        self.direct_connection = eval(os.getenv("PG_MONGO_DIRECT_CONNECTION", "False"))
        # P&G Aziendale MongoDB connection settings
        self.company_mongo_uri = os.getenv("PG_COMPANY_MONGO_URI", "mongodb://localhost:27017/")
        self.company_mongo_db = os.getenv("PG_COMPANY_MONGO_DB", "pgaziendale-dev")
        self.company_direct_connection = eval(os.getenv("PG_COMPANY_MONGO_DIRECT_CONNECTION", "False"))
        # P&G HSC MongoDB connection settings
        self.hsc_mongo_uri = os.getenv("PG_HSC_MONGO_URI", "mongodb://localhost:27017/")
        self.hsc_mongo_db = os.getenv("PG_HSC_MONGO_DB", "pghsc-dev")
        self.hsc_direct_connection = eval(os.getenv("PG_HSC_MONGO_DIRECT_CONNECTION", "False"))


    def get_track(self, territory_id: str, track_id: str):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri, directConnection=self.direct_connection)

        # Seleziona il database
        db = client[self.mongo_db]

        # Seleziona la collection
        collection = db["trackedInstances"]

        # Ottieni il documento specifico per track_id
        track = collection.find_one({"territoryId": territory_id, "_id": ObjectId(track_id)})

        client.close()

        if track and "validationResult" in track and "valid" in track["validationResult"] and track["validationResult"]["valid"] is True:
            return track
        return None


    def get_tracks(self, territory_id: str, start_time: str, end_time: str = None, mode: str = None):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri, directConnection=self.direct_connection)

        # Seleziona il database
        db = client[self.mongo_db]

        # Seleziona la collection
        collection = db["trackedInstances"]

        query = {"territoryId":territory_id}

        start_time_dt = datetime.fromisoformat(start_time)
        if end_time is not None:
            end_time_dt = datetime.fromisoformat(end_time)
            query["startTime"] = {"$gt": start_time_dt, "$lt": end_time_dt}
        else:
            query["startTime"] = {"$gt": start_time_dt}

        if mode is not None:
            query["freeTrackingTransport"] = mode
        
        # Ottieni un cursore per tutti i documenti della collection
        cursor = collection.find(query, batch_size=10)
        # Itera sui documenti
        for track in cursor:
            #get only valid docuemt
            if "validationResult" in track and "valid" in track["validationResult"] and track["validationResult"]["valid"] is True:
                yield track
        cursor.close()
        client.close()


    def get_campaign_tracks(self, territory_id: str, start_time: str, end_time: str = None):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri, directConnection=self.direct_connection)

        # Seleziona il database
        db = client[self.mongo_db]

        # estrae le campagne
        campaign_collection = db["campaigns"]
        campaign_cursor = campaign_collection.find({"territoryId":territory_id})
        campaign_map = {}
        for campaign in campaign_cursor:
            campaign_map[str(campaign["_id"])] = campaign

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
                if not "modeType" in track:
                    continue
                if track["valid"] is not True:
                    continue
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
        client.close()


    def get_campaign_groups(self, territory_id: str, year: str):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri, directConnection=self.direct_connection)
        # Seleziona il database
        db = client[self.mongo_db]

        # estrae le campagne
        campaign_collection = db["campaigns"]
        campaign_cursor = campaign_collection.find({"territoryId":territory_id})
        for campaign in campaign_cursor:
            if campaign["type"] != "company" and campaign["type"] != "school":
                continue
            if campaign["dateFrom"].strftime("%Y") != year:
                continue
            if campaign["type"] == "company":
                for c_group in self.get_company_group_info(territory_id, str(campaign["_id"])):
                    yield c_group    
            elif campaign["type"] == "school":
                for c_group in self.get_hsc_group_info(territory_id, str(campaign["_id"])):
                    yield c_group
        campaign_cursor.close()

        client.close()


    def get_company_group_info(self, territory_id: str, campaign_id: str):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.company_mongo_uri, directConnection=self.company_direct_connection)

        # Seleziona il database
        db = client[self.company_mongo_db]

        employee_collection = db["employee"]
        user_collection = db["user"]

        # estrale le company
        company_map = {}
        company_collection = db["company"]
        company_cursor = company_collection.find({"territoryId":territory_id})
        for company in company_cursor:
            company_id = str(company["_id"])
            company_map[company_id] = company

        employee_map = {}
        # seleziona gli employee con trackingRecord.campaign_id esistente
        employee_cursor = employee_collection.find({"trackingRecord." + campaign_id: {"$exists": True}})
        for employee in employee_cursor:
            company_id = employee["companyId"]
            if company_id not in company_map:
                continue
            company_code = company_map[company_id]["code"]
            employee_code = employee["code"]
            employee_key = f"{company_code}__{employee_code}"
            employee_map[employee_key] = employee
        employee_cursor.close()

        # seleziona gli user registrati alla campagna
        user_cursor = user_collection.find({"roles.subscriptions.campaign": campaign_id})
        for user in user_cursor:
            sub = get_company_subscription(user, campaign_id)
            if sub is None:
                continue
            company_code = str(sub["companyCode"])
            employee_code = str(sub["key"])
            employee_key = f"{company_code}__{employee_code}"
            if employee_key in employee_map:
                employee = employee_map[employee_key]
                c_group = CampaignGroup(
                    territory_id=territory_id,
                    player_id=user["playerId"],
                    campaign_id=campaign_id,
                    group_id=company_code
                )
                yield c_group
        user_cursor.close()

        client.close()


    def get_hsc_group_info(self, territory_id: str, campaign_id: str):
        client = MongoClient(self.hsc_mongo_uri, directConnection=self.hsc_direct_connection)
        db = client[self.hsc_mongo_db]

        initiative_collection = db["initiative"]
        initiative_cursor = initiative_collection.find({"campaign.territoryId":territory_id, "campaign.campaignId":campaign_id})
        for initiative in initiative_cursor:
            team_collection = db["playerTeam"]
            team_cursor = team_collection.find({"initiativeId":str(initiative["_id"])})
            for team in team_cursor:
                for player in team["members"]:
                    c_group = CampaignGroup(
                        territory_id=territory_id,
                        player_id=player["playerId"],
                        campaign_id=campaign_id,
                        group_id=str(team["_id"])
                    )
                    yield c_group
            team_cursor.close()
        initiative_cursor.close()

        client.close()


    def get_campaign_tracks_info(self, territory_id: str, year: str):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri, directConnection=self.direct_connection)
        # Seleziona il database
        db = client[self.mongo_db]

        # estrae le campagne
        campaign_collection = db["campaigns"]
        campaign_cursor = campaign_collection.find({"territoryId":territory_id})
        for campaign in campaign_cursor:
            if campaign["type"] != "company" and campaign["type"] != "school":
                continue
            if campaign["dateFrom"].strftime("%Y") != year:
                continue
            if campaign["type"] == "company":
                for c_group in self.get_company_tracks_info(territory_id, str(campaign["_id"])):
                    yield c_group    
            elif campaign["type"] == "school":
                continue
                #for c_group in self.get_hsc_group_info(territory_id, str(campaign["_id"])):
                #    yield c_group
        campaign_cursor.close()

        client.close()


    def get_company_tracks_info(self, territory_id: str, campaign_id: str):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.company_mongo_uri, directConnection=self.company_direct_connection)

        # Seleziona il database
        db = client[self.company_mongo_db]

        collection = db["dayStat"]

        cursor = collection.find({"campaign": campaign_id})
        for doc in cursor:
            if "tracks" not in doc:
                continue
            for track in doc["tracks"]:
                s_info = SpecificCampaingTrackInfo(
                    territory_id=territory_id,
                    player_id=doc["playerId"],
                    track_id=track["trackId"],
                    multimodal_id=track["multimodalId"],
                    campaign_id=campaign_id,
                    way_back=track.get("wayBack", False),
                    location_id=track.get("locationId", None)
                )
                yield s_info
        cursor.close()

        client.close()
