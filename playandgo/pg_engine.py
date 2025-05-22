import os
from pymongo import MongoClient
from datetime import datetime

class PlayAndGoEngine:
    
    def __init__(self):
        self.mongo_uri = os.getenv("PG_MONGO_URI", "mongodb://localhost:27017/")
        self.mongo_db = os.getenv("PG_MONGO_DB", "playngo-engine")


    def getTracks(self, territoryId: str, startTime: str):
        # Connessione al server MongoDB (modifica la stringa di connessione se necessario)
        client = MongoClient(self.mongo_uri)

        # Seleziona il database
        db = client[self.mongo_db]

        # Seleziona la collection
        collection = db["trackedInstances"]

        # Ottieni un cursore per tutti i documenti della collection
        start_time_dt = datetime.fromisoformat(startTime)
        cursor = collection.find({"territoryId":territoryId, "startTime":{"$gt":start_time_dt}})

        # Itera sui documenti
        for track in cursor:
            #get only valid docuemt
            if "validationResult" in track and "valid" in track["validationResult"] and track["validationResult"]["valid"] is True:
                yield track