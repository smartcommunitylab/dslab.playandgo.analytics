from sqlalchemy import create_engine, insert, delete
from sqlalchemy import MetaData, Table, Column, DateTime, Integer, BigInteger, String

import os

class PsycoEngine:
    def __init__(self):
        self.db_host = os.getenv("POSTGRES_HOST", "127.0.0.1")
        self.db_port = os.getenv("POSTGRES_PORT", "5432")
        self.db_user = os.getenv("POSTGRES_USER", "pg")
        self.db_password = os.getenv("POSTGRES_PASSWORD", "pg")
        self.db_name = os.getenv("POSTGRES_DB", "pganalytics")
        self.engine = None

        
    def getEngine(self):
        """
        Returns a SQLAlchemy engine for the PostgreSQL database.
        """
        if self.engine is None:
            connection_string = f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            self.engine = create_engine(connection_string)
        return self.engine
    

    def init_tables(self):
        """
        Initializes the database tables.
        """
        self.metadata_obj = MetaData()

        self.track_info_table = Table(
            "track_info",
            self.metadata_obj,
            Column("id", BigInteger, primary_key=True),
            Column("territory_id", String(250), index=True),
            Column("track_id", String(250), index=True),
            Column("player_id", String(250)),
            Column("multimodal_id", String(250)),
            Column("mode", String(50)),
            Column("start_time", DateTime, index=True)
        )

        self.nearest_edges_table = Table(
            "nearest_edges",
            self.metadata_obj,
            Column("id", BigInteger, primary_key=True),
            Column("territory_id", String(250), index=True),
            Column("track_id", String(250), index=True),
            Column("way_id", String(50)),
            Column("h3_cell", String(50)),
            Column("h3_parent", String(50)),
            Column("ordinal", Integer)
        )

        self.metadata_obj.create_all(self.getEngine(), checkfirst=True)


    def getConnection(self):
        """
        Returns a connection to the PostgreSQL database.
        """
        if self.engine is None:
            self.getEngine()
        return self.engine.connect()
    

    def import_track_info(self, territory_id, df_tracks_info):
        """
        Imports track information into the database.
        """
        infos = []
        track_ids = []
        for row in df_tracks_info.itertuples():
            infos.append((territory_id, row.track_id, row.player_id, row.multimodal_id, row.mode, row.start_time))
            track_ids.append(row.track_id)

        delete_stmt = delete(self.track_info_table).where(self.track_info_table.c.territory_id == territory_id).where(self.track_info_table.c.track_id.in_(track_ids))        

        insert_stmt = insert(self.track_info_table).values(            
            [{
                "territory_id": territory_id, 
                "track_id": track_id, 
                "player_id": player_id,
                "multimodal_id": multimodal_id,
                "mode": mode,
                "start_time": start_time
            } for territory_id, track_id, player_id, multimodal_id, mode, start_time in infos])
        
        with self.getConnection() as conn:
            conn.execute(delete_stmt)
            conn.execute(insert_stmt)
            conn.commit()
            