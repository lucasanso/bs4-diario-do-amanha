from sshtunnel import open_tunnel
from pymongo import MongoClient
from time import sleep
import yaml
from params.keywords import SEARCH_KEYWORDS, VALIDATION_KEYWORDS, GROUP_KEYWORDS, GROUP_VALIDATION_KEYWORDS, ACTIONS_VALIDATION_KEYWORDS, ACTIONS_KEYWORDS
from modules.middlewares import DuplicatedUrls
import asyncio

try:
    with open ("config.yaml", "r") as f:
        configs = yaml.safe_load(f)

        ssh_configs = configs["lamcad"]
        mongo_db_configs = configs["mongodb_lamcad"]

        # ATENÇÃO: A coleção aqui é a testDB
        db = mongo_db_configs["database"]
        collection = mongo_db_configs["accepted_news_collection"]

except FileNotFoundError as e:
    print(f"[ERRO] {e}")

class ConnectionsDiario:
    def __init__(self):
        pass
    
    def connect_ssh(self):
        SERVER = (ssh_configs["server_ip"], ssh_configs["server_port"])
        LOCAL = (ssh_configs["local_bind_ip"], ssh_configs["local_bind_port"])
        REMOTE = (ssh_configs["remote_bind_ip"], ssh_configs["remote_bind_port"])

        self.server = open_tunnel (
            SERVER,
            ssh_username = ssh_configs["ssh_username"],
            ssh_password = ssh_configs["ssh_password"],
            remote_bind_address = REMOTE,
            local_bind_address = LOCAL
        )

        print("[SUCESSO] Conexão SSH estabelecida")

        # Retornando completo, tem que inicializar!
        return self.server

    def connect_mongodb(self):
        connection_string = mongo_db_configs["uri"]

        self.client = MongoClient(connection_string)

        print("[SUCESSO] Conexão com o MongoDB estabelecida")
        
        # Retornando completo, tem que inicializar!
        return self.client


    def insert_news(self, database, collection, doc):
        self.client.get_database(database).get_collection(collection).insert_one(doc)
        
        print(f"[SUCESSO] URL foi inserida na coleção {collection}")