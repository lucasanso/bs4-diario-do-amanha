from sshtunnel import open_tunnel
from pymongo import MongoClient
from time import sleep
import yaml
from params.keywords import SEARCH_KEYWORDS, VALIDATION_KEYWORDS, GROUP_KEYWORDS, GROUP_VALIDATION_KEYWORDS, ACTIONS_VALIDATION_KEYWORDS, ACTIONS_KEYWORDS
from modules.middlewares import DuplicatedUrls

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
    """
    Classe que contém a conexão SSH e do MongoDB.
    """
    def __init__(self):
        pass
    
    def connect_ssh(self):
        """
        Realiza conexão SSH.
        """
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

        # Retornando completo, tem que inicializar (server.start())
        return self.server

    def connect_mongodb(self):
        """
        Realiza conexão com o MongoDB.
        """
        connection_string = mongo_db_configs["uri"]

        self.client = MongoClient(connection_string)

        print("[SUCESSO] Conexão com o MongoDB estabelecida")
        
        # Retornando completo, tem que inicializar. (get_database e get_collection)
        return self.client
    
    def close_connection(self):
        if self.client:
            self.client.close()

        if self.server:
            self.server.stop()