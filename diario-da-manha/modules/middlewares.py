from pymongo import MongoClient

class DuplicatedUrls:
    def __init__(self) -> None:
        print("[PROCESSO] Obtendo todas as notícias já vistas do MongoDB")

    def get_all_seen_urls(self, connection_mongodb: MongoClient) -> set:
        """
        Determina a quantidade total de URLs que já foram percorridas.

        Lógica de deduplicação: faremos a comparação se a URL que estamos lidando já está contida no conjunto de URLs que já foram percorridas.

        Args:
            connection_mongodb (MongoClient): Conexão com o MongoDB.

        Returns:
            set: Retorna o set contendo todas as URLs (uso do set para comparação ser de complexidade O(1)).
        """
        lst = []
        for doc in connection_mongodb.get_database("couser").get_collection("newsData").find():
            lst.append(doc['url'])
        
        for doc in connection_mongodb.get_database("couser").get_collection("unacceptedNews").find():
            lst.append(doc['url'])

        qtd = len(lst) 

        print(f"[SUCESSO] Todas as notícias foram carregadas. Quantidade [{qtd}]")

        return set(lst)