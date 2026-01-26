class DuplicatedUrls:
    def __init__(self):
        print("[PROCESSO] Obtendo todas as notícias já vistas do MongoDB")

    # Transformar em um set para melhorar complexidade de tempo para O(1)
    def get_all_seen_urls(self, connection_mongodb):
        self.list = []
        for doc in connection_mongodb.get_database("couser").get_collection("testDM").find():
            self.list.append(doc['url'])
        
        for doc in connection_mongodb.get_database("couser").get_collection("testDMok").find():
            self.list.append(doc['url'])

        qtd = len(self.list) 

        print(f"[SUCESSO] Todas as notícias foram carregadas. Quantidade [{qtd}]")

        return set(self.list)