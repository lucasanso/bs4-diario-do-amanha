from bs4 import BeautifulSoup
import requests
import re
from datetime import date
from params.keywords import SEARCH_KEYWORDS, GROUP_KEYWORDS, ACTIONS_KEYWORDS, GROUP_VALIDATION_KEYWORDS, ACTIONS_VALIDATION_KEYWORDS
from modules.pipelines import ConnectionsDiario
from modules.middlewares import DuplicatedUrls
import os
import sys
from pathlib import Path
import yaml

BUILD_SEARCH_PAGE = "https://www.dm.com.br/page/{}/?s={}"
PAGES = [p for p in range (1, 150)]
# É um portal que toda a implementação é linear a condição de parada pode ser determinada por um while...
class SpiderDiario:
    """
    Classe que contém a lógica principal do crawler.
    """
    def __init__(self) -> None:
        """
        Inicializa as conexões com o túnel SSH e o MongoDB.
        """
        self.connections = ConnectionsDiario()
        self.server = self.connections.connect_ssh()
        self.server.start()

        self.client = self.connections.connect_mongodb()
        self.all_urls = DuplicatedUrls()
        self.all_urls = self.all_urls.get_all_seen_urls(self.client)

        self.start_requests()

    def start_requests(self) -> None:
        """
        Inicia a requisição de páginas do portal Diário da Manhã de acordo com a formatação de:

        BUILD_SEARCH_PAGE

        """
        list_seen_kwords = self.get_keywords_path()

        for k in SEARCH_KEYWORDS:

            # Se a palavra-chave for composta, pula, porque a aba de buscar não admite busca por palavras-chave compostar. Ex: amigos de amigos
            if list_seen_kwords and k in list_seen_kwords:
                print(f"[AVISO] Pulando palavra-chave: {k}")

                continue

            if re.findall(r' ', k):
                continue

            print(f"[PROCESSO] Palavra-chave: {k}")

            for pages in PAGES:
                self.list_urls = []
                url_init = BUILD_SEARCH_PAGE.format(pages, k)
                print(f'[PROCESSO] Percorrendo a URL {url_init}')

                try:
                    response = requests.get(url_init)
                    html = BeautifulSoup(response.text, "html.parser")

                    # Caso a tela de alerta seja exibida, quer dizer que não existem mais resultados, portanto iremos para a próxima
                    if html.select_one(".alert.alert-info"):
                        break
                    
                    # Caso a próxima iteração da palavra-chave nos redirecione para a página inicial
                    #  vai para a próxima palavra-chave
                    main = "https://www.dm.com.br/"
                    tag = html.select_one('[rel="canonical"]')

                    if tag and tag['href'] == main:
                        break
                    
                    self.get_all_urls(html)
            
                except Exception as e:
                    print(f"[ERRO] Não foi possível fazer a requisição da página inicial {e}")

                    sys.exit(1)

                self.parse(k)

            self.insert_keywords(k)

        print("[SUCESSO] Fim de execução de extração do portal Diário do Amanhã! Obrigado por utilizar. © Lucas Santos Soares")

        self.client.close()
        self.server.stop()

        sys.exit(0)

    def get_all_urls(self, html: str) -> None:
        """
        Extrai todas as URLs encontradas na página de acordo com a palavra-chave e paginação.

        Cada URL encontrado é adicionado numa lista para que o conteúdo seja extraído.
        Args:
            html (str) : Conteúdo HTMl da página.
        """
        qtd = 0

        url = html.select('div .col-lg-6.col-md-6.col-12.post > [href *= "www.dm.com.br"]')

        for u in url:
            self.list_urls.append(u['href'])

        qtd += len(self.list_urls)

        print(f"[SUCESSO] Quantidade de URLs encontradas: {qtd}")

    def parse(self, keyword: str) -> None:
        """
        Extrai todo o conteúdo de cada URL encontrada em cada página.

        Nessa função é formatada a estrutura da notícia que entrará no banco.

        Args:
            keyword (str): Palavra-chave que está sendo percorrida.
        """
        for url in self.list_urls:
            if url in self.all_urls:
                print("[AVISO] URL já está no banco. Pulando.")

                continue

            else:
                self.all_urls.add(url)
                try:
                    response = requests.get(url)
                    html = BeautifulSoup(response.text, "html.parser")
                    article = self.extract_paragraph(html)
                    
                    if self.validate_article(article):
                        item = {
                            'keyword' : {},
                            'acquisition_date' : {},
                            'publication_date' : {},
                            'last_update' : {},
                            'newspaper' : {},
                            'url' : {},
                            'title' : {},
                            'article' : {},
                            'tags' : {},
                            'accepted_by': {},
                            'gangs' : {},
                            'id_event' : None,
                            'manual_relevance_class' : None
                        }

                        corpo = self.extract_paragraph(html)
                        corpo = self.process_article(corpo)

                        item['gangs'] = self.search_gangs(article)
                        item['tags'] = self.search_tags(article)
                        item['accepted_by'] = self.validate_article(article)
                        item["keyword"] = keyword
                        item["title"] = html.select_one("h1").text
                        item["url"] = url
                        item["article"] = corpo
                        item["acquisition_date"] = str(date.today())
                        item["newspaper"] = "Diario do Amanha"
                        item["publication_date"] = self.extract_publication_date(html)
                        item["last_update"] = item["publication_date"]
                        item['id_event'] = self.get_next_id_event()

                        self.client.get_database("couser").get_collection("newsData").insert_one(item)
                        print(f"[SUCESSO] item aceito adicionado na coleção testDMok")

                    else:
                        item = {"url" : url}
            
                        self.client.get_database("couser").get_collection("unacceptedNews").insert_one(item)
                        print(f"[SUCESSO] item recusado adicionado na coleção testDM")
        
                except Exception as e:
                    print(f"[ERRO] Erro ao tentar fazer requisição da url {url}: {e}")

    def extract_paragraph(self, article: str) -> str:
        """
        Extrai todos os parágrafos do corpo-texto.

        Cada parágrafo do corpo-texto da notícia é concatenado com o seu sucessor.

        Args:
            html (str): HTML de uma URL para percorrer parágrafos.

        Returns:
            str: Retorna um parágrafo inteiro em formato de string.
        """
        all_paragraph = ""

        paragraphs = article.select(".content.mt-5 > p")

        for p in paragraphs:
            # Caso o parágrafo contenha uma dessas palavras, será pulado, pois não agrega na extração da notícia.
            if re.findall(r'Foto|___|Leia|Reprodução|Vídeo', p.text):
                continue
            all_paragraph = all_paragraph + " " + str(p.text).strip()
        
        return all_paragraph


    def validate_article(self, article: str) -> str:
        """
        Verifica se o corpo-texto da notícia contém palavras-chave que fazem parte do keywords.py.
 
        Args:
            article (str): Corpo-texto da notícia.
        
            Se o corpo-texto possuir grupo criminoso e ação de grupo criminoso, então o corpo-texto será validado.
        Returns:
            str: String formatada contendo o par de palavras (validação - ação de grupo).
        """
        group = False
        action = False

        for k in GROUP_VALIDATION_KEYWORDS:
            if re.findall(fr'{k}', article):
                group = k; break

        for k in ACTIONS_VALIDATION_KEYWORDS:
            if re.findall(fr'{k}', article):
                action = k; break

        if group and action:
            return f"{group} - {action}"
        else:
            return False

    def search_gangs(self, article: str) -> str:
        """
        Busca, utilizando regex, gangues que podem estar contidas no corpo-texto da notícia.

        Args:
            article (str): Corpo-texto da notícia.

        Returns:
            list: Lista de gangues contidas no corpo-texto.
        """
        list_gangs = []

        for k in GROUP_KEYWORDS:
            if re.findall(fr'{k}', article):
                list_gangs.append(k)

        return list_gangs
    
    def search_tags(self, article: str) -> list:
        """
        Busca tags no corpo-texto da notícia com regex.
        
        Args:
            article (str): Corpo-texto da notícia.

        Returns:
            list: Lista de tags contidas no corpo-texto.
        """
        list_tags = []

        for k in ACTIONS_KEYWORDS:
            if re.findall(fr'{k}', article):
                list_tags.append(k)

        return list_tags
    
    def extract_publication_date(self, article: str) -> str:
        """
        Extrai a data da publicação da notícia.

        Args:
            article (str): Corpo-texto da notícia.

        Returns:
            str: Data de publicação da notícia formatada como string.
        """

        date = article.select_one(".infoautor.text-left.ml-3 span").text

        # Pensando no caso de .. de fevereiro de 20..
        date = str(date)[12:36]

        if re.findall(r'às|à', date):
            date = re.sub(r'às|à', '', date)

            if re.findall(r'  .|  ..|  ...', date):
                date = re.sub(r'  .|  ..|  ...', '', date)

                if re.findall(r'20[0-2][0-9][0-2]', date):
                    date = date[0:18]

        return date.strip()
    
    def process_article(self, article: str) -> str:
        """
        Remove o conjunto de caracteres (\\x97, \\x96) do corpo-texto da notícia.

        Durante a observação de extração de notícias, foi observado que esses caracteres estavam 

        poluindo o corpo-texto da notícia e, por isso, esta função foi criada.

        Args:
            article (str): Corpo-texto da notícia.
        Returns:
            str: Retorna a o corpo-texto limpo desses caracteres.
        """
        if re.findall(r'\x97|\x96', article):
            article = re.sub(r'\x97|\x96', '', article)

        article = str(article).strip()
        return article

    def get_keywords_path(self) -> list:
        """
        Lê e retorna as palavras-chave lidas em checked_words.yaml.

        Caso não exista o arquivo checked_words.yaml, este será criado.

        Returns:
            list: Uma lista contendo as palavras-chave lidas do YAML que já foram percorridas.
        """
        list_words = []
        if os.path.exists ("checked_words.yaml") == False:
            print("[SUCESSO] Arquivo .yaml contendo as palavras-chave já percorridas foi criado")
            Path.touch("checked_words.yaml")

            return list_words
        
        with open("checked_words.yaml", "r") as file:
            list_words = yaml.safe_load(file)

            print("[SUCESSO] Retornando lista de palavras-chave já percorridas")

            return list_words
    
    def insert_keywords(self, keyword: str) -> None:
        """
        Salva a palavra-chave que foi completamente percorrida no arquivo checked_words.yaml

        para evitar processamento desnecessário.

        Args:
            str: Palavra-chave que foi totalmente percorrida.
        """ 
        try:
            # w: sobrescreve, a: adiciona ao final (ex: abertosbolivia)
            with open("checked_words.yaml", "a") as file:
                file.write(f"\n{keyword}")
        except FileNotFoundError as e:
            print(f"[ERRO] {e}")

    def get_next_id_event(self): 
        last_record = self.client.get_database('couser').get_collection('newsData').find_one(sort=[('id_event', -1)])
        
        if last_record and 'id_event' in last_record:
            return last_record['id_event'] + 1
            
        # é como se tivesse um else aqui, para caso o banco esteja vazio, daí retorna 1.
        return 1 
if __name__ == "__main__":
    executa = SpiderDiario()