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
PAGES = [p for p in range (30, 50)]

class SpiderDiario:
    
    def __init__(self):
        self.connections = ConnectionsDiario()
        self.server = self.connections.connect_ssh()
        self.server.start()

        self.client = self.connections.connect_mongodb()
        self.all_urls = DuplicatedUrls()
        self.all_urls = self.all_urls.get_all_seen_urls(self.client)

        self.start_requests()

    def start_requests(self):
        """
        Função que realiza requests de cada uma das páginas de pesquisa

        """
        list_seen_kwords = self.get_keywords_path()

        for k in SEARCH_KEYWORDS:

            # Se a palavra-chave for composta, pula, porque o site não admite esse tipo de pesquisa
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

                    # Adicionando lógica de parar de iterar caso não encontrar mais resultados
                    if html.select_one(".alert.alert-info"):
                        break
                    
                    main = "https://www.dm.com.br/"
                    tag = html.select_one('[rel="canonical"]')
                    # Adicionando lógica que, caso redirecionados para a página inicial, vai para a próxima palavra-chave
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

    def get_all_urls(self, html):
        """
        Função que pega todas as urls de cada página de pesquisa

        Args:
            html (str) : Conteúdo da requisição para obter cada uma das URLs
        """
        qtd = 0

        url = html.select('div .col-lg-6.col-md-6.col-12.post > [href *= "www.dm.com.br"]')

        for u in url:
            self.list_urls.append(u['href'])

        qtd += len(self.list_urls)

        print(f"[SUCESSO] Quantidade de URLs encontradas: {qtd}")

    def parse(self, keyword):
        """
        Função que processa o conteúdo de uma notícia
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

                        self.client.get_database("couser").get_collection("testDMok").insert_one(item)
                        print(f"[SUCESSO] item aceito adicionado na coleção testDMok")

                    else:
                        item = {"url" : url}
            
                        self.client.get_database("couser").get_collection("testDM").insert_one(item)
                        print(f"[SUCESSO] item recusado adicionado na coleção testDM")

                    # pprint(item)
        
                except Exception as e:
                    print(f"[ERRO] Erro ao tentar fazer requisição da url {url}: {e}")

    def extract_paragraph(self, article):
        """
        Função que extrai o corpo-texto da notícia (parágrafos somente)

        Args:
            html (str): HTML de uma URL para percorrer parágrafos
        """
        all_paragraph = ""

        paragraphs = article.select(".content.mt-5 > p")

        for p in paragraphs:
            if re.findall(r'Foto|___|Leia|Reprodução|Vídeo', p.text):
                continue
            all_paragraph = all_paragraph + " " + str(p.text).strip()
        
        return all_paragraph


    def validate_article(self, article):
        """
        Função que retorna as palavras-chave de validação do corpo-texto da notícia

        Args:
            article (str): Corpo-texto da notícia
        
        
        Returns:
            str: String formatada contendo o par de palavras (validação | ação de grupo)
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

    def search_gangs(self, article):
        """
        Função que busca, utilizando expressões regulares, gangues que podem estar contidas no corpo-texto da notícia

        Args:
            article (str): Corpo-texto da notícia

        Returns:
            list: Lista de gangues contidas no corpo-texto
        """
        list_gangs = []

        for k in GROUP_KEYWORDS:
            if re.findall(fr'{k}', article):
                list_gangs.append(k)

        return list_gangs
    
    def search_tags(self, article):
        """
        Função que busca, utilizando expressões regulares, tags no corpo-texto da notícia
        
        Args:
            article (str): Corpo-texto da notícia

        Returns:
            list: Lista de tags contidas no corpo-texto
        """
        list_tags = []

        for k in ACTIONS_KEYWORDS:
            if re.findall(fr'{k}', article):
                list_tags.append(k)

        return list_tags
    
    def extract_publication_date(self, article):
        """
        Função que extrai a data

        Args:
            article (str): Corpo-texto da notícia

        Returns:
            date: Data de publicação da notícia
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
    
    def process_article(self, article):
        if re.findall(r'\x97|\x96', article):
            article = re.sub(r'\x97|\x96', '', article)

        article = str(article).strip()
        return article

    def get_keywords_path(self):
        # Criar .yaml caso não existir e, ao final de cada iteração do for k in KEYWORDS adicionar essa palavra ao arquivo .yaml
        list_words = []
        if os.path.exists ("checked_words.yaml") == False:
            print("[SUCESSO] Arquivo .yaml contendo as palavras-chave já percorridas foi criado")
            Path.touch("checked_words.yaml")

            return list_words
        
        with open("checked_words.yaml", "r") as file:
            list_words = yaml.safe_load(file)

            print("[SUCESSO] Retornando lista de palavras-chave já percorridas")

            return list_words
        
    def insert_keywords(self, keyword):
        try:
            # w: sobrescreve, a: adiciona ao final (sacoabertos)
            with open("checked_words.yaml", "a") as file:
                file.write(f"\n{keyword}")
        except FileNotFoundError as e:
            print(f"[ERRO] {e}")

if __name__ == "__main__":
    executa = SpiderDiario()