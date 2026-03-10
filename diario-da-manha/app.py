from modules.spider import SpiderDiario

class AppDiario:
    """
    Essa classe inica a execução do crawler do portal Diário da Manhã
    """
    def __init__(self):
        print("[PROCESSO] Iniciando crawler do portal Diário do Amanhã")

        self.worker = SpiderDiario()

        self.worker.start_requests()

if __name__ == "__main__":
    executar = AppDiario()