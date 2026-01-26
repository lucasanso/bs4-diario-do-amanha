from modules.spider import SpiderDiario

class AppDiario:
    def __init__(self):
        print("[PROCESSO] Iniciando crawler do portal Diário do Amanhã")

        self.worker = SpiderDiario()

        self.worker.start_requests()

if __name__ == "__main__":
    executar = AppDiario()