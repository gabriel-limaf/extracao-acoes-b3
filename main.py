import requests
from zipfile import ZipFile
from datetime import datetime
import csv


def download_file(url, destination, csv_vazio):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print("Download concluído.")
    else:
        print('Falha no download. Código de status:', response.status_code)
        print('Encerrando operação...')
        with open(csv_vazio, "w", newline="", encoding="utf-8") as arquivo_csv:
            escritor_csv = csv.writer(arquivo_csv, delimiter=";")

            # Escrever o cabeçalho
            cabecalho = ['data_pregao', 'codigo_bdi', 'ticker', 'preco_abertura', 'preco_maximo', 'preco_minimo',
                         'preco_medio', 'preco_fechamento', 'qnt_negociada', 'vol_negociado']
            escritor_csv.writerow(cabecalho)
        exit()


def unzip(path):
    # Abrir o arquivo zip
    with ZipFile(path, 'r') as zip_file:
        # extrair todos os arquivos
        print('Extraindo...')
        zip_file.extractall()
        print('Concluido!')


def gerar_arquivo(arq, arq_csv, tickers_lista):
    linhas_csv = []
    with open(arq, "r", encoding='utf-8') as arquivo:
        # Lê cada linha do arquivo
        for linha in arquivo:
            # Remove caracteres de nova linha (\n) no final da linha
            linha = linha.strip()
            # Ações linha a linha
            if linha[12:24].strip() in tickers_lista:
                data_pregao = datetime.strptime(linha[2:10], "%Y%m%d").date()
                codigo_bdi = linha[10:12].strip()  # pegar regra do bdi para colocar em condicional
                ticker = linha[12:24].strip()
                preco_abertura = "{:.2f}".format(int(linha[56:69])/100)
                preco_maximo = "{:.2f}".format(int(linha[69:82])/100)
                preco_minimo = "{:.2f}".format(int(linha[82:95])/100)
                preco_medio = "{:.2f}".format(int(linha[95:108])/100)
                preco_fechamento = "{:.2f}".format(int(linha[108:121])/100)
                qnt_negociada = int(linha[152:170])
                vol_negociado = "{:.2f}".format(int(linha[170:188])/100)
                linhas_csv.append([data_pregao, codigo_bdi, ticker, preco_abertura, preco_maximo, preco_minimo,
                                   preco_medio, preco_fechamento, qnt_negociada, vol_negociado])
    # Salvar o arquivo
    with open(arq_csv, "w", newline="", encoding="utf-8") as arquivo_csv:
        escritor_csv = csv.writer(arquivo_csv, delimiter=";")

        # Escrever o cabeçalho
        cabecalho = ['data_pregao', 'codigo_bdi', 'ticker', 'preco_abertura', 'preco_maximo', 'preco_minimo',
                     'preco_medio', 'preco_fechamento', 'qnt_negociada', 'vol_negociado']
        escritor_csv.writerow(cabecalho)
        for linha in linhas_csv[1:]:
            escritor_csv.writerow(linha)


def listar_tickers(tickers):
    tickers_lista = []
    with open(tickers, "r", encoding='utf-8') as arquivo:
        # Lê cada linha do arquivo
        for linha in arquivo:
            # Remove caracteres de nova linha (\n) no final da linha
            linha = linha.strip()
            tickers_lista.append(linha)
    return tickers_lista


data_atual = datetime.now()
ano = str(data_atual.year)
mes = str(data_atual.month).zfill(2)
dia = str(data_atual.day - 1).zfill(2)
data_consulta = dia + mes + ano

# URL do arquivo para fazer download
file_url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{data_consulta}.ZIP'

# Caminho onde deseja salvar o arquivo baixado
tickers_path = "tickers.txt"
save_path = f'COTAHIST_D{data_consulta}.ZIP'
txt_path = f'COTAHIST_D{data_consulta}.TXT'
csv_path = f'COTAHIST_D{data_consulta}.csv'

#  Use estas linhas para rodar o script coletando os dados do ano

# # URL do arquivo para fazer download
# file_url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A2022.ZIP'
#
# # Caminho onde deseja salvar o arquivo baixado
# save_path = f'COTAHIST_A2022.ZIP'
# txt_path = f'COTAHIST_A2022.TXT'
# csv_path = f'COTAHIST_A2022.csv'

download_file(file_url, save_path, csv_path)
unzip(save_path)
gerar_arquivo(txt_path, csv_path, listar_tickers(tickers_path))
