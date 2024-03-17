import os

arquivos_csv = []
todos_os_arquivos = os.listdir('pasta_gdown')
for arquivo in todos_os_arquivos:
    print(todos_os_arquivos)
    if arquivo.endswith(".csv"):
        caminho_completo = os.path.join('pasta_gdown', arquivo)
        arquivos_csv.append(caminho_completo)