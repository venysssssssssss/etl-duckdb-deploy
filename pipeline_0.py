import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv()


def baixar_os_arquivos_do_gd(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(
        url_pasta, output=diretorio_local, quiet=False, use_cookies=False
    )


# funcão que lista os arquivos CSV no diretorio especificado
def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        print(todos_os_arquivos)
        if arquivo.endswith('.csv'):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    print(arquivos_csv)
    return arquivos_csv


# função que le o arquivo CSV e rotorna um DataFreame
def ler_csv(caminho_do_arquivo: str) -> DuckDBPyRelation:
    dataframe_duckdb = duckdb.read_csv(caminho_do_arquivo)
    print(dataframe_duckdb)
    print(type(dataframe_duckdb))
    return dataframe_duckdb


def transformar(df: DuckDBPyRelation) -> DataFrame:
    # df_transformado = duckdb.sql("""
    #     SELECT categoria,
    #            COUNT(*) AS total_vendas,
    #            SUM(valor) AS valor_total
    #     FROM df
    #     GROUP BY categoria
    #     ORDER BY valor_total DESC;
    # """).df()
    df_transformado = duckdb.sql(
        'SELECT *, quantidade * valor AS total_vendas FROM df'
    ).df()
    print(df_transformado)
    return df_transformado


def salvar_no_postgres(df_duckdb, tabela):
    database_url = os.getenv('DATABASE_URL')
    engine = create_engine(database_url)
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    url_pasta = 'https://drive.google.com/drive/folders/19flL9P8UV9aSu4iQtM6Ymv-77VtFcECP'
    diretorio_local = './pasta_gdown'
    # baixar_os_arquivos_do_gd(url_pasta, diretorio_local)
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)

    for caminho_do_arquivo in lista_de_arquivos:
        duck_db_df = ler_csv(caminho_do_arquivo)
        pandas_df_transformado = transformar(duck_db_df)
        salvar_no_postgres(pandas_df_transformado, 'vendas_calculado')
