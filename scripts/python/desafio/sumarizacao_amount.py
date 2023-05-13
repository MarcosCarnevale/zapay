# ======================================================================================#
# Script: sumarizacao_amount.py                                                         #
# Objetivo: Sumarizar o valores de  "amount" que esta dentro do campo "json_response"   #
# Autor: Marcos Vinicios Martins Carnevale                                              #
# Data: 2023-05-12                                                                      #
# Pontuação Ágil: 2 (fibonacci)                                                         #
# Versão: 1.0                                                                           #
# Python: 3.8.5                                                                         #
# ======================================================================================#

#---------------------------------------------------------------------------------------#
# Enunciado do desafio                                                                  
# +-----------------------------------------------------------------------------------+  
# |  Vamos imaginar que você recebe uma demanda super importante para extrair um dado | 
# | de uma base que ainda não foi 100% estruturada, nela, possui três campos/colunas: | 
# | ID, Created_At e json_response.                                                   | 
# | Precisamos entender qual o valor sumarizado da chave "amount" que esta dentro do  | 
# | campo "json_response" para o mês de Maio/2023.                                    | 
# | No diretório do desafio possui uma base chamada events.csv utilize ela para o     | 
# | desenvolvimento, você pode utilizar Python e/ou SQL.                              | 
# +-----------------------------------------------------------------------------------+ 

#---------------------------------------------------------------------------------------#
# Diretório do desafio
# +-----------------------------------------------------------------------------------+
# | Caminho:                                                                          |
# | /scripts/python/desafio/sumarizacao_amount.py                                     |
# | /scr/csv/desafio/events.csv                                                       |
# +-----------------------------------------------------------------------------------+

# ======================================================================================
# Importando bibliotecas
import pandas as pd 
import os

# ======================================================================================
# Definindo funções
class SumarizacaoAmount:

    def __init__(self):
        pass
  

    def json_to_columns(self, df: pd.DataFrame, json_column: str) -> pd.DataFrame:

        """
        Função para transformar colunas de um json em colunas de um dataframe
            :param df: dataframe
            :param json_column: nome da coluna que contém o json
            :return: dataframe com as colunas do json
        """
        exploded = pd.json_normalize(df[json_column].apply(eval))
    
        # Renomeando as colunas do dataframe afim de evitar conflitos
        exploded = exploded.rename(columns=lambda x: x.replace(json_column, f"{json_column}_{x}"))
        df = pd.concat([df, exploded], axis=1)

        
        df = df.drop(json_column, axis=1)
        return df

    #---------------------------------------------------------------------------------------#

    def summarize_month(self, df: pd.DataFrame, column_agg: str, column_date: str, month: int, year: int) -> pd.DataFrame:
        """
        Função para sumarizar os valores de uma coluna de um dataframe
            :param df: dataframe
            :param column_agg: coluna que será sumarizada
            :param column_date: coluna que será agrupada
            :param month: mês
            :param year: ano
            :return: dataframe com a coluna sumarizada
        """
        df[column_date] = pd.to_datetime(df[column_date])
        df = df[(df[column_date].dt.month == month) & (df[column_date].dt.year == year)]
        df = df[[column_date, column_agg]].fillna(0)
        df[column_agg] = df[column_agg].astype(float)
        value = df[column_agg].sum()
        return value
    
    #---------------------------------------------------------------------------------------#
    
    def summarize_by_month(self, df: pd.DataFrame, column_agg: str, column_date: str) -> pd.DataFrame:
        """
        Função para sumarizar os valores de uma coluna de um dataframe
            :param df: dataframe
            :param column_agg: coluna que será sumarizada
            :param column_date: coluna que será agrupada
            :return: dataframe com a coluna sumarizada
        """
        df[column_date] = pd.to_datetime(df[column_date])
        df[column_date] = df[column_date].dt.strftime('%Y-%m')
        df[column_agg] = df[column_agg].astype(float)
        df = df[[column_date, column_agg]].fillna(0)
        df = df.groupby([column_date]).sum().reset_index()
        return df
    

    #---------------------------------------------------------------------------------------#

    def read_file(self, path: str, file: str, sep: str, encoding: str, header: int, index_col: int) -> pd.DataFrame:
        """
        Função para ler um arquivo csv
            :param path: caminho do arquivo
            :param file: nome do arquivo
            :param sep: separador
            :param encoding: encoding do arquivo
            :param header: linha do cabeçalho
            :param index_col: coluna que será utilizada como índice
            :return: dataframe
        """
        df = pd.read_csv(f"{path}/{file}", sep=sep, encoding=encoding, header=header, index_col=index_col)
        return df

    #---------------------------------------------------------------------------------------#

    def transform_data(self, month: int, year: int,  path: str, file: str, sep: str, encoding: str, header: int, index_col: int) -> pd.DataFrame:
        """
        Função para transformar os dados
            :param month: mês
            :param year: ano
            :param path: caminho do arquivo
            :param file: nome do arquivo
            :param sep: separador
            :param encoding: encoding do arquivo
            :param header: linha do cabeçalho
            :param index_col: coluna que será utilizada como índice
            :return: dataframe
            """
        df = self.read_file(path, file, sep, encoding, header, index_col)
        df = self.json_to_columns(df, 'json_response')
        df = self.summarize_month(df, 'amount', 'created_at', month, year)
        return df
    
    #---------------------------------------------------------------------------------------#
    def resume_by_month(self, path: str, file: str, sep: str, encoding: str, header: int, index_col: int) -> pd.DataFrame:
        """
        Função para transformar os dados
            :param month: mês
            :param year: ano
            :param path: caminho do arquivo
            :param file: nome do arquivo
            :param sep: separador
            :param encoding: encoding do arquivo
            :param header: linha do cabeçalho
            :param index_col: coluna que será utilizada como índice
            :return: dataframe
            """
        df = self.read_file(path, file, sep, encoding, header, index_col)
        df = self.json_to_columns(df, 'json_response')
        df = self.summarize_by_month(df, 'amount', 'created_at')
        return df
    
#========================================================================================
# Variáveis para execução do script
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src', 'csv', 'desafio'))
file = 'events.csv'
sep = ','
encoding = 'utf-8'
header = 0
index_col = 0
month = 5
year = 2023

# Processamento dos dados
valor_mes = SumarizacaoAmount().transform_data(month, year, path, file, sep, encoding, header, index_col)
dataframe_events = SumarizacaoAmount().resume_by_month(path, file, sep, encoding, header, index_col)

#========================================================================================
# Impressão dos resultados
print(dataframe_events)
print("#", "=" * 100, "#")
print(f'  A soma dos valores da coluna "amount" em {str(month).rjust(2, "0")}/{year} é: {valor_mes}')
print("#", "=" * 100, "#")

#========================================================================================
# Resultados
#   created_at      amount
# 0          0  1347377.47
# 1    2023-05   114232.75
# # ==================================================================================================== #
#   A soma dos valores da coluna "amount" em 05/2023 é: 114232.75
# # ==================================================================================================== #

