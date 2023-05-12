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

def json_to_columns(df: pd.DataFrame, json_column: str) -> pd.DataFrame:
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

def summarize_month(df: pd.DataFrame, column_agg: str, column_date: str, month: int, year: int) -> pd.DataFrame:
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
    df[column_date] = df[column_date].dt.strftime('%Y-%m') 
    df[column_agg] = df[column_agg].astype(float)
    df = df.groupby(column_date).agg({column_agg: 'sum'}).reset_index()
    return df

#========================================================================================
# Lendo arquivo csv
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src', 'csv', 'desafio'))
file = 'events.csv'
dataframe_events = pd.read_csv(f"{path}/{file}", sep=',', encoding='utf-8', header=0, index_col=None)

#========================================================================================
# Transformando coluna JSON_Response em colunas
dataframe_events = json_to_columns(dataframe_events, 'json_response')

#========================================================================================
# Sumarizando os valores da coluna amount
dataframe_events = summarize_month(dataframe_events, 'amount', 'created_at', 5, 2023)

#========================================================================================
print(dataframe_events.head())


