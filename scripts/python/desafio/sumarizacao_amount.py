# ======================================================================================#
# Script: sumarizacao_amount.py                                                         #
# Objetivo: Sumarizar o valores de  "amount" que esta dentro do campo "json_response"   #
# Autor: Marcos Vinicios Martins Carnevale                                              #
# Data: 2023-05-12                                                                      #
# Pontuação Ágil: 2 (fibonacci)                                                         #
# Versão: 1.0                                                                           #
# Python: 3.8.5                                                                         #
# Git: https://github.com/MarcosCarnevale/zapay                                         #
# ======================================================================================#

# ======================================================================================
# Importando bibliotecas
import pandas as pd 
import os

# ======================================================================================
# Definindo funções
class SumarizacaoAmount:

    def __init__(self, schema=None):
        """
        Construtor da classe
            :param schema: schema do dataframe (opcional)
        """
        self.schema = schema
        
  

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
        df = df.sort_values(by=[column_date], ascending=False)
        return df
    

    #---------------------------------------------------------------------------------------#

    def read_file(self, path: str, file: str, sep: str, encoding: str, header: int) -> pd.DataFrame:
        """
        Função para ler um arquivo csv
            :param path: caminho do arquivo
            :param file: nome do arquivo
            :param sep: separador
            :param encoding: encoding do arquivo
            :param header: linha do cabeçalho
            :return: dataframe
        """
        df = pd.read_csv(f"{path}/{file}", sep=sep, encoding=encoding, header=header)
        return df
    
    #---------------------------------------------------------------------------------------#
    
    def data_type_apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Função para aplicar o tipo de dado em um dataframe
            :param df: dataframe
            :return: dataframe com o tipo de dado aplicado
        """
        if self.schema:
            for k, v in self.schema.items():
                df[k] = df[k].astype(v)
        return df
    
    #---------------------------------------------------------------------------------------#

    def transform_data(self, month: int, year: int,  path: str, file: str, sep: str, encoding: str, header: int) -> pd.DataFrame:
        """
        Função para transformar os dados
            :param month: mês
            :param year: ano
            :param path: caminho do arquivo
            :param file: nome do arquivo
            :param sep: separador
            :param encoding: encoding do arquivo
            :param header: linha do cabeçalho
            :return: dataframe
            """
        df = self.read_file(path, file, sep, encoding, header)
        df = self.json_to_columns(df, 'json_response')
        df = self.data_type_apply(df)
        df = self.summarize_month(df, 'amount', 'created_at', month, year)
        return df
    
    #---------------------------------------------------------------------------------------#
    def resume_by_month(self, path: str, file: str, sep: str, encoding: str, header: int) -> pd.DataFrame:
        """
        Função para transformar os dados
            :param month: mês
            :param year: ano
            :param path: caminho do arquivo
            :param file: nome do arquivo
            :param sep: separador
            :param encoding: encoding do arquivo
            :param header: linha do cabeçalho
            :return: dataframe
            """
        df = self.read_file(path, file, sep, encoding, header)
        df = self.data_type_apply(df)
        df = self.summarize_by_month(df, 'amount', 'created_at')
        return df
    
#========================================================================================#
