#=======================================================================================#
# Script: main.py                                                                       #
# Objetivo: Script principal para execução do desafio                                   #
# Autor: Marcos Vinicios Martins Carnevale                                              #
# Data: 2023-05-12                                                                      #
# Pontuação Ágil: 2 (fibonacci)                                                         #
# Versão: 1.0                                                                           #
# Python: 3.8.5                                                                         #
# Git: https://github.com/MarcosCarnevale/zapay                                         #
# ======================================================================================#


#---------------------------------------------------------------------------------------#
# Enunciado do desafio                                                                  
# +-----------------------------------------------------------------------------------+  
# | 1.Vamos imaginar que você recebe uma demanda super importante para extrair um dado| 
# | de uma base que ainda não foi 100% estruturada, nela, possui três campos/colunas: | 
# | ID, Created_At e json_response.                                                   | 
# | Precisamos entender qual o valor sumarizado da chave "amount" que esta dentro do  | 
# | campo "json_response" para o mês de Maio/2023.                                    | 
# | No diretório do desafio possui uma base chamada events.csv utilize ela para o     | 
# | desenvolvimento, você pode utilizar Python e/ou SQL.                              | 
# |                                                                                   |
# +-----------------------------------------------------------------------------------+
# |2.Agora, mostre um pouco dos seus conhecimentos em python, no diretório do desafio |
# |possui uma base chamada orders.csv com as colunas/campos: ID, Created_AT, Protocol |
# |e amount.                                                                          |
# | Escreva um script que leia o arquivo .csv e converta o tipo de dados da base para |
# |os seguintes datatype: Integer, Datetime, String e Decimal, respectivamente. Após  |
# |isso, sumarize o campo "Amount" agrupado por Mês de "created_at" e exporte o       |
# |resultado em um arquivo csv.                                                       |
# |                                                                                   |
# +-----------------------------------------------------------------------------------+

#---------------------------------------------------------------------------------------#
# Diretório do desafio
# +-----------------------------------------------------------------------------------+
# | Caminho:                                                                          |
# | /scripts/python/desafio/sumarizacao_amount.py                                     |
# | /scr/csv/desafio/events.csv                                                       |
# | /scr/csv/desafio/orders.csv                                                       |
# +-----------------------------------------------------------------------------------+
import os
from sumarizacao_amount import SumarizacaoAmount

#---------------------------------------------------------------------------------------#
# Variáveis para execução do script
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src', 'csv', 'desafio'))
file = 'events.csv'
sep = ','
encoding = 'utf-8'
header = 0
month = 5
year = 2023


# Processamento dos dados
valor_mes = str(SumarizacaoAmount().transform_data(month, year, path, file, sep, encoding, header)).replace('.', ',')

#========================================================================================
# Impressão dos resultados
print("#", "=" * 100, "#" )
print("Resultados desafio 1")
print("#", "=" * 100, "#" )
print(f'  A soma dos valores da coluna "amount" em {str(month).rjust(2, "0")}/{year} é: R$ {valor_mes}')
print("#", "=" * 100, "#")

#========================================================================================
# Resultados
#   created_at      amount
# 0          0  1347377.47
# 1    2023-05   114232.75
# # ==================================================================================================== #
#   A soma dos valores da coluna "amount" em 05/2023 é: 114232.75
# # ==================================================================================================== #

#========================================================================================
# Variáveis para execução do script
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src', 'csv', 'desafio'))
file = 'orders.csv'
sep = ','
encoding = 'utf-8'
header = 0
month = 5
year = 2023
schema = {
    'id': 'int',
    'created_at': 'datetime64',
    'protocol': 'str',
    'amount': 'float64'
}

# Processamento dos dados
dataframe_orders = SumarizacaoAmount(schema=schema).resume_by_month(path, file, sep, encoding, header).reset_index()

# Impressão dos resultados
print("#", "=" * 100, "#", "\n", "Resultados desafio 2", "\n", "#", "=" * 100, "#")
print(dataframe_orders.head(10))
print("#", "=" * 100, "#", "\n", " Exportando para arquivo csv", "\n", "#", "=" * 100, "#")
dataframe_orders.to_csv(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'out', 'orders_sumarizado.csv')), sep=';', encoding='utf-8', index=False)

#========================================================================================
# Resultados
# # ==================================================================================== #
#    created_at     amount
# 59    2023-04   66214.09
# 58    2023-03  371801.97
# 57    2023-02  347782.68
# 56    2023-01  487613.13
# 55    2022-12  201507.98
# 54    2022-11  135093.44
# 53    2022-10  185527.54
# 52    2022-09  191423.78
# ...       ...        ...
