# -*- coding: utf-8 -*-
"""Projeto ETL - Dio.ipynb

# Desafio de Projeto da DIO - ETL com Python 🐍💻
**Santander** Dev Week 2023

planilha: 'SDW2023.csv', com uma lista de IDs de usuário do banco conforme listada abaixo:
  ```
  UserID
  5692
  5697
  5694
  5695
  5696
  ```

## Importação das bibliotecas 📚
"""

!pip install openai

import pandas as pd
from google.colab.data_table import DataTable
import requests
import json
import openai

"""## **Extract** 🔢"""

# Repositório da API: https://github.com/digitalinnovationone/santander-dev-week-2023-api
sdw2023_api_url = 'https://sdw-2023-prd.up.railway.app'

data_frame = pd.read_csv('SDW2023.csv', encoding='ISO-8859-1',
                         na_values ='NaN', delimiter=';')
DataTable(data_frame)

user_ids = data_frame['UserID'].tolist()
print(user_ids)

def get_user(id):
  response = requests.get(f'{sdw2023_api_url}/users/{id}')
  return response.json() if response.status_code == 200 else None

users = [user for id in user_ids if (user := get_user(id)) is not None]
print(json.dumps(users, indent=2))

"""## **Transform**

Utilize a API do OpenAI GPT-4 para gerar uma mensagem de marketing personalizada para cada usuário.
"""

openai_api_key = ''

openai.api_key = openai_api_key

def generate_ai_news(user):
  completion = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
      {
          "role": "system",
          "content": "Você é um especialista em markting bancário."
      },
      {
          "role": "user",
          "content": f"Crie uma mensagem para {user['name']} sobre a importância dos investimentos (máximo de 100 caracteres)"
      }
    ]
  )
  return completion.choices[0].message.content.strip('\"')

for user in users:
  news = generate_ai_news(user)
  print(news)
  user['news'].append({
      "icon": "https://digitalinnovationone.github.io/santander-dev-week-2023-api/icons/credit.svg",
      "description": news
  })

"""## **Load**

Atualize a lista de "news" de cada usuário na API com a nova mensagem gerada.
"""

print(users)

def update_user(user):
  response = requests.put(f"{sdw2023_api_url}/users/{user['id']}", json=user)
  return True if response.status_code == 200 else False

for user in users:
  success = update_user(user)
  print(f"User {user['name']} updated? {success}!")