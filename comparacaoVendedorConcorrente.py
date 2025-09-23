import pandas as pd
import requests
import json
import re
import time

# 1. Ler os arquivos parquet 
df_vendedor = pd.read_parquet("produtos_vendedor.parquet", engine="pyarrow")
df_concorrentes = pd.read_parquet("produtos_concorrentes.parquet", engine="pyarrow")

# 2. Configurar API do Groq/DeepSeek 
API_KEY = "COLAR A KEY DO SITE https://console.groq.com/keys"  # Troque pela sua chave
url = "https://api.groq.com/openai/v1/chat/completions"  # URL Groq para chat/DeepSeek

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# 3. Comparar linha a linha usando DeepSeek 
matches = []

for i, prod_v in df_vendedor.iterrows():
    for j, prod_c in df_concorrentes.iterrows():
        print(f"Comparando: {prod_v['nome']} x {prod_c['nome']}")
        prompt = f"""
Compare os dois produtos e responda apenas 'SIM' se eles representam o mesmo produto, 
ou 'NAO' se forem produtos diferentes.

Produto Vendedor:
- Nome: {prod_v['nome']}
- Descrição: {prod_v['descricao']}
- Preço: {prod_v['preco']}

Produto Concorrente:
- Nome: {prod_c['nome']}
- Descrição: {prod_c['descricao']}
- Preço: {prod_c['preco']}
"""

        payload = {
            "model": "deepseek-r1-distill-llama-70b",
            "messages": [
                {"role": "system", "content": "Você é um assistente que compara produtos e verifica se são equivalentes."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0
        }

        # Chamada à API
        response = requests.post(url, headers=headers, json=payload)
        result = response.json()

        # Extrair apenas SIM ou NAO 
        raw_resposta = ""
        if "choices" in result and len(result["choices"]) > 0:
            raw_resposta = result["choices"][0].get("message", {}).get("content", "").strip()
        elif "data" in result and len(result["data"]) > 0:
            raw_resposta = result["data"][0].get("content", "").strip()
        else:
            print("Resposta inesperada da API:", result)

        match_resposta = re.search(r'\b(SIM|NAO)\b', raw_resposta.upper())
        resposta = match_resposta.group(1) if match_resposta else "NAO"

        print(f"Resultado: {resposta}\n")

        if resposta == "SIM":
            matches.append({
                "nome_vendedor": prod_v["nome"],
                "preco_vendedor": prod_v["preco"],
                "nome_concorrente": prod_c["nome"],
                "preco_concorrente": prod_c["preco"]
            })

        # Delay para não sobrecarregar a API 
        time.sleep(1)  

# 4. Salvar produtos combinados em parquet 
df_matches = pd.DataFrame(matches)
df_matches.to_parquet("produtos_combinados.parquet", engine="pyarrow", index=False)

print(f"Arquivo produtos_combinados.parquet gerado com sucesso! Total de matches: {len(matches)}")
