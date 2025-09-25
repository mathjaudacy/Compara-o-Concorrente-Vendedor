import pandas as pd
import requests
import time
from itertools import product
from difflib import SequenceMatcher

# 1. Ler o arquivo parquet 
df = pd.read_parquet("comparacao.parquet", engine="pyarrow")

API_KEY = "Colque sua key aqui"  # coloque sua chave aqui
url = "https://api.groq.com/openai/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

matches = []

# Função para comparar similaridade local 
def similaridade_local(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

# Função para chamar API com retry automático 
def verificar_api(produto_ref, concorrente):
    prompt = f"""
Compare os dois nomes de produtos abaixo. 
Responda **apenas** com 'SIM' ou 'NAO'.

Critérios:
- Responda 'SIM' apenas se os produtos forem praticamente o mesmo (mesmo modelo/marca/nome).
- Pequenas diferenças como acentos, letras maiúsculas ou abreviações ainda contam como 'SIM'.
- Se houver qualquer dúvida ou nomes diferentes, responda 'NAO'.

Produto referência: {produto_ref}
Produto concorrente: {concorrente}
"""
    payload = {
        "model": "deepseek-r1-distill-llama-70b",
        "messages": [
            {"role": "system", "content": "Você é um verificador rigoroso de equivalência de produtos."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0
    }

    while True:
        response = requests.post(url, headers=headers, json=payload)
        result = response.json()

        # Se der rate limit, espera e tenta novamente
        if "error" in result and result["error"]["code"] == "rate_limit_exceeded":
            wait_time = 1  # ajustar se necessário
            print(f"Rate limit atingido. Aguardando {wait_time}s...")
            time.sleep(wait_time)
            continue

        try:
            resposta_raw = result["choices"][0]["message"]["content"].strip()
            # extrai apenas SIM ou NAO da última linha
            resposta = None
            for linha in reversed(resposta_raw.splitlines()):
                linha = linha.strip().upper()
                if linha in ["SIM", "NAO"]:
                    resposta = linha
                    break
            if not resposta:
                resposta = "NAO"
        except Exception as e:
            print("Erro na resposta da API:", result)
            resposta = "NAO"

        return resposta

# 2. Comparar todas as combinações com filtro de similaridade local 
for produto_ref, concorrente in product(df["principal"], df["concorrente"]):
    # Filtra pares com baixa similaridade para não gastar tokens
    if similaridade_local(produto_ref, concorrente) < 0.5:
        continue

    resposta = verificar_api(produto_ref, concorrente)
    print(f"REF: {produto_ref} | CONC: {concorrente} => {resposta}")

    # Apenas adiciona se a resposta for SIM
    if resposta == "SIM":
        matches.append({
            "produto_referencia": produto_ref,
            "concorrente": concorrente
        })

# 3. Salvar produtos combinados em parquet 
df_matches = pd.DataFrame(matches)
df_matches.to_parquet("produtos_combinados.parquet", engine="pyarrow", index=False)

print(f"\nArquivo produtos_combinados.parquet gerado com sucesso! Total de matches: {len(matches)}")
