import pandas as pd
from groq import Groq
import re

# Configuração
API_KEY = "Coloque sua key do Groq aqui"
MODELO = "openai/gpt-oss-20b"

client = Groq(api_key=API_KEY)

def extrair_compatibilidade(conteudo):
    """
    Extrai SIM ou NÃO da resposta da IA de forma robusta,
    ignorando maiúsculas, espaços ou variações no texto.
    """
    conteudo_upper = conteudo.upper()
    if re.search(r'\bSIM\b', conteudo_upper):
        return "SIM"
    else:
        return "NÃO"

def comparar_com_ia(principal, concorrente):
    prompt = f"""
Compare os dois produtos abaixo e diga se são compatíveis ou não.
Leve em consideração nome, marca, cor, unidade, voltagem (se houver) e ano do produto. Só aceite caso os parâmetros indicados combinem e tenham uma compatibilidade de no mínimo 97%.

Produto principal: {principal}
Produto concorrente: {concorrente}

Formato da resposta:
Compatibilidade: SIM ou NÃO
Justificativa: texto explicativo breve.
"""
    response = client.chat.completions.create(
        model=MODELO,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    conteudo = response.choices[0].message.content.strip()
    compatibilidade = extrair_compatibilidade(conteudo)

    # Mostrar no console
    print("\n🔎 Comparação")
    print("Principal   :", principal)
    print("Concorrente :", concorrente)
    print("👉 Resposta IA:", conteudo)
    print("Compatibilidade extraída:", compatibilidade, "\n")

    return compatibilidade

def processar_parquet(origem_path, destino_path):
    df = pd.read_parquet(origem_path)

    # Aplica a comparação
    df["compatibilidade"] = df.apply(
        lambda row: comparar_com_ia(row["principal"], row["concorrente"]), axis=1
    )

    # Salva o novo parquet
    df.to_parquet(destino_path, index=False)
    print("\n✅ Novo parquet salvo em:", destino_path)

if __name__ == "__main__":
    origem = "Nome do .parquet" #Insira o nome do arquivo .parquet aqui
    destino = "comparacao_com_resultado.parquet"
    processar_parquet(origem, destino)
