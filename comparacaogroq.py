import pandas as pd
from groq import Groq
import re

# Configuração do GPT-OSS-20B
API_KEY = "Coloque a key aqui"
MODELO = "openai/gpt-oss-20b"
client = Groq(api_key=API_KEY)

# ------------------ Funções ------------------

def separar_nome_preco(coluna):
    """Separa o nome e o preço de strings no formato 'nome|preco'"""
    nomes = []
    precos = []
    for valor in coluna:
        if "|" in valor:
            nome, preco = valor.split("|", 1)
            preco = preco.replace("R$", "").replace(" ", "").replace(",", ".")
            try:
                preco = float(preco)
            except:
                preco = 0.0
        else:
            nome = valor
            preco = 0.0
        nomes.append(nome.strip())
        precos.append(preco)
    return nomes, precos

def extrair_compatibilidade(conteudo):
    conteudo_upper = conteudo.upper()
    if re.search(r'\bSIM\b', conteudo_upper):
        return "SIM"
    else:
        return "NÃO"

def extrair_justificativa(conteudo):
    match = re.search(r'Justificativa\s*:\s*(.*)', conteudo, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return "Sem justificativa"

def extrair_preco_sugerido(conteudo, preco_atual):
    match = re.search(r'Preco\s*sugerido\s*[:\-]?\s*[\$Rr\s]*([\d.,]+)', conteudo, re.IGNORECASE)
    if match:
        preco_str = match.group(1).replace(",", ".")
        try:
            return float(preco_str)
        except:
            return preco_atual
    return preco_atual  # fallback

def extrair_justificativa_preco(conteudo):
    match = re.search(r'Justificativa\s*do\s*preco\s*[:\-]?\s*(.*)', conteudo, re.IGNORECASE)
    if match:
        justificativa = match.group(1).strip()
        if justificativa == "":
            return "Preço ajustado com base no concorrente"
        return justificativa
    return "Preço ajustado com base no concorrente"

def comparar_com_ia(principal, concorrente, preco_principal, preco_concorrente):
    """Usa GPT-OSS-20B para comparar produtos e sugerir preço"""
    prompt = f"""
Compare os dois produtos abaixo e diga se são compatíveis ou não.
Leve em consideração nome, marca, cor, unidade, voltagem (se houver) e ano do produto. 
Responda apenas se a compatibilidade for >=97%.

Produto principal: {principal}, Preco: {preco_principal}
Produto concorrente: {concorrente}, Preco: {preco_concorrente}

⚠️ Instruções importantes para o preço:
- Sempre sugira um ajuste de preço para o produto principal.
- Se o preço do principal for maior que o concorrente, sugira uma redução para ficar um pouco abaixo.
- Se o preço do principal for menor que o concorrente, sugira um aumento leve, mas ainda abaixo do concorrente.
- Nunca deixe o campo de preço sugerido vazio.

Formato da resposta:
Compatibilidade: SIM ou NÃO
Justificativa: breve explicação.
Preco sugerido: valor sugerido para o produto principal
Justificativa do preco: breve explicação do ajuste de preco
"""
    response = client.chat.completions.create(
        model=MODELO,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    conteudo = response.choices[0].message.content.strip()

    compatibilidade = extrair_compatibilidade(conteudo)
    justificativa = extrair_justificativa(conteudo)

    if compatibilidade == "SIM":
        preco_sugerido = extrair_preco_sugerido(conteudo, preco_principal)
        justificativa_preco = extrair_justificativa_preco(conteudo)
    else:
        preco_sugerido = None
        justificativa_preco = None

    # Terminal limpo
    print("\n🔎 Comparação de Produto")
    print("Principal   :", principal)
    print("Concorrente :", concorrente)
    print("Preço Principal:", preco_principal)
    print("Preço Concorrente:", preco_concorrente)
    print("Compatibilidade:", compatibilidade)
    print("Justificativa   :", justificativa)
    if compatibilidade == "SIM":
        print("Preco sugerido:", preco_sugerido)
        print("Justificativa preco:", justificativa_preco)
    print("-" * 50)

    return {
        "compatibilidade": compatibilidade,
        "justificativa": justificativa,
        "preco_sugerido": preco_sugerido,
        "justificativa_preco": justificativa_preco
    }

# ------------------ Processamento ------------------

def processar_parquet(origem_path, destino_path):
    df = pd.read_parquet(origem_path)

    # 1️⃣ Separar nome e preço
    df["nome_principal"], df["preco_principal"] = separar_nome_preco(df["principal"])
    df["nome_concorrente"], df["preco_concorrente"] = separar_nome_preco(df["concorrente"])

    # 2️⃣ Comparação GPT-OSS-20B + sugestão de preço
    resultados = df.apply(
        lambda row: comparar_com_ia(
            row["nome_principal"],
            row["nome_concorrente"],
            row["preco_principal"],
            row["preco_concorrente"]
        ),
        axis=1
    )

    df["compatibilidade"] = resultados.apply(lambda x: x["compatibilidade"])
    df["justificativa"] = resultados.apply(lambda x: x["justificativa"])
    df["preco_sugerido"] = resultados.apply(lambda x: x["preco_sugerido"])
    df["justificativa_preco"] = resultados.apply(lambda x: x["justificativa_preco"])

    # 3️⃣ Seleciona apenas colunas finais
    colunas_finais = [
        "nome_principal",
        "nome_concorrente",
        "preco_principal",
        "preco_concorrente",
        "compatibilidade",
        "justificativa",
        "preco_sugerido",
        "justificativa_preco"
    ]
    df_final = df[colunas_finais]

    # 4️⃣ Filtra apenas produtos compatíveis
    df_final = df_final[df_final["compatibilidade"] == "SIM"]

    # 5️⃣ Salva resultado final
    df_final.to_parquet(destino_path, index=False)
    print("\n✅ Arquivo final salvo em:", destino_path)

# ------------------ Execução ------------------

if __name__ == "__main__":
    origem = r"comparacao_20250927_131519.parquet"
    destino = r"comparacao_com_precos_ia_filtrado.parquet"
    processar_parquet(origem, destino)
