import pandas as pd
import os
from datetime import datetime
from playwright.sync_api import sync_playwright

# Função para buscar concorrentes na Amazon
def buscar_concorrentes(produto_nome, max_pages=2):
    url_base = f"https://www.amazon.com.br/s?k={produto_nome.replace(' ', '+')}"
    concorrentes = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="pt-BR")
        page = context.new_page()

        for page_num in range(1, max_pages + 1):
            url = f"{url_base}&page={page_num}"
            page.goto(url, timeout=60000)
            page.wait_for_selector("div.s-main-slot")

            cards = page.query_selector_all("div[data-component-type='s-search-result']")
            for card in cards:
                nome_el = card.query_selector("h2 span")
                nome = nome_el.inner_text().strip() if nome_el else None
                if not nome or "hq" in nome.lower():
                    continue

                preco_el = card.query_selector("span.a-price span.a-offscreen")
                preco = preco_el.inner_text().strip() if preco_el else None

                img_el = card.query_selector("img.s-image")
                imagem = img_el.get_attribute("src") if img_el else None

                link_el = card.query_selector("h2 a")
                href = link_el.evaluate('node => node.getAttribute("href")') if link_el else None
                url = f"https://www.amazon.com.br{href}" if href else None

                concorrentes.append({
                    "nome": nome,
                    "preco": preco,
                    "imagem": imagem,
                    "url": url
                })

        browser.close()
    return concorrentes

# Função principal
def raspagem_concorrentes(produtos):
    raspagem = []

    for produto in produtos:
        concorrentes = buscar_concorrentes(produto["nome"])
        print(f"1° produto {produto["nome"]}")
        bloco = {
            "principal": {
                "nome": produto["nome"],
                "preco": produto["preco"],
                "imagem": "...",  # pode ser preenchido depois
                "url": "..."
            },
            "concorrentes": concorrentes
        }
        raspagem.append(bloco)

    linhas = []
    #Cria um dataframe pra salva em parquet
    for bloco in raspagem:
        principal = bloco["principal"]
        principal_txt = f"{principal['nome']}|{principal['preco']}"
        for concorrente in bloco["concorrentes"]:
            concorrente_txt = f"{concorrente['nome']}|{concorrente['preco']}"
            linhas.append({
                "principal": principal_txt,
                "concorrente": concorrente_txt
            })

    df = pd.DataFrame(linhas)
    df = df.astype(str)
    os.makedirs("media", exist_ok=True)
    nome_arquivo = f"comparacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    caminho = os.path.join("media", nome_arquivo)
    df.to_parquet(caminho, index=False, engine="pyarrow")

    return raspagem, nome_arquivo