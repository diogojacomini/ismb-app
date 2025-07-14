""" Funções auxiliares para realizar scraping de dados tabulares
de páginas web, bem como para transformar esses dados em um formato adequado para análise.

Pipeline: data_ingestion
"""
import re
from typing import Dict, List
from time import sleep as time_sleep
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd


def scraping(url: str, headers: Dict[str, str]) -> List[List[str]]:
    """
    Web scraping.

    Esta função executa web scraping procurando por tabelas HTML que contenham as colunas
    'Date' e 'Price'.

    Args:
        url (str): URL da página web para fazer scraping.
        headers (Dict[str, str]): Cabeçalhos HTTP para incluir na requisição.

    Returns:
        List[List[str]]: Uma lista com os dados obtidos.

    """
    validate_url_and_headers(url=url, headers=headers)

    tentativa = 0
    while tentativa < 3:
        try:
            response = requests.get(url, headers=headers, timeout=60)
            soup = BeautifulSoup(response.content, "html.parser")
            tables = soup.find_all("table")

            for table in tables:
                hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
                if ('Date' in hdrs and 'Price' in hdrs) or ('Data' in hdrs and 'Último' in hdrs):
                    print("Tabela encontrada")
                    break
            else:
                raise ValueError("Tabela não encontrada")
            break
        except requests.exceptions.RequestException as error_web_scraping:
            tentativa += 1
            if tentativa == 3:
                raise ValueError(f"Erro ao coletar dados da página: {url}") from error_web_scraping
            time_sleep(3600)

    data = []
    for row in table.find("tbody").find_all("tr"):
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        data.append(cols)

    return data


def validate_url_and_headers(url: str, headers: Dict[str, str]) -> None:
    """
    Validação de parametros de URL e headers.

    Args:
        url (str): URL HTTP ou HTTPS válida.
        headers (Dict[str, str]): Dicionário de cabeçalhos HTTP.

    Raises:
        ValueError: Se a URL e/ou headers for inválidos.

    """
    url_pattern = re.compile(r"^https?://[\w\.-]+(:\d+)?(/[\w\.-]*)*/?")

    if not isinstance(url, str) or not url_pattern.match(url):
        raise ValueError(f"URL inválida: {url}")

    if not isinstance(headers, dict) or not headers:
        raise ValueError("Headers devem ser um dicionário não vazio")


def scraping_infomoney(url: str, class_: str) -> List[Dict[str, str]]:
    r = requests.get(url, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    blocos = soup.find_all("div", class_=class_)
    noticias = []

    for bloco in blocos:
        titulo = bloco.text.strip()
        h2 = bloco.find("h2")
        if h2:
            a_tag = h2.find("a")
            link = a_tag["href"] if a_tag and a_tag.has_attr("href") else None
            data_el = bloco.find_next("time")
            data = data_el["datetime"] if data_el else datetime.today().isoformat()
            noticias.append({"fonte": "InfoMoney", "titulo": titulo, "dat_ref": data, "link": link})

    return noticias


def scraping_valorinveste(url, class_post, class_date) -> List[Dict[str, str]]:
    r = requests.get(url, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    blocos = soup.find_all("a", class_=class_post)
    datas = soup.find_all("span", class_=class_date)
    noticias = []

    for i, bloco in enumerate(blocos):
        titulo = bloco.text.strip()
        data = datas[i].text.strip() if i < len(datas) else datetime.today().isoformat()
        noticias.append({"fonte": "Valor Investe", "titulo": titulo, "dat_ref": data, "link": bloco['href']})
    return noticias


def scraping_seudinheiro(url: str, class_feed: str, class_title: str, class_date: str) -> List[Dict[str, str]]:
    r = requests.get(url, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    blocos = soup.find_all("div", class_=class_feed)
    noticias = []

    for b in blocos:
        h2 = b.find("h2", class_=class_title)
        if not h2:
            continue

        a_tag = h2.find("a")
        titulo = a_tag.get_text(strip=True) if a_tag else h2.get_text(strip=True)
        link = a_tag["href"] if a_tag and a_tag.has_attr("href") else None

        data_el = b.find("div", class_=class_date)
        data = data_el.get_text(strip=True) if data_el else datetime.today().isoformat()
        noticias.append({"fonte": "Seu Dinheiro", "titulo": titulo, "dat_ref": data, "link": link})
    return noticias


def scraping_moneytimes(url: str, class_item: str, class_title: str, class_date: str) -> List[Dict[str, str]]:
    r = requests.get(url, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    blocos = soup.find_all("div", class_=class_item)
    noticias = []

    for b in blocos:
        h2 = b.find("h2", class_=class_title)
        if not h2:
            continue

        a_tag = h2.find("a")
        titulo = a_tag.get_text(strip=True) if a_tag else h2.get_text(strip=True)
        link_url = a_tag["href"] if a_tag and a_tag.has_attr("href") else None

        data_el = b.find("span", class_=class_date)
        data = data_el.get_text(strip=True) if data_el else datetime.today().isoformat()
        noticias.append({"fonte": "MoneyTimes", "titulo": titulo, "dat_ref": data, "link": link_url})
    return noticias


def extrair_campos(texto):
    partes = re.split(r'\s{2,}', texto.strip())
    if len(partes) >= 3:
        categoria = partes[0]
        titulo = partes[1]
        data_publicacao = partes[2]
    else:
        palavras = texto.strip().split()
        categoria = palavras[0]
        data_publicacao = palavras[-3] + ' ' + palavras[-2] + ' ' + palavras[-1]
        titulo = ' '.join(palavras[1:-3])

    return pd.Series([categoria, titulo, data_publicacao])


def extrair_data_url(link):
    m = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', link)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    return None


def parse_data_portugues(texto):
    meses = {
        "janeiro": "01", "fevereiro": "02", "março": "03", "abril": "04",
        "maio": "05", "junho": "06", "julho": "07", "agosto": "08",
        "setembro": "09", "outubro": "10", "novembro": "11", "dezembro": "12"
    }
    m = re.search(r'(\d{1,2}) de (\w+) de (\d{4})', texto)
    if m:
        dia = m.group(1).zfill(2)
        mes = meses.get(m.group(2).lower())
        ano = m.group(3)
        if mes:
            return f"{ano}-{mes}-{dia}"
    return None


def data_relativa_para_absoluta(texto, agora=None):
    if agora is None:
        agora = datetime.now()
    texto = texto.lower()
    if "hora" in texto:
        horas = int(re.search(r'(\d+)', texto).group(1))
        dt = agora - timedelta(hours=horas)
    elif "dia" in texto:
        dias = int(re.search(r'(\d+)', texto).group(1))
        dt = agora - timedelta(days=dias)
    else:
        return None
    return dt.strftime('%Y-%m-%d')


def select_cast_midia(df: pd.DataFrame) -> pd.DataFrame:
    """Seleciona e converte colunas do DataFrame."""
    df = df[['dat_ref', 'fonte', 'titulo', 'link']]
    df = df.astype({col: 'string' for col in df.columns if col != 'dat_ref'})
    return df
