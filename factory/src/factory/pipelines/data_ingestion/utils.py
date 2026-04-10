"""
Funcoes auxiliares de scraping e transformacao para o pipeline data_ingestion.
"""
import hashlib
import re
from typing import Dict, List
from time import sleep as time_sleep
import unicodedata
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def scraping(url: str, headers: Dict[str, str]) -> List[List[str]]:
    """Extrai linhas de uma tabela HTML (com cabecalho Date/Price ou Data/Ultimo) via scraping."""
    validate_url_and_headers(url=url, headers=headers)

    tentativa = 0
    while tentativa < 3:
        try:
            response = requests.get(url, headers=headers, timeout=60)
            soup = BeautifulSoup(response.content, "html.parser")
            tables = soup.find_all("table")

            for table in tables:
                hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
                if ("Date" in hdrs and "Price" in hdrs) or (
                    "Data" in hdrs and "Último" in hdrs
                ):
                    logger.info("scraping: Tabela encontrada")
                    break
            else:
                raise ValueError("Tabela não encontrada")
            break
        except requests.exceptions.RequestException as error_web_scraping:
            tentativa += 1
            if tentativa == 3:
                raise ValueError(
                    "Erro ao coletar dados da pagina: %s - MS: %s" % (url, error_web_scraping)
                ) from error_web_scraping
            time_sleep(3600)

    data = []
    for row in table.find("tbody").find_all("tr"):
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        data.append(cols)

    return data


def validate_url_and_headers(url: str, headers: Dict[str, str]) -> None:
    """Valida URL e headers HTTP. Levanta ValueError se invalidos."""
    url_pattern = re.compile(r"^https?://[\w\.-]+(:\d+)?(/[\w\.-]*)*/?")

    if not isinstance(url, str) or not url_pattern.match(url):
        raise ValueError("URL invalida: %s" % url)

    if not isinstance(headers, dict) or not headers:
        raise ValueError("Headers devem ser um dicionario nao vazio")


def scraping_infomoney(url: str, class_: str) -> List[Dict[str, str]]:
    """Extrai titulo, link e data de publicacao dos blocos do InfoMoney."""
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
            noticias.append(
                {"fonte": "InfoMoney", "titulo": titulo, "dat_ref": data, "link": link}
            )

    return noticias


def scraping_valorinveste(
    url: str, class_post: str, class_date: str
) -> List[Dict[str, str]]:
    """Extrai titulo, link e data dos artigos do Valor Investe."""
    r = requests.get(url, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    blocos = soup.find_all("a", class_=class_post)
    datas = soup.find_all("span", class_=class_date)
    noticias = []

    for i, bloco in enumerate(blocos):
        titulo = bloco.text.strip()
        data = datas[i].text.strip() if i < len(datas) else datetime.today().isoformat()
        noticias.append(
            {
                "fonte": "Valor Investe",
                "titulo": titulo,
                "dat_ref": data,
                "link": bloco["href"],
            }
        )
    return noticias


def scraping_seudinheiro(
    url: str, class_feed: str, class_title: str, class_date: str
) -> List[Dict[str, str]]:
    """Extrai titulo, link e data dos artigos do portal Seu Dinheiro."""
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
        noticias.append(
            {"fonte": "Seu Dinheiro", "titulo": titulo, "dat_ref": data, "link": link}
        )
    return noticias


def scraping_moneytimes(
    url: str, class_item: str, class_title: str, class_date: str
) -> List[Dict[str, str]]:
    """Extrai titulo, link e data dos artigos do portal MoneyTimes."""
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
        noticias.append(
            {"fonte": "MoneyTimes", "titulo": titulo, "dat_ref": data, "link": link_url}
        )
    return noticias


def extrair_campos(texto: str) -> pd.Series:
    """Divide um bloco de texto em categoria, titulo e data de publicacao."""
    partes = re.split(r"\s{2,}", texto.strip())
    if len(partes) >= 3:
        categoria = partes[0]
        titulo = partes[1]
        data_publicacao = partes[2]
    else:
        palavras = texto.strip().split()
        categoria = palavras[0]
        data_publicacao = palavras[-3] + " " + palavras[-2] + " " + palavras[-1]
        titulo = " ".join(palavras[1:-3])

    return pd.Series([categoria, titulo, data_publicacao])


def extrair_data_url(link: str) -> str | None:
    """Extrai a data YYYY/MM/DD embutida em uma URL padrao de artigo."""
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", link)
    if m:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    return None


def parse_data_portugues(texto: str) -> str | None:
    """Converte expressao de data em portugues (ex: '3 de abril de 2025') para YYYY-MM-DD."""
    meses = {
        "janeiro": "01",
        "fevereiro": "02",
        "março": "03",
        "abril": "04",
        "maio": "05",
        "junho": "06",
        "julho": "07",
        "agosto": "08",
        "setembro": "09",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }
    m = re.search(r"(\d{1,2}) de (\w+) de (\d{4})", texto)
    if m:
        dia = m.group(1).zfill(2)
        mes = meses.get(m.group(2).lower())
        ano = m.group(3)
        if mes:
            return f"{ano}-{mes}-{dia}"
    return None


def data_relativa_para_absoluta(
    texto: str, agora: datetime | None = None
) -> str | None:
    """Converte expressao relativa como '2 horas atras' ou '1 dia atras' para YYYY-MM-DD."""
    if agora is None:
        agora = datetime.now()
    texto = texto.lower()
    if "hora" in texto:
        horas = int(re.search(r"(\d+)", texto).group(1))
        dt = agora - timedelta(hours=horas)
    elif "dia" in texto:
        dias = int(re.search(r"(\d+)", texto).group(1))
        dt = agora - timedelta(days=dias)
    else:
        return None
    return dt.strftime("%Y-%m-%d")


def select_cast_midia(df: pd.DataFrame) -> pd.DataFrame:
    """Seleciona e converte colunas do DataFrame."""
    df = df[["id_news", "dat_ref", "fonte", "titulo", "link"]]
    df = df.astype({col: "string" for col in df.columns if col != "dat_ref"})
    return df


def create_news_id(titulo: str = "", fonte: str = "", dat_ref: str = "") -> str:
    """
    Gera um ID único para uma notícia com base no conteúdo e na fonte.

    - Concatena campos relevantes (titulo, fonte, dat_ref).
    - Calcula SHA-256 da string resultante e retorna um prefixo truncado (16 hex chars) com um prefixo reconhecível.
    """
    parts = []
    for field in (titulo, fonte, dat_ref):
        text = re.sub(r"<[^>]+>", "", str(field))
        text = re.sub(r"#\S+", "", text)
        text = unicodedata.normalize("NFKC", text).lower()
        text = re.sub(r"\s+", " ", text).strip()
        parts.append(text)

    canonical = "||".join(parts)
    meta = f"{len(canonical)}|{canonical}"
    digest = hashlib.sha256(meta.encode("utf-8")).hexdigest()
    return f"ismb_{digest[:16]}"
