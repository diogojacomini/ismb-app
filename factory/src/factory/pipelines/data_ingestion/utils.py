""" Funções auxiliares para realizar scraping de dados tabulares
de páginas web, bem como para transformar esses dados em um formato adequado para análise.

Pipeline: data_ingestion
"""
import re
from typing import Dict, List
from time import sleep as time_sleep
import requests
from bs4 import BeautifulSoup


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
                if 'Date' in hdrs and 'Price' in hdrs:
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
