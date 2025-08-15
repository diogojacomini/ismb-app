"""
This is a boilerplate pipeline 'data_ingestion' generated using Kedro 0.19.14
"""
from typing import Dict
from datetime import datetime
import pandas as pd
import yfinance as yf
from .utils import (
    scraping,
    scraping_infomoney,
    scraping_valorinveste,
    scraping_seudinheiro,
    scraping_moneytimes,
)
from .utils import (
    extrair_campos,
    extrair_data_url,
    parse_data_portugues,
    data_relativa_para_absoluta,
    select_cast_midia,
    logger,
)


def extract_transform_html_table(scraping_mapping: dict, columns_order: list, parameters: dict) -> pd.DataFrame:
    """Função para extrair tabelas HTML."""
    odate = parameters.get("odate")
    environment = parameters.get("environment", "production")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("Parameters - Odate: %s, Environment: %s, Full Data: %s", odate, environment, process_full_data)

    have_replace = scraping_mapping.get("replace_decimal", False)
    dat_ref_format = scraping_mapping.get("dat_ref_format")

    if environment == "test":
        return _make_dataframe_test_wbf(odate)

    data = scraping(
        scraping_mapping.get("url", None), scraping_mapping.get("headers", None)
    )

    if (not data or len(data) == 0) and scraping_mapping.get("scraping_except", None):
        logger.info("Data scraping failed. Try extracting data from another source.")
        data = scraping(scraping_mapping.get("scraping_except").get('url'),
                        scraping_mapping.get("headers"))

        have_replace = scraping_mapping.get("scraping_except", False).get('replace_decimal')
        columns_order = scraping_mapping.get("scraping_except").get('columns_order')
        dat_ref_format = scraping_mapping.get("scraping_except").get('dat_ref_format')

    df = pd.DataFrame(data)

    df_transformed = _transform_html_table(df, columns_order, dat_ref_format, have_replace)
    logger.info("Data transformed successfully")

    if not process_full_data:
        df_transformed = df_transformed[df_transformed["dat_ref"] == odate]

        if len(df_transformed) == 0 and scraping_mapping.get("scraping_except", None):
            df_transformed = _retry_scraping_odate(odate, scraping_mapping.get("scraping_except"), scraping_mapping.get("headers"))

        logger.info("Filtered data for date '%s': %d records", odate, len(df_transformed))

    return df_transformed


def _retry_scraping_odate(odate, scraping_mapping, headers):
    """Tenta re-extrair dados para uma data específica."""
    logger.info("Retrying data extraction for date: %s", odate)

    data = scraping(scraping_mapping.get('url'), headers)
    df = pd.DataFrame(data)

    df_transformed = _transform_html_table(df,
                                           scraping_mapping.get('columns_order'),
                                           scraping_mapping.get('dat_ref_format'),
                                           scraping_mapping.get('replace_decimal'))

    return df_transformed[df_transformed["dat_ref"] == odate]


def _transform_html_table(raw_data: pd.DataFrame, columns_order: list, dat_format: str, replace_decimal: bool = False) -> pd.DataFrame:
    """Transformação de dados html."""
    df = raw_data.copy()
    if len(df.columns) == 7:
        df.drop(columns=5, inplace=True)

    df.columns = columns_order
    df["dat_ref"] = pd.to_datetime(df["dat_ref"], format=dat_format).dt.strftime("%Y-%m-%d")
    df = df.sort_values("dat_ref", ascending=False).reset_index(drop=True)
    df["change_percentage"] = df["change_percentage"].str.replace("%", "", regex=False)

    columns_order.remove("dat_ref")

    for col in columns_order:
        if replace_decimal:
            df[col] = (
                df[col]
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def extract_transform_api_yf(ticker: str, columns_mapping: Dict[str, str], parameters: dict) -> pd.DataFrame:
    """Extrai e transforma dados de ações usando a API do yfinance."""
    odate = parameters.get("odate")
    environment = parameters.get("environment", "production")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("Yahoo Finance - Ticker: %s, Data: %s, Environment: %s, Full Data: %s", ticker, odate, environment, process_full_data)

    if environment == "test":
        return _make_dataframe_test_yf(odate)

    start_date = "2017-01-01" if process_full_data is True else odate
    df = yf.download(ticker, start=start_date, auto_adjust=False)
    logger.info("Data collected successfully: %d", len(df))

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df = df.reset_index(inplace=False)
    df = df.rename(columns=columns_mapping)
    logger.info("Data transformed successfully")

    if not process_full_data:
        df["dat_ref"] = df["dat_ref"].dt.strftime("%Y-%m-%d")
        df = df[df["dat_ref"] == odate]
        logger.info("Filtered Yahoo Finance data for date '%s': %d records", odate, len(df))

    return df


def extract_transform_infomoney(mapping_class: Dict[str, str], parameters: dict) -> pd.DataFrame:
    """Extrai e transforma dados do InfoMoney."""
    odate = parameters.get("odate")
    environment = parameters.get("environment", "production")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("InfoMoney - Odate: %s, Environment: %s, Full Data: %s", odate, environment, process_full_data)

    if environment == "test":
        return _make_dataframe_test_news(odate, "InfoMoney")

    df = pd.DataFrame(
        scraping_infomoney(mapping_class.get("url"), mapping_class.get("class_"))
    )
    logger.info("Data collected successfully from URL: %s - Data collected: %d", mapping_class.get("url"), len(df))

    df[["categoria", "titulo", "data_publicacao"]] = df["titulo"].apply(extrair_campos)
    df["dat_ref"] = pd.to_datetime(df["dat_ref"]).dt.strftime("%Y-%m-%d")
    df = df[df["categoria"] != "Esportes"]
    df = select_cast_midia(df)
    logger.info("Data transformed successfully")

    if not process_full_data:
        df = df[df["dat_ref"] == odate]
        logger.info("Filtered InfoMoney data for date '%s': %d records", odate, len(df))

    return df


def extract_transform_valorinveste(mapping_class: Dict[str, str], parameters: dict) -> pd.DataFrame:
    """Extrai e transforma dados do Valor Investe."""
    odate = parameters.get("odate")
    environment = parameters.get("environment", "production")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("ValorInveste - Odate: %s, Environment: %s, Full Data: %s", odate, environment, process_full_data)

    if environment == "test":
        return _make_dataframe_test_news(odate, "ValorInveste")

    df = pd.DataFrame(
        scraping_valorinveste(
            mapping_class.get("url"),
            mapping_class.get("class_post"),
            mapping_class.get("class_date"),
        )
    )
    logger.info("Data collected successfully from URL: %s - Data collected: %d", mapping_class.get("url"), len(df))

    df["dat_ref"] = df["link"].apply(extrair_data_url)
    df = select_cast_midia(df)
    df["dat_ref"] = pd.to_datetime(df["dat_ref"], format="%Y/%m/%d").dt.strftime("%Y-%m-%d")
    logger.info("Data transformed successfully")

    if not process_full_data:
        df = df[df["dat_ref"] == odate]
        logger.info("Filtered ValorInveste data for date '%s': %d records", odate, len(df))

    return df


def extract_transform_seudinheiro(mapping_class: Dict[str, str], parameters: dict) -> pd.DataFrame:
    """Extrai e transforma dados do Seu Dinheiro."""
    odate = parameters.get("odate")
    environment = parameters.get("environment", "production")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("SeuDinheiro - Odate: %s, Environment: %s, Full Data: %s", odate, environment, process_full_data)

    if environment == "test":
        return _make_dataframe_test_news(odate, "SeuDinheiro")

    max_pages = mapping_class.get("max_pages_full", 10) if process_full_data else mapping_class.get("max_pages", 5)
    all_data = []

    for page in range(1, max_pages + 1):
        url = f"{mapping_class.get('url')}{mapping_class.get('pattern_pages').replace('<number>', str(page))}"
        logger.info("Scraping page: %s", url)
        data = scraping_seudinheiro(
            url,
            mapping_class.get("class_feed"),
            mapping_class.get("class_title"),
            mapping_class.get("class_date"),
        )
        if not data:
            logger.info("No data found on page: %s", url)
            continue

        all_data.extend(data)

    df = pd.DataFrame(all_data)
    logger.info("Data collected successfully from URL: %s - Data collected: %d", mapping_class.get("url"), len(df))

    if not df.empty:
        df["dat_ref"] = df["dat_ref"].apply(parse_data_portugues)
        df = select_cast_midia(df)
        logger.info("Data transformed successfully")

        if not process_full_data:
            df = df[df["dat_ref"] == odate]
            logger.info("Filtered SeuDinheiro data for date '%s': %d records", odate, len(df))

        return df


def extract_transform_moneytimes(mapping_class: Dict[str, str], parameters: dict) -> pd.DataFrame:
    """Extrai e transforma dados do MoneyTimes."""
    odate = parameters.get("odate")
    environment = parameters.get("environment", "production")
    process_full_data = parameters.get("process_full_data", False)
    logger.info("MoneyTimes - Odate: %s, Environment: %s, Full Data: %s", odate, environment, process_full_data)

    if environment == "test":
        return _make_dataframe_test_news(odate, "MoneyTimes")

    max_pages = mapping_class.get("max_pages_full", 10) if process_full_data else mapping_class.get("max_pages", 5)
    all_data = []

    for page in range(1, max_pages + 1):
        url = f"{mapping_class.get('url')}{mapping_class.get('pattern_pages').replace('<number>', str(page))}"
        logger.info("Scraping page: %s", url)

        data = scraping_moneytimes(
            url,
            mapping_class.get("class_item"),
            mapping_class.get("class_title"),
            mapping_class.get("class_date"),
        )
        if not data:
            logger.info("No data found on page: %s", url)
            continue

        all_data.extend(data)

    df = pd.DataFrame(all_data)
    logger.info("Data collected successfully from URL: %s - Data collected: %d", mapping_class.get("url"), len(df))

    if not df.empty:
        df["dat_ref"] = df["dat_ref"].apply(data_relativa_para_absoluta)
        df = select_cast_midia(df)
        df['dat_ref'].fillna(datetime.today().strftime('%Y-%m-%d'), inplace=True)  # para notícias recém publicadas
        logger.info("Data transformed successfully")

    if not process_full_data:
        df = df[df["dat_ref"] == odate]
        logger.info("Filtered MoneyTimes data for date '%s': %d records", odate, len(df))

    return df


def _make_dataframe_test_news(odate: str, context) -> pd.DataFrame:
    """Cria um DataFrame de teste."""
    logger.info("Running in test environment, returning test data for news.")
    return pd.DataFrame({"dat_ref": [odate],
                         "fonte": [context],
                         "titulo": ["Titulo de Test"],
                         "link": [f"link_test_{context}.com"],
                         })


def _make_dataframe_test_wbf(odate: str) -> pd.DataFrame:
    """Cria um DataFrame de teste para dados de mercado."""
    logger.info("Running in test environment, returning test data for market.")
    return pd.DataFrame({"dat_ref": [odate],
                         "close_price": [999.99],
                         "open_price": [9999],
                         "high_price": [9999],
                         "low_price": [9999],
                         "change_percentage": [9999]
                         })


def _make_dataframe_test_yf(odate: str) -> pd.DataFrame:
    """Cria um DataFrame de teste para dados do Yahoo Finance."""
    logger.info("Running in test environment, returning test data for Yahoo Finance.")
    return pd.DataFrame({"dat_ref": [odate],
                         "close": [999.99],
                         "high": [9999],
                         "low": [9999],
                         "vpen": [9999],
                         "volume": [9999]
                         })
