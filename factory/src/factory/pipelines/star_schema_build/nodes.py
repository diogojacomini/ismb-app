"""
This is a boilerplate pipeline 'star_schema_build'
generated using Kedro 0.19.14
"""
import logging
import pandas as pd
from workalendar.america import Brazil
from datetime import date

logger = logging.getLogger(__name__)


def build_dim_tempo(parameters: pd.DataFrame) -> pd.DataFrame:
    """Constrói a dimensão Tempo. 1 registro por dia.

    Atributos:
        sk_tempo (int): Surrogate key (YYYYMMDD).
        dat_ref (str): Data de referência (YYYY-MM-DD).
        ano (int): Ano.
        mes (int): Mês.
        dia_mes (int) Dia.
        nome_mes (str): Nome do mês.
        nome_dia_semana (str): Nome do dia da semana.
        dia_semana (int): Dia da semana (0=Segunda, 6=Domingo).
        trimestre (int): Trimestre.
        semana_ano (int): Semana. 
        dia_util (int): Indica se é dia útil 1=sim, 0=nao.
        feriado (int): Indica se é feriado 1=sim, 0=nao.
        nome_feriado (str): Nome do feriado. 
        fim_semana (int): Indica se é fim de semana 1=sim, 0=nao.
    """
    environment = parameters.get("environment", "production")

    date_start = parameters.get('dim_tempo').get('date_start')
    year_range = parameters.get('dim_tempo').get('year_range')

    if environment == 'test':
        date_start = '2025-01-01'
        year_range = 0

    # Range de datas
    dates = pd.date_range(start=date_start, end=f'{date.today().year + year_range}-01-01', freq='D')
    years = range(pd.to_datetime(date_start).year, date.today().year + year_range)

    # Feriados
    cal = Brazil()
    feriados = [cal.holidays(y) for y in years]
    feriados = [{'dat_ref': str(dt), 'feriado': 1, 'nome_feriado': nome} for year in feriados for dt, nome in year]
    df_feriados = pd.DataFrame(feriados)

    # Tabela dimensão
    df_dim = pd.DataFrame({'dat_ref': dates})
    df_dim['dat_ref'] = pd.to_datetime(df_dim['dat_ref'])

    # SK YYYYMMDD
    df_dim['sk_tempo'] = df_dim['dat_ref'].dt.strftime('%Y%m%d').astype(int)

    # Atributos temporais
    df_dim['ano'] = df_dim['dat_ref'].dt.year
    df_dim['mes'] = df_dim['dat_ref'].dt.month
    df_dim['trimestre'] = df_dim['dat_ref'].dt.quarter
    df_dim['dia_semana'] = df_dim['dat_ref'].dt.dayofweek
    df_dim['dia_mes'] = df_dim['dat_ref'].dt.day
    df_dim['semana_ano'] = df_dim['dat_ref'].dt.isocalendar().week

    dias_semanas = {0:'Segunda', 1:'Terça', 2:'Quarta', 3:'Quinta', 4:'Sexta', 5:'Sábado', 6:'Domingo'}
    meses = {1:'Janeiro', 2:'Fevereiro', 3:'Março', 4:'Abril', 5:'Maio', 6:'Junho', 7:'Julho', 8:'Agosto', 9:'Setembro', 10:'Outubro', 11:'Novembro', 12:'Dezembro'}

    df_dim['nome_dia_semana'] = df_dim['dia_semana'].map(dias_semanas)
    df_dim['nome_mes'] = df_dim['mes'].map(meses)

    df_dim['fim_semana'] = (df_dim['dia_semana'] >= 5).astype(int)

    df_dim['dat_ref'] = df_dim['dat_ref'].dt.strftime('%Y-%m-%d')

    # Adicionando feriados
    df_dim = pd.merge(df_dim, df_feriados, how='left', on='dat_ref')
    df_dim.fillna(value={'feriado': 0, 'nome_feriado': ''}, inplace=True)

    # Flag dia util
    df_dim['dia_util'] = ((df_dim['fim_semana'] == 0) & (df_dim['feriado'] == 0)).astype(int)

    df_dim = df_dim.sort_values('sk_tempo').reset_index(drop=True)

    return df_dim[['sk_tempo', 'dat_ref', 'ano', 'mes', 'dia_mes', 'nome_mes', 'nome_dia_semana',
                   'dia_semana', 'trimestre', 'semana_ano', 'dia_util', 'feriado', 'nome_feriado', 'fim_semana']]


def build_dim_indice(parameters: dict) -> pd.DataFrame:
    """Constrói a dimensão Índice. 1 registro por tipo de índice.

    Atributos:
        sk_indice (int): Surrogate key.
        cod_indice (str): Código do índice (CDS, IBOV, IVVB, IFIX).
        nome_indice (str): Nome do índice.
        categoria (str): Categoria do índice.
        descricao (str): Descrição do índice.

    """
    environment = parameters.get("environment", "production")
    if environment == "test":
        return pd.DataFrame({"sk_indice": [1],
                            "cod_indice": ['CDS'],
                            "nome_indice": ['Credit Default Swap Brasil 5 Anos'],
                            "categoria": ['Risco de credito'],
                            "descricao": ['Medida de risco de crédito do Brasil']
                            })

    indices = []
    for k in parameters.get('indices').keys():
        indices.append(parameters.get('indices').get(k))

    return pd.DataFrame(indices)


def build_fonte_noticia(parameters: dict) -> pd.DataFrame:
    """Constrói a dimensão Fonte de Notícias. 1 registro por fonte.

    Atributos:
        sk_fonte (int): Surrogate key.
        cod_fonte (str): Código da fonte.
        nome_fonte (str): Nome da fonte.
        url (str): URL da fonte.
        confiabilidade (str): Nível de confiabilidade da fonte.

    """
    environment = parameters.get("environment", "production")
    if environment == "test":
        return pd.DataFrame({"sk_fonte": [1],
                            "cod_fonte": ['INFOMONEY'],
                            "nome_fonte": ['InfoMoney'],
                            "url": ['https://www.infomoney.com.br/'],
                            "confiabilidade": ['Alta']
                            })

    fontes = []
    for k in parameters.get('fontes_noticias').keys():
        fontes.append(parameters.get('fontes_noticias').get(k))

    return pd.DataFrame(fontes)


def build_dim_indicador(parameters: dict) -> pd.DataFrame:
    """Constrói a dimensão Tipo de Indicador. 1 registro por tipo de indicador.

    Atributos:
        sk_indicador (int): Surrogate key.
        cod_indicador (str): Código do tipo de indicador.
        nome_indicador (str): Nome do tipo de indicador.
        descricao (str): Descrição do tipo de indicador.
        peso_ismb (float): Peso do indicador no cálculo do ISMB.

    """
    environment = parameters.get("environment", "production")
    if environment == "test":
        return pd.DataFrame({"sk_indicador": [1],
                            "cod_indicador": ['RISCO_CREDITO'],
                            "nome_indicador": ['Risco_de_credito'],
                            "descricao": ['Indicador baseado em CDS e volatilidade'],
                            "peso_ismb": [0.25]
                            })

    indicadores = []
    for k in parameters.get('indicadores').keys():
        indicadores.append(parameters.get('indicadores').get(k))

    return pd.DataFrame(indicadores)
