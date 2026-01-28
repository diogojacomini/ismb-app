from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from ..services import csv_service
from datetime import datetime, timedelta

router = APIRouter()


@router.get('/indice')
def api_indice(indice: Optional[str] = Query(None, description="Nome do indice: ismb, A, B, C, D, E, F")):
    try:
        rows = csv_service.ler_csv('indice')
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if not indice:
        return JSONResponse(content=rows)

    key = indice.strip().lower()

    mapping = {
        'ismb': ['indice_ismb'],
        'a': ['score_risco_credito'],
        'b': ['score_retorno_mercado'],
        'c': ['score_volatilidade_mercado'],
        'd': ['score_atividade_mercado'],
        'e': ['score_confianca_mercado'],
        'f': ['score_noticias'],
    }

    if key not in mapping:
        raise HTTPException(status_code=400, detail='indice inválido')

    candidate_cols = mapping[key]
    simplified = []

    # Para os indices A-E, filtro de 1 ano
    apply_one_year = key in {'a', 'b', 'c', 'd', 'e'}
    cutoff_date = None
    if apply_one_year:
        dates = []
        for r in rows:
            date_str = r.get('dat_ref')
            if not date_str:
                continue
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                dates.append(dt)
            except Exception:
                continue
        if dates:
            max_date = max(dates)
            cutoff_date = max_date - timedelta(days=365)

    for r in rows:
        data_val = r.get('dat_ref')

        if apply_one_year and cutoff_date is not None:
            if not data_val:
                continue
            try:
                row_date = datetime.strptime(data_val, "%Y-%m-%d").date()
            except Exception:
                continue
            if row_date < cutoff_date:
                continue

        valor = None
        for col in candidate_cols:
            if col in r and r[col] is not None:
                valor = r[col]
                break

        # Dados de noticias podem ter valores None, datas antigas
        if key == 'f' and (valor is None):
            continue

        if isinstance(valor, (int, float)):
            try:
                valor = round(float(valor), 2)
            except Exception:
                pass

        simplified.append({'data': data_val, 'valor': valor})

    return JSONResponse(content=simplified)


@router.get('/indicador/{nome}')
def api_indicador(nome: str):
    try:
        data = csv_service.get_indicador(nome)
        return JSONResponse(content=data)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail='Indicador não encontrado')


@router.get('/analytics')
def api_analytics_list():
    files = csv_service.list_analytics()
    return JSONResponse(content=files)


@router.get('/analytics/{filename}')
def api_analytics_file(filename: str):
    try:
        data = csv_service.read_analytics(filename)
        return JSONResponse(content=data)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail='Arquivo não encontrado')
