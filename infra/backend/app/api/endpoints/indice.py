"""
ISMB Index API Endpoints.

Fornece acesso ao indice ISMB principal.

Endpoints disponíveis:
    - GET /indice: Série temporal do indice ISMB
    - GET /resumo: Últimas 2 observações para dashboard
"""
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from ...services import db_service

router = APIRouter(tags=["indice ISMB"])

# Mapping: query key
_COL_MAP = {
    "ismb": ["indice_ismb"],
    "a": ["score_risco_credito"],
    "b": ["score_retorno_mercado"],
    "c": ["score_volatilidade_mercado"],
    "d": ["score_atividade_mercado"],
    "e": ["score_confianca_mercado"],
    "f": ["score_noticias"],
}

_ONE_YEAR_KEYS = {"a", "b", "c", "d", "e"}


def _apply_date_cutoff(rows: list, days: int) -> tuple[list, object | None]:
    """
    Aplica janela temporal aos dados, retornando últimos N dias.

    Args:
        rows: Lista de dicionários com campo 'dat_ref'
        days: Número de dias a partir da data mais recente

    Returns:
        Tupla (linhas filtradas, data de corte)
    """
    dates = []
    for r in rows:
        ds = r.get("dat_ref")
        if not ds:
            continue
        try:
            dates.append(datetime.strptime(ds, "%Y-%m-%d").date())
        except ValueError:
            pass
    if not dates:
        return rows, None
    cutoff = max(dates) - timedelta(days=days)
    filtered = []
    for r in rows:
        ds = r.get("dat_ref")
        if not ds:
            continue
        try:
            if datetime.strptime(ds, "%Y-%m-%d").date() >= cutoff:
                filtered.append(r)
        except ValueError:
            pass
    return filtered, cutoff


@router.get("/indice", summary="Série temporal de um indice/sub-score ISMB")
def get_indice(
    indice: Optional[str] = Query(None, description="ismb | A | B | C | D | E | F")
):
    """
    Retorna série temporal do indice ISMB ou seus indicadores.

    Sem parâmetro, retorna todas as colunas. Com parâmetro, filtra:
    - ismb: indice ISMB consolidado (0-100)
    - a: Score de Risco de Crédito
    - b: Score de Retorno do Mercado
    - c: Score de Volatilidade
    - d: Score de Atividade
    - e: Score de Confiança
    - f: Score de Sentimento de Notícias

    Args:
        indice: Código do indice/componente (opcional)

    Returns:
        JSONResponse: Lista de {data, valor} ou registros completos

    Raises:
        HTTPException 400: Código de indice inválido
        HTTPException 404: Dados não encontrados
    """
    try:
        rows = db_service.read_indice()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not indice:
        return JSONResponse(content=rows)

    key = indice.strip().lower()
    if key not in _COL_MAP:
        raise HTTPException(status_code=400, detail=f"indice inválido: '{indice}'")

    candidate_cols = _COL_MAP[key]

    if key in _ONE_YEAR_KEYS:
        rows, _ = _apply_date_cutoff(rows, 365)

    result = []
    for r in rows:
        valor = None
        for col in candidate_cols:
            if r.get(col) is not None:
                valor = r[col]
                break

        # Skip rows where noticias score is absent
        if key == "f" and valor is None:
            continue

        if isinstance(valor, (int, float)):
            valor = round(float(valor), 2)

        result.append({"data": r.get("dat_ref"), "valor": valor})

    return JSONResponse(content=result)


@router.get("/resumo", summary="Últimas 2 linhas do indice ISMB (para o dashboard)")
def get_resumo():
    """
    Retorna as duas observações mais recentes do indice ISMB.

    Usado pelo dashboard para exibir valor atual e anterior,
    incluindo todos os indicadores.

    Returns:
        JSONResponse: Lista com últimos 2 registros ordenados por data

    Raises:
        HTTPException 404: Dados não encontrados

    Example:
        GET /api/resumo

        Response:
        [
            {
                "dat_ref": "2026-03-18",
                "indice_ismb": 34.18,
                "score_risco_credito": 57.95,
                ...
            },
            {
                "dat_ref": "2026-03-19",
                "indice_ismb": 49.6,
                "score_risco_credito": 60.6,
                ...
            }
        ]
    """
    try:
        rows = db_service.read_indice()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    sorted_rows = sorted(
        (r for r in rows if r.get("dat_ref")),
        key=lambda r: r["dat_ref"],
    )
    return JSONResponse(
        content=sorted_rows[-2:] if len(sorted_rows) >= 2 else sorted_rows
    )
