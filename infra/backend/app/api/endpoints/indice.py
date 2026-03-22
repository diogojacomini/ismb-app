"""
Endpoints: /indice  /resumo
"""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ...services import csv_service

router = APIRouter(tags=["Índice ISMB"])

# Mapping: query key  →  CSV column(s) to return
_COL_MAP = {
    "ismb": ["indice_ismb"],
    "a": ["score_risco_credito"],
    "b": ["score_retorno_mercado"],
    "c": ["score_volatilidade_mercado"],
    "d": ["score_atividade_mercado"],
    "e": ["score_confianca_mercado"],
    "f": ["score_noticias"],
}

# Keys whose data is filtered to the last 365 days (high-frequency sub-scores)
_ONE_YEAR_KEYS = {"a", "b", "c", "d", "e"}


def _apply_date_cutoff(rows: list, days: int) -> tuple[list, object | None]:
    """Return (rows, cutoff_date) applying a trailing-N-day window."""
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


@router.get("/indice", summary="Série temporal de um índice/sub-score ISMB")
def get_indice(
    indice: Optional[str] = Query(
        None, description="ismb | A | B | C | D | E | F"
    )
):
    try:
        rows = csv_service.read_indice()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if not indice:
        return JSONResponse(content=rows)

    key = indice.strip().lower()
    if key not in _COL_MAP:
        raise HTTPException(status_code=400, detail=f"Índice inválido: '{indice}'")

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


@router.get("/resumo", summary="Últimas 2 linhas do índice ISMB (para o dashboard)")
def get_resumo():
    """Returns the two most-recent rows with all component scores.
    Used by the dashboard mini-cards and radar chart in a single call."""
    try:
        rows = csv_service.read_indice()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    sorted_rows = sorted(
        (r for r in rows if r.get("dat_ref")),
        key=lambda r: r["dat_ref"],
    )
    return JSONResponse(content=sorted_rows[-2:] if len(sorted_rows) >= 2 else sorted_rows)
