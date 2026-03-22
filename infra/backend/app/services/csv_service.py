"""
CSV reading layer — pure I/O, no routing or business logic.

All public readers are decorated with @cached(ttl=300) so repeated requests
within a 5-minute window hit an in-process store instead of the filesystem.
"""

import csv
from pathlib import Path
from typing import Any, Dict, List

from ..core.cache import cached
from ..core.config import DataPaths


# ── low-level reader ──────────────────────────────────────────────────────

def _read_csv_file(path: Path) -> List[Dict[str, Any]]:
    """Read a CSV, coercing numeric strings to float and blanks to None."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(str(path))

    data: List[Dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            for k, v in list(row.items()):
                if v is None or v == "":
                    row[k] = None
                    continue
                if k.lower().startswith("dat") or k.lower().startswith("date"):
                    # Preserve date strings as-is
                    row[k] = v
                    continue
                try:
                    row[k] = float(v)
                except (ValueError, TypeError):
                    row[k] = v
            data.append(dict(row))
    return data


# ── cached domain readers ─────────────────────────────────────────────────

@cached(ttl=300)
def read_indice() -> List[Dict[str, Any]]:
    return _read_csv_file(DataPaths.INDICE_PATH)


@cached(ttl=300)
def read_serie_temporal() -> List[Dict[str, Any]]:
    return _read_csv_file(DataPaths.SERIE_TEMPORAL_PATH)


@cached(ttl=300)
def read_correlacao() -> List[Dict[str, Any]]:
    return _read_csv_file(DataPaths.CORRELACAO_PATH)


@cached(ttl=300)
def read_kpis() -> List[Dict[str, Any]]:
    return _read_csv_file(DataPaths.KPIS_PATH)


@cached(ttl=300)
def read_dashboard_diario() -> List[Dict[str, Any]]:
    return _read_csv_file(DataPaths.DASHBOARD_DIARIO_PATH)


@cached(ttl=300)
def read_quality() -> List[Dict[str, Any]]:
    if not DataPaths.QUALITY_PATH.exists():
        return []
    return _read_csv_file(DataPaths.QUALITY_PATH)


@cached(ttl=300)
def read_indicador_full(nome: str) -> List[Dict[str, Any]]:
    filename = DataPaths.INDICATOR_FILES.get(nome.lower())
    if not filename:
        raise FileNotFoundError(nome)
    return _read_csv_file(DataPaths.INDICATORS_DIR / filename)


@cached(ttl=300)
def read_mercado() -> List[Dict[str, Any]]:
    return _read_csv_file(DataPaths.MERCADO_PATH)


# ── analytics helpers (uncached — directory listing is fast) ──────────────

def list_analytics() -> List[str]:
    if not DataPaths.ANALYTICS_DIR.exists():
        return []
    return [p.name for p in sorted(DataPaths.ANALYTICS_DIR.glob("*.csv"))]


def read_analytics(filename: str) -> List[Dict[str, Any]]:
    """Read a file from the analytics directory by (safe) filename."""
    if Path(filename).name != filename:
        raise FileNotFoundError(filename)
    p = DataPaths.ANALYTICS_DIR / filename
    if not p.exists():
        p = p.with_suffix(".csv")
    if not p.exists():
        raise FileNotFoundError(filename)
    return _read_csv_file(p)


def get_indicador(nome: str) -> List[Dict[str, Any]]:
    """Legacy fuzzy search inside the analytics directory."""
    if not DataPaths.ANALYTICS_DIR.exists():
        raise FileNotFoundError(str(DataPaths.ANALYTICS_DIR))
    for p in DataPaths.ANALYTICS_DIR.glob("*.csv"):
        if p.stem.lower() == nome.lower() or nome.lower() in p.name.lower():
            return _read_csv_file(p)
    raise FileNotFoundError(nome)
