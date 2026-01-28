"""
This is a boilerplate pipeline 'data_validation'
generated using Kedro 0.19.14
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def validate_mercado_consistency(data_consolidated_mercado: pd.DataFrame, dim_tempo: pd.DataFrame, parameters) -> pd.DataFrame:
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)

    df_all = data_consolidated_mercado.copy()
    df_all["dat_ref"] = pd.to_datetime(df_all["dat_ref"], errors="coerce")

    # filtrar por odate quando apropriado
    if not process_full_data:
        df_all = df_all.loc[df_all["dat_ref"] == pd.to_datetime(odate, errors="coerce")]

    cods = df_all["cod_indice"].dropna().unique().tolist()

    rows = []
    for cod in sorted(cods):
        sub = df_all.loc[df_all["cod_indice"] == cod].copy()
        n_rows = len(sub)

        # stats de datas
        date_min = sub["dat_ref"].min() if n_rows > 0 else None
        date_max = sub["dat_ref"].max() if n_rows > 0 else None
        unique_dates = int(sub["dat_ref"].nunique()) if n_rows > 0 else 0

        # nulls em val_fechamento
        null_count = int(sub["val_fechamento"].isna().sum()) if "val_fechamento" in sub.columns else None
        null_pct = 100.0 * null_count / n_rows if n_rows > 0 and null_count is not None else None

        # volumes negativos
        neg_volume = None
        if "volume" in sub.columns:
            try:
                neg_volume = int((pd.to_numeric(sub["volume"], errors="coerce") < 0).sum())
            except Exception:
                neg_volume = None

        notes = []
        ok = True
        if null_pct is not None and null_pct > 0:
            notes.append(f"{null_count} nulls ({null_pct:.2f}%)")
            ok = False

        if neg_volume and neg_volume > 0:
            notes.append(f"{neg_volume} negative volumes")
            ok = False

        row = {
            "cod_indice": cod,
            "dat_ref": odate,
            "n_rows": int(n_rows),
            "ok": bool(ok),
            "date_min": pd.Timestamp(date_min).isoformat() if date_min is not None else None,
            "date_max": pd.Timestamp(date_max).isoformat() if date_max is not None else None,
            "unique_dates": int(unique_dates),
            "null_count": int(null_count) if null_count is not None else None,
            "null_pct": round(null_pct, 2) if null_pct is not None else None,
            "negative_volume_count": int(neg_volume) if neg_volume is not None else None,
            "note": "; ".join(notes) if notes else "",
        }
        rows.append(row)

    df_stats = pd.DataFrame(rows)

    return df_stats

def validate_indicadores_consistency(
    indicador_risco_credito: pd.DataFrame,
    indicador_retorno_mercado: pd.DataFrame,
    indicador_volatilidade_mercado: pd.DataFrame,
    indicador_atividade_mercado: pd.DataFrame,
    indicador_confianca_mercado_local: pd.DataFrame,
    indicador_sentimento_noticias: pd.DataFrame,
    parameters,
) -> pd.DataFrame:
    odate = parameters.get("odate")
    process_full_data = parameters.get("process_full_data", False)
    
    indicadores = [
        ("RISCO_CREDITO", indicador_risco_credito, "score_risco_credito"),
        ("RETORNO_MERCADO", indicador_retorno_mercado, "score_retorno_mercado"),
        ("VOLATILIDADE_MERCADO", indicador_volatilidade_mercado, "score_volatilidade_mercado"),
        ("ATIVIDADE_MERCADO", indicador_atividade_mercado, "score_atividade_mercado"),
        ("CONFIANCA_LOCAL", indicador_confianca_mercado_local, "score_confianca_mercado"),
        ("SENTIMENTO_NOTICIAS", indicador_sentimento_noticias, "score_noticias"),
    ]

    rows = []
    for name, df, score_col in indicadores:
        row = {
            "indicator": name,
            "score_col": score_col,
            "ok": True,
            "dat_ref": odate,
        }

        df['dat_ref'] = pd.to_datetime(df['dat_ref'], errors="coerce")

        n_rows = len(df)
        null_count = int(df[score_col].isna().sum())
        null_pct = 100.0 * null_count / n_rows if n_rows > 0 else None

        valid_mask = df[score_col].notna()
        valid_vals = df.loc[valid_mask, score_col]

        # basic stats
        val_min = valid_vals.min() if not valid_vals.empty else None
        val_max = valid_vals.max() if not valid_vals.empty else None
        val_mean = valid_vals.mean() if not valid_vals.empty else None
        val_std = valid_vals.std() if not valid_vals.empty else None

        unique_dates = None
        date_min = None
        date_max = None

        unique_dates = int(df['dat_ref'].nunique())
        date_min = df['dat_ref'].min()
        date_max = df['dat_ref'].max()
        
        # aggregate row
        row.update({
            "n_rows": int(n_rows),
            "unique_dates": unique_dates,
            "date_min": pd.Timestamp(date_min).isoformat() if date_min is not None else None,
            "date_max": pd.Timestamp(date_max).isoformat() if date_max is not None else None,
            "null_count": int(null_count),
            "null_pct": round(null_pct, 2) if null_pct is not None else None,
            "min": float(val_min) if val_min is not None else None,
            "max": float(val_max) if val_max is not None else None,
            "mean": float(val_mean) if val_mean is not None else None,
            "std": float(val_std) if val_std is not None else None,
        })

        # flags/notes
        notes = []
        if null_pct is not None and null_pct > 0:
            notes.append(f"high null pct: {null_pct:.1f}%")

        if int(null_count) > 0:
            row["ok"] = False

        if notes:
            row["ok"] = False
            row["note"] = "; ".join(notes)

        rows.append(row)

    df_stats = pd.DataFrame(rows)

    return df_stats


def validate_ismb_index(indice_isbm: pd.DataFrame, parameters) -> pd.DataFrame:
    """Validação para o índice ISMB."""
    odate = parameters.get("odate")
    params = parameters or {}

    row = {
        "indicator": 'indice_ismb',
        "ok": True,
    }

    df = indice_isbm.copy()
    df['dat_ref'] = pd.to_datetime(df['dat_ref'], errors="coerce")
    df['indice_ismb'] = pd.to_numeric(df['indice_ismb'], errors="coerce")
    
    n_rows = len(df)
    null_count = int(df['indice_ismb'].isna().sum())

    valid_mask = df['indice_ismb'].notna()
    valid_vals = df.loc[valid_mask, 'indice_ismb']

    val_min = valid_vals.min() if not valid_vals.empty else None
    val_max = valid_vals.max() if not valid_vals.empty else None
    val_mean = valid_vals.mean() if not valid_vals.empty else None
    val_std = valid_vals.std() if not valid_vals.empty else None

    # out of range
    out_of_range = 0
    mask_range = pd.Series(True, index=valid_vals.index)
    mask_range &= (valid_vals >= float(0))
    mask_range &= (valid_vals <= float(100))
    out_of_range = int((~mask_range).sum())

    # duplicates and coverage
    unique_dates = int(df['dat_ref'].nunique())
    date_min = df['dat_ref'].min()
    date_max = df['dat_ref'].max()

    # flags/notes
    notes = []
    if null_count is not None and null_count > 0:
        notes.append(f"null count: {null_count}")

    if out_of_range and out_of_range > 0:
        notes.append(f"{out_of_range} values out of range")

    if notes:
        row["ok"] = False

    # build row
    row.update({
        "dat_ref": odate,
        "n_rows": int(n_rows),
        "unique_dates": unique_dates,
        "date_min": pd.Timestamp(date_min).isoformat() if date_min is not None else None,
        "date_max": pd.Timestamp(date_max).isoformat() if date_max is not None else None,
        "null_count": int(null_count),
        "out_of_range": int(out_of_range),
        "min": float(val_min) if val_min is not None else None,
        "max": float(val_max) if val_max is not None else None,
        "mean": float(val_mean) if val_mean is not None else None,
        "std": float(val_std) if val_std is not None else None,
        "note": "; ".join(notes) if notes else "",
    })

    return pd.DataFrame([row])
