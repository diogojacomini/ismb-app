"""
This is a boilerplate pipeline 'data_analytics'
generated using Kedro 0.19.14
"""
import logging
import pandas as pd
import numpy as np
import math

logger = logging.getLogger(__name__)


def build_dash_diario(df_consolidado_mercado, df_dim_tempo, df_tim_indice, df_ismb, parameters) -> pd.DataFrame:
    """Constrói datamart diário consolidado por índice."""
    odate_param = parameters.get("odate")

    # defensivas
    if df_consolidado_mercado is None or df_consolidado_mercado.empty:
        logger.warning("build_dash_diario: df_consolidado_mercado vazio -> retorna DataFrame vazio")
        return pd.DataFrame()

    df = df_consolidado_mercado.copy()
    # detectar coluna de close possível
    close_col = 'val_fechamento'
    vol_col = "qtd_volume"

    # coerções
    df["dat_ref"] = pd.to_datetime(df["dat_ref"], errors="coerce")
    dim = df_dim_tempo[df_dim_tempo['dia_util'] == 1]
    dim["dat_ref"] = pd.to_datetime(dim["dat_ref"], errors="coerce")

    odate = pd.to_datetime(odate_param).normalize() if odate_param is not None else None

    # determinar prev trading day
    if dim is not None:
        cand = dim.loc[(dim["dat_ref"] < odate) & (dim.get("dia_util", 1) == 1), "dat_ref"].dropna().sort_values()
        if not cand.empty:
            prev_date = cand.max().normalize()

    # selecionar linhas do dia e do dia anterior
    df_day = df.loc[df["dat_ref"].dt.normalize() == odate].copy()
    df_prev = df.loc[df["dat_ref"].dt.normalize() == prev_date].copy()

    # agregar último por índice se houver múltiplas linhas no dia (usar última por dat_ref/hora)
    if close_col:
        df_day_last = df_day.sort_values(["cod_indice", "dat_ref"]).groupby("cod_indice", as_index=False).last()
        df_prev_last = df_prev.sort_values(["cod_indice", "dat_ref"]).groupby("cod_indice", as_index=False).last()
    else:
        # sem close coluna, apenas pegar última linha por índice
        df_day_last = df_day.sort_values(["cod_indice", "dat_ref"]).groupby("cod_indice", as_index=False).last()
        df_prev_last = df_prev.sort_values(["cod_indice", "dat_ref"]).groupby("cod_indice", as_index=False).last()

    # construir tabela base
    result = pd.DataFrame()
    result["cod_indice"] = df_day_last["cod_indice"]

    # close, prev_close
    if close_col:
        result["close"] = pd.to_numeric(df_day_last.get(close_col), errors="coerce")
        prev_map = df_prev_last.set_index("cod_indice")[close_col].to_dict() if not df_prev_last.empty else {}
        result["prev_close"] = result["cod_indice"].map(prev_map).astype(float)
        # changes
        result["abs_change"] = result["close"] - result["prev_close"]
        result["pct_change"] = np.where(result["prev_close"].replace(0, np.nan).notna(),
                                        (result["abs_change"] / result["prev_close"].replace(0, np.nan)) * 100,
                                        np.nan)
    else:
        result["close"] = None
        result["prev_close"] = None
        result["abs_change"] = None
        result["pct_change"] = None

    # volume
    if vol_col:
        vol_map = df_day_last.set_index("cod_indice")[vol_col].to_dict()
        result["volume"] = result["cod_indice"].map(vol_map).astype("Int64")
    else:
        result["volume"] = None

    # dat_ref, dia_util
    result["dat_ref"] = odate.strftime("%Y-%m-%d")
    if dim is not None:
        dia_util_map = dim.set_index(dim["dat_ref"].dt.normalize())["dia_util"].to_dict()
        result["dia_util"] = int(dia_util_map.get(odate, dim.get("dia_util", 1).iloc[0]) if "dia_util" in dim.columns else 1)
    else:
        result["dia_util"] = 1

    # merge indicadores (se tiver cod_indice em df_tim_indice)
    if isinstance(df_tim_indice, pd.DataFrame) and "cod_indice" in df_tim_indice.columns:
        # prefix indicator cols to avoid clashes
        ind = df_tim_indice.copy()
        # se houver dat_ref em ind, filtrar por odate quando possível
        if "dat_ref" in ind.columns:
            ind["dat_ref"] = pd.to_datetime(ind["dat_ref"], errors="coerce")
            if odate in ind["dat_ref"].dt.normalize().values:
                ind = ind.loc[ind["dat_ref"].dt.normalize() == odate]
        # drop dat_ref to merge
        if "dat_ref" in ind.columns:
            ind = ind.drop(columns=["dat_ref"])
        # rename non-key cols
        non_key = [c for c in ind.columns if c != "cod_indice"]
        ind = ind.rename(columns={c: f"ind_{c}" for c in non_key})
        result = result.merge(ind, on="cod_indice", how="left")
    else:
        # nothing to merge
        pass

    # ISMB context: pick value at odate and prev_date
    ismb_val = None
    ismb_prev = None
    if isinstance(df_ismb, pd.DataFrame) and "dat_ref" in df_ismb.columns:
        tmp_ismb = df_ismb.copy()
        tmp_ismb["dat_ref"] = pd.to_datetime(tmp_ismb["dat_ref"], errors="coerce")
        s_today = tmp_ismb.loc[tmp_ismb["dat_ref"].dt.normalize() == odate]
        s_prev = tmp_ismb.loc[tmp_ismb["dat_ref"].dt.normalize() == prev_date]
        if not s_today.empty:
            ismb_val = pd.to_numeric(s_today.iloc[-1]['indice_ismb'], errors="coerce")

        if not s_prev.empty:
            ismb_prev = pd.to_numeric(s_prev.iloc[-1]['indice_ismb'], errors="coerce")

    # compute ismb changes
    ismb_abs = None
    ismb_pct = None
    if ismb_val is not None:
        ismb_abs = None if ismb_prev is None else ismb_val - ismb_prev
        ismb_pct = None if ismb_prev is None or ismb_prev == 0 else (ismb_abs / ismb_prev) * 100

    result["ismb_value"] = ismb_val
    result["ismb_abs_change"] = ismb_abs
    result["ismb_pct_change"] = ismb_pct

    result['dat_ref'] = odate.strftime("%Y-%m-%d")
    final = result

    # select
    cols_order = ["cod_indice", "dat_ref", "close", "prev_close", "abs_change", "pct_change", "volume",
                  "ismb_value", "ismb_abs_change", "ismb_pct_change"]

    other_cols = [c for c in final.columns if c not in cols_order]
    final = final[cols_order + other_cols]
    final.drop(columns=["dia_util"], inplace=True, errors="ignore")
    
    final = final.sort_values("cod_indice").reset_index(drop=True)
    logger.info("build_dash_diario: produced %d rows for dateref %s", len(final), odate.strftime("%Y-%m-%d"))

    return final


def build_serie_temporal_ismb(df_ismb, df_dim_tempo, parameters) -> pd.DataFrame:
    """
    Constroi serie temporal completa do ISMB com features:
    - value, daily_return
    - moving averages (ma_short, ma_long)
    - bandas de bollinger (bb_mid, bb_upper, bb_lower)
    - rolling volatility (annualized) e rolling mean/std
    """
    if df_ismb is None or df_ismb.empty:
        return pd.DataFrame()

    df = df_ismb.copy()
    df["dat_ref"] = pd.to_datetime(df["dat_ref"], errors="coerce")
    df = df.sort_values("dat_ref").dropna(subset=["dat_ref"]).reset_index(drop=True)

    # detectar coluna de valor
    val_col = next((c for c in ["value", "close", "val_fechamento", "price"] if c in df.columns), None)
    if val_col is None:
        # pegar primeira numérica diferente de dat_ref
        numeric_cols = [c for c in df.columns if c != "dat_ref" and pd.api.types.is_numeric_dtype(df[c])]
        if numeric_cols:
            val_col = numeric_cols[0]
        else:
            return pd.DataFrame()

    df["value"] = pd.to_numeric(df[val_col], errors="coerce")
    df["daily_return"] = df["value"].pct_change()

    params = parameters or {}
    ma_short = int(params.get("ma_short_days", 7))
    ma_long = int(params.get("ma_long_days", 21))
    boll_window = int(params.get("boll_window", 20))
    boll_k = float(params.get("boll_k", 2.0))
    roll_window = int(params.get("rolling_window_days", 21))
    trading_days_per_year = int(params.get("trading_days_per_year", 252))

    df[f"ma_{ma_short}"] = df["value"].rolling(window=ma_short, min_periods=1).mean()
    df[f"ma_{ma_long}"] = df["value"].rolling(window=ma_long, min_periods=1).mean()

    bb_mid = df["value"].rolling(window=boll_window, min_periods=1).mean()
    bb_std = df["value"].rolling(window=boll_window, min_periods=1).std().fillna(0.0)
    df["bb_mid"] = bb_mid
    df["bb_upper"] = bb_mid + boll_k * bb_std
    df["bb_lower"] = bb_mid - boll_k * bb_std

    # rolling statistics (annualized volatility)
    df["rolling_mean"] = df["daily_return"].rolling(window=roll_window, min_periods=1).mean()
    df["rolling_std"] = df["daily_return"].rolling(window=roll_window, min_periods=1).std().fillna(0.0)
    df["rolling_vol_annual"] = df["rolling_std"] * math.sqrt(trading_days_per_year)

    # zscore of returns (useful to spot anomalies)
    df["ret_zscore"] = (df["daily_return"] - df["daily_return"].rolling(window=roll_window, min_periods=1).mean()) \
                        / df["daily_return"].rolling(window=roll_window, min_periods=1).std().replace(0, pd.NA)

    # tidy and select columns
    out_cols = [
        "dat_ref", "value", "daily_return",
        f"ma_{ma_short}", f"ma_{ma_long}",
        "bb_mid", "bb_upper", "bb_lower",
        "rolling_mean", "rolling_std", "rolling_vol_annual", "ret_zscore"
    ]
    return df[[c for c in out_cols if c in df.columns]].reset_index(drop=True)


def build_analise_correlacao(df_fato_mercado, df_dim_tempo, df_dim_indice, df_ismb, parameters) -> pd.DataFrame:
    """
    Constroi análise de correlação entre índices (e ISMB).
    Retorna tabela pairwise: idx_a, idx_b, corr, n_obs, start_date, end_date
    """
    if df_fato_mercado is None or df_fato_mercado.empty:
        return pd.DataFrame()

    params = parameters or {}
    months = int(params.get("corr_period_months", 12))
    odate = params.get("odate")
    try:
        odate_dt = pd.to_datetime(odate) if odate is not None else df_fato_mercado["dat_ref"].max()
    except Exception:
        odate_dt = df_fato_mercado["dat_ref"].max()
    start_dt = pd.to_datetime(odate_dt) - pd.DateOffset(months=months)

    df = df_fato_mercado.copy()
    df["dat_ref"] = pd.to_datetime(df["dat_ref"], errors="coerce")
    mask = (df["dat_ref"] >= start_dt) & (df["dat_ref"] <= pd.to_datetime(odate_dt))
    df = df.loc[mask].dropna(subset=["dat_ref"])

    # detectar coluna de fechamento
    close_col = next((c for c in ["val_fechamento", "close", "value", "price"] if c in df.columns), None)
    if close_col is None:
        # tentar qualquer numérico não-dat_ref, not cod_indice
        candidates = [c for c in df.columns if c not in ("dat_ref", "cod_indice") and pd.api.types.is_numeric_dtype(df[c])]
        close_col = candidates[0] if candidates else None
    if close_col is None:
        return pd.DataFrame()

    pivot = df.pivot_table(index="dat_ref", columns="cod_indice", values=close_col, aggfunc="last")
    pivot = pivot.sort_index().ffill().dropna(how="all")

    # adicionar ISMB se disponível
    if df_ismb is not None and isinstance(df_ismb, pd.DataFrame) and "dat_ref" in df_ismb.columns:
        tmp = df_ismb.copy()
        tmp["dat_ref"] = pd.to_datetime(tmp["dat_ref"], errors="coerce")
        valcol = next((c for c in ["value", "close", "val_fechamento", "price"] if c in tmp.columns), None)
        if valcol:
            tmp = tmp.set_index("dat_ref")[[valcol]]
            tmp = tmp.rename(columns={valcol: "ISMB"})
            # align by index
            pivot = pivot.merge(tmp, left_index=True, right_index=True, how="left")

    # compute correlation matrix and counts
    corr = pivot.corr(method="pearson")
    pairs = []
    cols = corr.columns.tolist()
    for i, a in enumerate(cols):
        for b in cols[i+1:]:
            valid = pivot[[a, b]].dropna()
            n_obs = len(valid)
            pairs.append({
                "idx_a": a,
                "idx_b": b,
                "corr": float(corr.loc[a, b]) if pd.notna(corr.loc[a, b]) else None,
                "n_obs": int(n_obs),
                "start_date": pd.to_datetime(start_dt).date().isoformat() if start_dt is not None else None,
                "end_date": pd.to_datetime(odate_dt).date().isoformat() if odate_dt is not None else None
            })
    df_pairs = pd.DataFrame(pairs).sort_values("corr", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
    return df_pairs


def _max_drawdown_from_returns(returns: pd.Series) -> float:
    """Calcula max drawdown a partir de series de retornos (pct, não acumulados). Retorna negativo (ex: -0.2)."""
    if returns is None or returns.empty:
        return 0.0
    # construir série de preços hipotéticos
    cum = (1 + returns.fillna(0)).cumprod()
    running_max = cum.cummax()
    drawdown = (cum / running_max) - 1.0
    return float(drawdown.min())


def build_kpis_agregados(df_ismb, df_fato_mercado, df_dim_tempo, parameters) -> pd.DataFrame:
    """Constroi KPIs agregados"""
    rows = []
    params = parameters or {}
    trading_days = int(params.get("trading_days_per_year", 252))

    # ISMB KPIs
    if df_ismb is not None and not df_ismb.empty:
        tmp = df_ismb.copy()
        tmp["dat_ref"] = pd.to_datetime(tmp["dat_ref"], errors="coerce")
        val_col = next((c for c in ["value", "close", "val_fechamento", "price"] if c in tmp.columns), None)
        if val_col:
            tmp["value"] = pd.to_numeric(tmp[val_col], errors="coerce")
            tmp = tmp.dropna(subset=["dat_ref"]).sort_values("dat_ref")
            tmp["year"] = tmp["dat_ref"].dt.year
            tmp["ret"] = tmp["value"].pct_change()
            for year, group in tmp.groupby("year"):
                vals = group["value"].dropna()
                if vals.empty:
                    continue
                annual_return = (vals.iloc[-1] / vals.iloc[0] - 1.0) if len(vals) >= 1 else None
                vol = group["ret"].std() * math.sqrt(trading_days) if group["ret"].notna().sum() > 1 else 0.0
                maxdd = _max_drawdown_from_returns(group["ret"])
                rows.append({
                    "entity_type": "ISMB",
                    "entity_name": "ISMB",
                    "year": int(year),
                    "annual_return": annual_return,
                    "annual_volatility": float(vol),
                    "max_drawdown": float(maxdd),
                    "n_obs": int(len(group)),
                    "avg_volume": None
                })

    # Indices KPIs
    if df_fato_mercado is not None and not df_fato_mercado.empty:
        df = df_fato_mercado.copy()
        df["dat_ref"] = pd.to_datetime(df["dat_ref"], errors="coerce")
        close_col = next((c for c in ["val_fechamento", "close", "value", "price"] if c in df.columns), None)
        vol_col = "volume" if "volume" in df.columns else None
        if close_col:
            df["value"] = pd.to_numeric(df[close_col], errors="coerce")
            df["ret"] = df.groupby("cod_indice")["value"].pct_change()
            df["year"] = df["dat_ref"].dt.year
            grouped = df.groupby(["cod_indice", "year"])
            for (cod, year), g in grouped:
                vals = g["value"].dropna()
                if vals.empty:
                    continue
                annual_return = (vals.iloc[-1] / vals.iloc[0] - 1.0) if len(vals) >= 1 else None
                vol = g["ret"].std() * math.sqrt(trading_days) if g["ret"].notna().sum() > 1 else 0.0
                maxdd = _max_drawdown_from_returns(g["ret"])
                avg_vol = None
                if vol_col and vol_col in g.columns:
                    avg_vol = float(pd.to_numeric(g[vol_col], errors="coerce").dropna().mean()) if not g[vol_col].dropna().empty else None
                rows.append({
                    "entity_type": "INDEX",
                    "entity_name": cod,
                    "year": int(year),
                    "annual_return": annual_return,
                    "annual_volatility": float(vol),
                    "max_drawdown": float(maxdd),
                    "n_obs": int(len(g)),
                    "avg_volume": avg_vol
                })

    df_kpis = pd.DataFrame(rows)
    df_kpis = df_kpis.sort_values(["entity_type", "entity_name", "year"]).reset_index(drop=True)
    return df_kpis


def build_serie_temporal_ismb(df_ismb, df_dim_tempo, parameters) -> pd.DataFrame:
    """
    Constroi serie temporal completa do ISMB com features:
    - value, daily_return
    - moving averages (ma_short, ma_long)
    - bandas de bollinger (bb_mid, bb_upper, bb_lower)
    - rolling volatility (annualized) e rolling mean/std
    """
    params = parameters or {}
    odate = params.get("odate")

    df = df_ismb.copy()
    df["dat_ref_fmt"] = pd.to_datetime(df["dat_ref"], errors="coerce")
    df = df.sort_values("dat_ref_fmt").dropna(subset=["dat_ref_fmt"]).reset_index(drop=True).drop(columns=["dat_ref_fmt"])

    df["value"] = pd.to_numeric(df['indice_ismb'], errors="coerce")
    df["daily_return"] = df["value"].pct_change()

    ma_short = int(params.get("ma_short_days", 7))
    ma_long = int(params.get("ma_long_days", 21))
    boll_window = int(params.get("boll_window", 20))
    boll_k = float(params.get("boll_k", 2.0))
    roll_window = int(params.get("rolling_window_days", 21))
    trading_days_per_year = int(params.get("trading_days_per_year", 252))

    df[f"ma_{ma_short}"] = df["value"].rolling(window=ma_short, min_periods=1).mean()
    df[f"ma_{ma_long}"] = df["value"].rolling(window=ma_long, min_periods=1).mean()

    bb_mid = df["value"].rolling(window=boll_window, min_periods=1).mean()
    bb_std = df["value"].rolling(window=boll_window, min_periods=1).std().fillna(0.0)
    df["bb_mid"] = bb_mid
    df["bb_upper"] = bb_mid + boll_k * bb_std
    df["bb_lower"] = bb_mid - boll_k * bb_std

    # rolling statistics (annualized volatility)
    df["rolling_mean"] = df["daily_return"].rolling(window=roll_window, min_periods=1).mean()
    df["rolling_std"] = df["daily_return"].rolling(window=roll_window, min_periods=1).std().fillna(0.0)
    df["rolling_vol_annual"] = df["rolling_std"] * math.sqrt(trading_days_per_year)

    # zscore of returns (useful to spot anomalies)
    df["ret_zscore"] = (df["daily_return"] - df["daily_return"].rolling(window=roll_window, min_periods=1).mean()) \
                        / df["daily_return"].rolling(window=roll_window, min_periods=1).std().replace(0, pd.NA)

    #select columns
    out_cols = [
        "dat_ref", "value", "daily_return",
        f"ma_{ma_short}", f"ma_{ma_long}",
        "bb_mid", "bb_upper", "bb_lower",
        "rolling_mean", "rolling_std", "rolling_vol_annual", "ret_zscore"
    ]
    return df[[c for c in out_cols if c in df.columns]].reset_index(drop=True)


def build_analise_correlacao(df_fato_mercado, df_dim_tempo, df_dim_indice, df_ismb, parameters) -> pd.DataFrame:
    """Constroi análise de correlação entre índices (e ISMB). """
    params = parameters or {}
    months = int(params.get("corr_period_months", 12))
    odate = params.get("odate")

    odate_dt = pd.to_datetime(odate) if odate is not None else df_fato_mercado["dat_ref"].max()
    start_dt = pd.to_datetime(odate_dt) - pd.DateOffset(months=months)

    df = df_fato_mercado.copy()
    df = pd.merge(df, df_dim_tempo[df_dim_tempo["dia_util"] == 1], on="dat_ref", how="inner")

    df["dat_ref"] = pd.to_datetime(df["dat_ref"], errors="coerce")
    mask = (df["dat_ref"] >= start_dt) & (df["dat_ref"] <= pd.to_datetime(odate_dt))
    df = df.loc[mask].dropna(subset=["dat_ref"])

    # detectar coluna de fechamento
    close_col = 'val_fechamento'

    pivot = df.pivot_table(index="dat_ref", columns="cod_indice", values=close_col, aggfunc="last")
    pivot = pivot.sort_index().ffill().dropna(how="all")

    # adicionar ISMB
    tmp = df_ismb.copy()
    tmp["dat_ref"] = pd.to_datetime(tmp["dat_ref"], errors="coerce")
    valcol = 'indice_ismb'
    tmp = tmp.set_index("dat_ref")[[valcol]]
    tmp = tmp.rename(columns={valcol: "ISMB"})

    pivot = pivot.merge(tmp, left_index=True, right_index=True, how="left")

    # matrix de correlação
    corr = pivot.corr(method="pearson")
    pairs = []
    cols = corr.columns.tolist()
    for i, a in enumerate(cols):
        for b in cols[i+1:]:
            valid = pivot[[a, b]].dropna()
            n_obs = len(valid)
            pairs.append({
                "idx_a": a,
                "idx_b": b,
                "corr": float(corr.loc[a, b]) if pd.notna(corr.loc[a, b]) else None,
                "n_obs": int(n_obs),
                "start_date": pd.to_datetime(start_dt).date().isoformat() if start_dt is not None else None,
                "end_date": pd.to_datetime(odate_dt).date().isoformat() if odate_dt is not None else None
            })

    df_pairs = pd.DataFrame(pairs).sort_values("corr", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
    df_pairs['dat_ref'] = pd.to_datetime(odate).strftime("%Y-%m-%d")
    return df_pairs


def _max_drawdown_from_returns(returns: pd.Series) -> float:
    """Calcula max drawdown a partir de series de retornos (pct, não acumulados). Retorna negativo (ex: -0.2)."""
    if returns is None or returns.empty:
        return 0.0
    # construir série de preços hipotéticos
    cum = (1 + returns.fillna(0)).cumprod()
    running_max = cum.cummax()
    drawdown = (cum / running_max) - 1.0
    return float(drawdown.min())


def build_kpis_agregados(df_ismb: pd.DataFrame, df_fato_mercado: pd.DataFrame, df_dim_tempo: pd.DataFrame, parameters) -> pd.DataFrame:
    """Constroi KPIs para os indicadores"""
    params = parameters or {}
    odate = parameters.get("odate")
    trading_days = int(params.get("trading_days_per_year", 252))

    df_ismb = pd.merge(df_ismb, df_dim_tempo, on="dat_ref", how="left")
    df_ismb = df_ismb[df_ismb["dia_util"] == 1]
    
    rows = []
    # ISMB KPIs
    tmp = df_ismb.copy()
    tmp["dat_ref"] = pd.to_datetime(tmp["dat_ref"], errors="coerce")

    tmp["indice_ismb"] = pd.to_numeric(tmp['indice_ismb'], errors="coerce")
    tmp = tmp.dropna(subset=["dat_ref"]).sort_values("dat_ref")

    tmp["ret"] = tmp["indice_ismb"].pct_change()

    for year, group in tmp.groupby("ano"):
        vals = group["indice_ismb"].dropna()

        annual_return = (vals.iloc[-1] / vals.iloc[0] - 1.0) if len(vals) >= 1 else None
        vol = group["ret"].std() * math.sqrt(trading_days) if group["ret"].notna().sum() > 1 else 0.0
        maxdd = _max_drawdown_from_returns(group["ret"])
        rows.append({
            "entity_type": "ISMB",
            "entity_name": "ISMB",
            "year": int(year),
            "annual_return": annual_return,
            "annual_volatility": float(vol),
            "max_drawdown": float(maxdd),
            "n_obs": int(len(group)),
        })

    # Indices KPIs
    df_fato_mercado = pd.merge(df_fato_mercado, df_dim_tempo, on="dat_ref", how="left")
    df_fato_mercado = df_fato_mercado[df_fato_mercado["dia_util"] == 1]
    df = df_fato_mercado.copy()

    df["dat_ref"] = pd.to_datetime(df["dat_ref"], errors="coerce")
    df["val_fechamento"] = pd.to_numeric(df['val_fechamento'], errors="coerce")

    df["ret"] = df.groupby("cod_indice")["val_fechamento"].pct_change()
    grouped = df.groupby(["cod_indice", "ano"])

    for (cod, year), g in grouped:
        vals = g["val_fechamento"].dropna()
        if vals.empty:
            continue
        annual_return = (vals.iloc[-1] / vals.iloc[0] - 1.0) if len(vals) >= 1 else None
        vol = g["ret"].std() * math.sqrt(trading_days) if g["ret"].notna().sum() > 1 else 0.0
        maxdd = _max_drawdown_from_returns(g["ret"])

        rows.append({
            "entity_type": "INDEX",
            "entity_name": cod,
            "year": int(year),
            "annual_return": annual_return,
            "annual_volatility": float(vol),
            "max_drawdown": float(maxdd),
            "n_obs": int(len(g)),
        })

    df_kpis = pd.DataFrame(rows)
    df_kpis['dat_ref'] = pd.to_datetime(odate).strftime("%Y-%m-%d")
    df_kpis = df_kpis.sort_values(["entity_type", "entity_name", "dat_ref"]).reset_index(drop=True)

    return df_kpis
