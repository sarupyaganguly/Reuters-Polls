import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import lseg.data as ld
from openpyxl.chart import LineChart, BarChart, Reference
from openpyxl.styles import Font

ld.open_session()

#Insert current poll values on line 160"

def format_month_year(df: pd.DataFrame, month_fmt="%b-%Y"):
    """Convert any datetime64 columns in df to Month-Year strings (e.g., Jan-2024)."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime(month_fmt)
    return df

START = "1990-03-01"
END = datetime.today().strftime("%Y-%m-%d")
INTERVAL = "monthly"
OUT_XLSX = Path("eur_forecast_metrics.xlsx")

# Base RICs per horizon
base_horizon_to_universe = {
    1: "EUR1MP=",
    3: "EUR3MP=",
    6: "EUR6MP=",
    12: "EUR1YP=",
}

forecast_fields = [
    'FCAST_MODE', 'FCAST_MEDN', 'FCAST_MEAN',
    'FCAST_LOW', 'FCAST_HIGH', 'STD_DEV', 'FCAST_NUM'
]

preferred_order = [
    'FCAST_MODE', 'FCAST_MEDN', 'FCAST_MEAN',
    'FCAST_LOW', 'FCAST_HIGH', 'STD_DEV', 'FCAST_NUM',
    'actual_value',
    'expected_percentage_change', 'forecast_error'
]

# -----------------------------
# Helpers
# -----------------------------
def fetch_forecast_df(ric: str) -> pd.DataFrame:
    """Fetch a forecast DataFrame for a single RIC and normalize columns."""
    dfh = ld.get_history(
        universe=ric,
        start=START,
        end=END,
        interval=INTERVAL,
        fields=forecast_fields
    )
    if dfh is None or dfh.empty:
        raise RuntimeError(f"No data returned for {ric}")

    # Flatten MultiIndex columns if present (RIC x Field)
    if isinstance(dfh.columns, pd.MultiIndex) and dfh.columns.nlevels == 2:
        dfh = dfh.droplevel(0, axis=1)

    # Normalize field names by stripping prefixes like 'TR.'
    dfh = dfh.rename(columns={c: c.split('.')[-1] for c in dfh.columns})

    # Keep only requested fields that exist
    present = [c for c in forecast_fields if c in dfh.columns]
    if not present:
        raise RuntimeError(f"No requested forecast fields present for {ric}. Returned: {list(dfh.columns)}")
    return dfh[present]

def compute_horizon_metrics(df_actual: pd.DataFrame, df_fore: pd.DataFrame, horizon_months: int):
    """Compute current_value (t-h), actual_value (t), expected % change, forecast_error, and stats."""
    df_fore = df_fore.copy()
    df_fore.index = pd.to_datetime(df_fore.index)
    df_fore['ym'] = df_fore.index.to_period('M')

    # Build actuals map locally without mutating df_actual
    actual_period = pd.to_datetime(df_actual.index).to_period('M')
    actual_map = pd.Series(df_actual['Bid Price'].values, index=actual_period).groupby(level=0).last()

    manual_current = df_fore['current_value'].copy() if 'current_value' in df_fore.columns else None
    
    prior_map = actual_map.shift(1)
    future_map = actual_map.shift(-(horizon_months - 1))

    
    df_fore['actual_value'] = df_fore['ym'].map(future_map)
    df_fore['current_value'] = df_fore['ym'].map(prior_map)

    if manual_current is not None:
        df_fore['current_value'] = manual_current.combine_first(df_fore['current_value'])

    if 'FCAST_MEDN' in df_fore.columns:
        df_fore['expected_percentage_change'] = (
            (df_fore['FCAST_MEDN'] - df_fore['current_value']) / df_fore['current_value'] * 100
        )
        df_fore['forecast_error'] = df_fore['FCAST_MEDN'] - df_fore['actual_value']
        df_fore['expected_percentage_change'] = df_fore['expected_percentage_change'].round(2)
        df_fore['forecast_error'] = df_fore['forecast_error'].round(2)

    df_fore = df_fore.drop(columns=['ym']).sort_index()

    # Stats: last higher/lower since, extrema, and error sign counts
    last_date = df_fore.index.max()
    results = []
    num_cols = df_fore.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        s = df_fore[col].dropna()
        if s.empty or (last_date not in s.index):
            continue
        latest_value = s.loc[last_date]
        prior = s.loc[:last_date].iloc[:-1]
        higher_mask = prior > latest_value
        lower_mask = prior < latest_value
        higher_than_since = prior.index[higher_mask][-1] if higher_mask.any() else None
        lower_than_since = prior.index[lower_mask][-1] if lower_mask.any() else None
        max_idx = s.idxmax()
        min_idx = s.idxmin()
        results.append({
            'column': col,
            'latest_date': last_date,
            'latest_value': latest_value,
            'higher_than_since': higher_than_since,
            'lower_than_since': lower_than_since,
            'max_date': max_idx, 'max_value': s.loc[max_idx],
            'min_date': min_idx, 'min_value': s.loc[min_idx],
        })
    stats_df = pd.DataFrame(results).set_index('column') if results else pd.DataFrame()

    if 'forecast_error' in df_fore.columns:
        s_err = df_fore['forecast_error'].dropna()
        counts = {
            'positive': (s_err > 0).sum(),
            'negative': (s_err < 0).sum(),
            'zero': (s_err == 0).sum(),
        }
    else:
        counts = {'positive': 0, 'negative': 0, 'zero': 0}

    return df_fore, stats_df, counts

# -----------------------------
# 1) Actuals (EUR=)
# -----------------------------
df_actual = ld.get_history(
    universe="EUR=",
    fields=['TR.BIDPRICE'],
    start=START,
    end=END,
    interval=INTERVAL
)
df_actual.index = pd.to_datetime(df_actual.index)
if 'TR.BIDPRICE' in df_actual.columns:
    df_actual = df_actual.rename(columns={'TR.BIDPRICE': 'Bid Price'})

horizon_outputs = {}
worked_universes = {}

manual_date = pd.Timestamp('2025-12-31').normalize()
# >>> Add this block: per-horizon manual row(s)
per_h_values = {
    1: {'FCAST_MEAN': 1.16, 'FCAST_MEDN': 1.17, 'FCAST_LOW': 1.15,'current_value': 1.1733, 'FCAST_HIGH': 1.2,"STD_DEV":0.97},
    3: {'FCAST_MEAN': 1.17, 'FCAST_MEDN': 1.17, 'FCAST_LOW': 1.12,'current_value': 1.1733, 'FCAST_HIGH': 1.23,"STD_DEV":0.97},
    6: {'FCAST_MEAN': 1.18, 'FCAST_MEDN': 1.1868, 'FCAST_LOW': 1.12,'current_value': 1.1733, 'FCAST_HIGH': 1.24,"STD_DEV":0.97},
    12: {'FCAST_MEAN': 1.18, 'FCAST_MEDN': 1.20, 'FCAST_LOW': 1.10,'current_value': 1.1733, 'FCAST_HIGH': 1.27,"STD_DEV":0.0352},
}

for h, ric in base_horizon_to_universe.items():
    try:
        dfh = fetch_forecast_df(ric)

        manual_values = per_h_values.get(h, {})

# Add to dfh (the raw forecast data)
        if manual_values:
            if manual_date not in dfh.index:
                dfh.loc[manual_date] = np.nan
            for col, val in manual_values.items():
                if col not in dfh.columns:
                    dfh[col] = np.nan
                dfh.loc[manual_date, col] = val
            dfh = dfh.sort_index()

        dfh_res, stats_df, counts = compute_horizon_metrics(df_actual, dfh, horizon_months=h)
          
        horizon_outputs[h] = {'df': dfh_res, 'stats': stats_df, 'counts': counts}
        worked_universes[h] = ric
    except Exception as e:
        print(f"[WARN] Failed {h}M ({ric}): {e}")

# -----------------------------
# 3) Export to Excel including summary tables
# -----------------------------
with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
    # Per-horizon sheets
    for h in sorted(horizon_outputs.keys()):
        dfh = horizon_outputs[h]['df'].copy()

        # Order columns
        cols = [c for c in preferred_order if c in dfh.columns]
        cols += [c for c in dfh.columns if c not in cols]

# Remove current_value from final output
        #cols = [c for c in cols if c != 'current_value']

        dfh = dfh[cols]

        dfh = dfh.reset_index().rename(columns={'index': 'Date'})
        dfh = format_month_year(dfh, month_fmt="%b-%Y")
        dfh.to_excel(writer, sheet_name=f"{h}M", index=False)

        # Stats sheet
        stats_df = horizon_outputs[h].get('stats')
        if isinstance(stats_df, pd.DataFrame) and not stats_df.empty:
            stats_out = stats_df.loc[stats_df.index != 'current_value'].reset_index()
            stats_out = format_month_year(stats_out, month_fmt="%b-%Y")
            stats_out.to_excel(writer, sheet_name=f"{h}M_stats", index=False)

    # -----------------------------
    # Summary Tables (single sheet with four tables)
    # -----------------------------
    # Collect a consistent set of variables across horizons from stats
    all_vars = set()
    for h, out in horizon_outputs.items():
        s = out.get('stats')
        if isinstance(s, pd.DataFrame) and not s.empty:
            all_vars.update(s.index.tolist())

    exclude = {'current_value'}
    all_vars = sorted(v for v in all_vars if v not in exclude)
    horizons = sorted(horizon_outputs.keys())

    def assemble_matrix(field_name: str) -> pd.DataFrame:
        """Return a DataFrame with variables as rows, horizons as columns, from stats[field_name]."""
        data = {}
        for h in horizons:
            s = horizon_outputs[h].get('stats')
            if isinstance(s, pd.DataFrame) and not s.empty and field_name in s.columns:
                col_series = s.reindex(all_vars)[field_name]
            else:
                col_series = pd.Series(index=all_vars, dtype='object')
            data[f"{h}M"] = col_series
        dfm = pd.DataFrame(data, index=all_vars)
        dfm.index.name = 'variable'
        return dfm

    # Build the four logical tables
    highest_since_tbl = assemble_matrix('higher_than_since')
    lowest_since_tbl = assemble_matrix('lower_than_since')

    # Precompute once to avoid repeated assemble_matrix calls
    max_date_tbl = assemble_matrix('max_date')
    max_value_tbl = assemble_matrix('max_value')
    min_date_tbl = assemble_matrix('min_date')
    min_value_tbl = assemble_matrix('min_value')

    # Max and Min as combined (date/value) tables per horizon
    max_combined = pd.concat(
        {f"{h}M": pd.DataFrame({'max_date': max_date_tbl[f"{h}M"], 'max_value': max_value_tbl[f"{h}M"]})
         for h in horizons},
        axis=1,
    )

    min_combined = pd.concat(
        {f"{h}M": pd.DataFrame({'min_date': min_date_tbl[f"{h}M"], 'min_value': min_value_tbl[f"{h}M"]})
         for h in horizons},
        axis=1,
    )

    max_combined.index.name = 'variable'
    min_combined.index.name = 'variable'

    sheet_name = "Summary Tables"

    def write_summary_sections():
        start_row = 0

        def write_section(df: pd.DataFrame, title: str):
            nonlocal start_row
            # Title row
            title_df = pd.DataFrame({title: []})
            title_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row)
            start_row += 1
            # Data
            df_out = df.reset_index()

            # Flatten MultiIndex columns if present
            if isinstance(df_out.columns, pd.MultiIndex):
                df_out.columns = [' - '.join(map(str, tup)).strip()
                                  for tup in df_out.columns.to_flat_index()]
            df_out = format_month_year(df_out, month_fmt="%b-%Y")
            df_out.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row)
            start_row += len(df_out) + 2  # gap

        write_section(highest_since_tbl, "Highest Since (date last higher than latest)")
        write_section(lowest_since_tbl, "Lowest Since (date last lower than latest)")
        write_section(max_combined, "All-Time Max (date and value)")
        write_section(min_combined, "All-Time Min (date and value)")

    write_summary_sections()

    # -----------------------------
    # Summary sheet of error counts
    # -----------------------------
    summary_rows = []
    for h in sorted(horizon_outputs.keys()):
        counts = horizon_outputs[h].get('counts', {})
        summary_rows.append({
            'horizon_months': h,
            'overestimate': counts.get('positive', 0),
            'underestimate': counts.get('negative', 0),
            'zero_errors': counts.get('zero', 0),
            'ric_used': worked_universes.get(h, "")
        })
    if summary_rows:
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="over_underestimate_summary", index=False)


    
    ordered_names = ["Summary Tables"]
    for h in sorted(horizon_outputs.keys()):
        ordered_names.append(f"{h}M")
        stats_name = f"{h}M_stats"
        if stats_name in name_to_ws:
            ordered_names.append(stats_name)

    # Append any remaining sheets
    for ws in wb.worksheets:
        if ws.title not in ordered_names:
            ordered_names.append(ws.title)

    # Apply order
    wb._sheets = [name_to_ws[n] for n in ordered_names if n in name_to_ws]

print(f"Saved: {OUT_XLSX.resolve()}")