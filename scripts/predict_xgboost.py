"""Predict next-interval return for given symbols using previously trained XGBoost model.

Requirements:
  - Model + metadata saved by scripts/train_xgboost.py (xgb_model.json, model_meta.json)
  - Intraday data present in source table (legacy stock_data or warehouse fact_intraday_price)

Usage examples (PowerShell):
  # Predict for AAPL using legacy table
  python scripts/predict_xgboost.py --symbols AAPL --table stock_data

  # Predict for multiple symbols using warehouse fact table
  python scripts/predict_xgboost.py --symbols AAPL MSFT --table fact_intraday_price --model-dir artifacts/models

Outputs:
  - Console table of predicted next return & projected next close
  - Optional CSV via --output-csv

Notes:
  - Prediction horizon is 1 interval ahead (consistent with training target_return definition)
  - Feature engineering MUST mirror training; keep in sync if you modify training script
"""
from __future__ import annotations
import sys, os, json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from xgboost import XGBRegressor

from utils import get_db_connection, setup_logging

logger = setup_logging('predict_xgboost')


def load_model(model_dir: str) -> tuple[XGBRegressor, dict]:
    model_path = os.path.join(model_dir, 'xgb_model.json')
    meta_path = os.path.join(model_dir, 'model_meta.json')
    if not os.path.exists(model_path) or not os.path.exists(meta_path):
        raise FileNotFoundError(f"Model or metadata not found in {model_dir}. Run training first.")
    model = XGBRegressor()
    model.load_model(model_path)
    with open(meta_path, 'r') as f:
        meta = json.load(f)
    return model, meta


def fetch_recent(symbols: list[str], table: str, lookback_bars: int) -> pd.DataFrame:
    with get_db_connection() as conn:
        placeholders = ','.join(['%s'] * len(symbols))
        if table == 'fact_intraday_price':
            sql = f"""
                SELECT s.symbol, f.datetime, f.open, f.high, f.low, f.close, f.volume
                FROM fact_intraday_price f
                JOIN dim_symbol s ON f.symbol_id = s.symbol_id
                WHERE s.symbol IN ({placeholders})
                ORDER BY f.datetime DESC
                LIMIT %s
            """
        else:
            sql = f"""
                SELECT symbol, datetime, open, high, low, close, volume
                FROM {table}
                WHERE symbol IN ({placeholders})
                ORDER BY datetime DESC
                LIMIT %s
            """
        # NOTE: LIMIT applies to combined set; we'll re-filter per symbol after
        df = pd.read_sql(sql, conn, params=[*symbols, lookback_bars * max(1, len(symbols))])
    if df.empty:
        logger.warning('No recent data fetched.')
        return df
    df['datetime'] = pd.to_datetime(df['datetime'])
    # Keep only lookback_bars per symbol ascending order
    trimmed = []
    for sym in symbols:
        sub = df[df['symbol'] == sym].sort_values('datetime').tail(lookback_bars)
        if not sub.empty:
            trimmed.append(sub)
    if not trimmed:
        return pd.DataFrame()
    out = pd.concat(trimmed, ignore_index=True)
    return out.sort_values(['symbol','datetime'])


def engineer_features_for_prediction(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    engineered = []
    for sym, sub in df.groupby('symbol'):
        s = sub.sort_values('datetime').reset_index(drop=True)
        s['return'] = s['close'].pct_change()
        for lag in [1,2,3,4,5]:
            s[f'return_lag_{lag}'] = s['return'].shift(lag)
        s['roll_mean_5'] = s['close'].rolling(5).mean()
        s['roll_std_5'] = s['close'].rolling(5).std()
        s['roll_mean_10'] = s['close'].rolling(10).mean()
        s['roll_std_10'] = s['close'].rolling(10).std()
        s['vol_ma_5'] = s['volume'].rolling(5).mean()
        s['vol_ratio'] = s['volume'] / s['vol_ma_5']
        s['hl_range'] = s['high'] - s['low']
        s['close_pos_range'] = (s['close'] - s['low']) / s['hl_range'].replace(0, np.nan)
        engineered.append(s)
    out = pd.concat(engineered, ignore_index=True)
    return out


def build_feature_matrix(fe_df: pd.DataFrame, feature_order: list[str]) -> pd.DataFrame:
    latest_rows = []
    for sym, sub in fe_df.groupby('symbol'):
        sub = sub.sort_values('datetime')
        latest = sub.iloc[-1:].copy()
        latest_rows.append(latest)
    latest_df = pd.concat(latest_rows, ignore_index=True)
    # Ensure all required features present
    for col in feature_order:
        if col not in latest_df.columns:
            latest_df[col] = np.nan
    latest_df = latest_df[['symbol','datetime'] + feature_order]
    # Fill remaining NaNs in features (due to insufficient history) with simple defaults
    feat_only = latest_df[feature_order].copy()
    feat_only = feat_only.fillna(method='ffill', axis=0).fillna(0)
    latest_df[feature_order] = feat_only
    return latest_df


def predict_next_interval(model: XGBRegressor, meta: dict, latest_df: pd.DataFrame) -> pd.DataFrame:
    feature_cols = meta['features']
    X = latest_df[feature_cols]
    preds = model.predict(X)
    # Append predictions
    out = latest_df[['symbol','datetime']].copy()
    out['pred_return_next'] = preds
    # Derive projected next close using last close
    out = out.merge(latest_df[['symbol','close']], on='symbol', how='left') if 'close' in latest_df.columns else out
    if 'close' in latest_df.columns:
        out['projected_next_close'] = out['close'] * (1 + out['pred_return_next'])
    # Estimate next timestamp (assume interval of last 2 bars)
    est_next_times = []
    for sym, sub in latest_df.groupby('symbol'):
        sub = sub.sort_values('datetime')
        if len(sub) >= 2:
            delta = sub['datetime'].iloc[-1] - sub['datetime'].iloc[-2]
            est_next_times.append((sym, sub['datetime'].iloc[-1] + delta))
        else:
            est_next_times.append((sym, sub['datetime'].iloc[-1] + pd.Timedelta(minutes=15)))
    est_df = pd.DataFrame(est_next_times, columns=['symbol','estimated_next_datetime'])
    out = out.merge(est_df, on='symbol', how='left')
    return out


def main():
    parser = argparse.ArgumentParser(description='Predict next-interval return using trained XGBoost model.')
    parser.add_argument('--symbols', nargs='+', default=['AAPL'], help='Symbols to score')
    parser.add_argument('--table', default='stock_data', help='Source table (stock_data or fact_intraday_price)')
    parser.add_argument('--lookback-bars', type=int, default=300, help='Bars to pull per symbol for feature calc')
    parser.add_argument('--model-dir', default='artifacts/models', help='Directory containing xgb_model.json & model_meta.json')
    parser.add_argument('--output-csv', default='', help='Optional path to save predictions CSV')
    args = parser.parse_args()

    try:
        model, meta = load_model(args.model_dir)
    except Exception as e:
        logger.error(f'Failed to load model: {e}')
        return
    feature_order = meta.get('features', [])
    if not feature_order:
        logger.error('Model metadata missing feature list.')
        return

    raw = fetch_recent(args.symbols, args.table, args.lookback_bars)
    if raw.empty:
        logger.error('No data fetched; aborting.')
        return
    fe = engineer_features_for_prediction(raw)
    if fe.empty:
        logger.error('Feature engineering produced empty DataFrame; aborting.')
        return
    latest = build_feature_matrix(fe, feature_order)
    # We need last close; ensure it's present
    if 'close' not in latest.columns:
        # merge from raw
        last_close = raw.sort_values('datetime').groupby('symbol').tail(1)[['symbol','close']]
        latest = latest.merge(last_close, on='symbol', how='left')
    preds = predict_next_interval(model, meta, latest)

    # Pretty print
    display_cols = ['symbol','datetime','estimated_next_datetime','pred_return_next','close','projected_next_close']
    for col in display_cols:
        if col not in preds.columns:
            preds[col] = np.nan
    print('\nPredictions:')
    print(preds[display_cols].to_string(index=False, justify='center', float_format=lambda x: f'{x:0.6f}'))

    if args.output_csv:
        os.makedirs(os.path.dirname(args.output_csv), exist_ok=True)
        preds.to_csv(args.output_csv, index=False)
        logger.info(f'Predictions saved to {args.output_csv}')

if __name__ == '__main__':
    main()
