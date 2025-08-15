"""Train an XGBoost model to predict next-interval return using intraday OHLCV data.

Steps:
 1. Pull recent intraday data from operational tables (stock_data) or new fact table if available.
 2. Engineer features (lagged returns, moving averages, volatility, volume ratios).
 3. Train/test split by time (no leakage), train XGBoost regression model.
 4. Evaluate (MAE, RMSE, R^2) and feature importance.
 5. Persist model + feature column order in artifacts/models.

Usage (PowerShell):
  python scripts/train_xgboost.py --symbols AAPL MSFT --days 60 --interval 15 --table stock_data

If you have migrated to the warehouse fact table, pass --table fact_intraday_price and the script
will adapt (requires dim_symbol join or you can create a view vw_intraday_all).
"""
from __future__ import annotations
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

from utils import get_db_connection, setup_logging

logger = setup_logging('train_xgboost')


def load_intraday(symbols, days: int, table: str) -> pd.DataFrame:
    end = datetime.utcnow()
    start = end - timedelta(days=days)
    with get_db_connection() as conn:
        placeholders = ','.join(['%s'] * len(symbols))
        if table == 'fact_intraday_price':
            # assume dim_symbol exists
            sql = f"""
                SELECT s.symbol, f.datetime, f.open, f.high, f.low, f.close, f.volume
                FROM fact_intraday_price f
                JOIN dim_symbol s ON f.symbol_id = s.symbol_id
                WHERE s.symbol IN ({placeholders}) AND f.datetime BETWEEN %s AND %s
                ORDER BY f.datetime
            """
        else:
            # legacy per-asset table (stock_data)
            sql = f"""
                SELECT symbol, datetime, open, high, low, close, volume
                FROM {table}
                WHERE symbol IN ({placeholders}) AND datetime BETWEEN %s AND %s
                ORDER BY datetime
            """
        df = pd.read_sql(sql, conn, params=[*symbols, start, end])
    if df.empty:
        logger.warning("No data returned for given parameters.")
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df


def engineer_features(df: pd.DataFrame, horizon: int = 1) -> pd.DataFrame:
    if df.empty:
        return df
    feats = []
    for sym, sub in df.groupby('symbol'):
        s = sub.sort_values('datetime').reset_index(drop=True)
        s['return'] = s['close'].pct_change()
        # Target: next interval return
        s['target_return'] = s['return'].shift(-horizon)
        # Lags
        for lag in [1,2,3,4,5]:
            s[f'return_lag_{lag}'] = s['return'].shift(lag)
        # Rolling stats
        s['roll_mean_5'] = s['close'].rolling(5).mean()
        s['roll_std_5'] = s['close'].rolling(5).std()
        s['roll_mean_10'] = s['close'].rolling(10).mean()
        s['roll_std_10'] = s['close'].rolling(10).std()
        # Volume features
        s['vol_ma_5'] = s['volume'].rolling(5).mean()
        s['vol_ratio'] = s['volume'] / s['vol_ma_5']
        # Price position within range
        s['hl_range'] = s['high'] - s['low']
        s['close_pos_range'] = (s['close'] - s['low']) / s['hl_range'].replace(0, np.nan)
        s['symbol'] = sym
        feats.append(s)
    out = pd.concat(feats, ignore_index=True)
    # Drop rows with insufficient history
    out = out.dropna(subset=['target_return'])
    # Forward fill any remaining NaNs in engineered features (edge cases) then drop still-missing
    feature_cols = [c for c in out.columns if c not in ['datetime','symbol','target_return']]
    out[feature_cols] = out[feature_cols].fillna(method='ffill')
    out = out.dropna(subset=feature_cols)
    return out


def time_based_split(df: pd.DataFrame, test_size: float = 0.2):
    df = df.sort_values('datetime')
    split_idx = int(len(df) * (1 - test_size))
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    return train, test


def train_xgb(df: pd.DataFrame, params: dict) -> tuple[XGBRegressor, dict]:
    feature_cols = [c for c in df.columns if c not in ['datetime','symbol','target_return']]
    X = df[feature_cols]
    y = df['target_return']
    train_df, test_df = time_based_split(df)
    X_train, y_train = train_df[feature_cols], train_df['target_return']
    X_test, y_test = test_df[feature_cols], test_df['target_return']

    model = XGBRegressor(**params)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    preds = model.predict(X_test)
    metrics = {
        'MAE': float(mean_absolute_error(y_test, preds)),
        'RMSE': float(mean_squared_error(y_test, preds, squared=False)),
        'R2': float(r2_score(y_test, preds)),
        'n_train': int(len(X_train)),
        'n_test': int(len(X_test))
    }
    return model, {'metrics': metrics, 'features': feature_cols}


def save_model(model: XGBRegressor, meta: dict, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    model_path = os.path.join(out_dir, 'xgb_model.json')
    model.save_model(model_path)
    with open(os.path.join(out_dir, 'model_meta.json'), 'w') as f:
        json.dump(meta, f, indent=2)
    logger.info(f"Model saved to {model_path}")


def main():
    parser = argparse.ArgumentParser(description='Train XGBoost on intraday data to predict next return.')
    parser.add_argument('--symbols', nargs='+', default=['AAPL'], help='Symbols to include')
    parser.add_argument('--days', type=int, default=60, help='How many days of data')
    parser.add_argument('--table', default='stock_data', help='Source table (stock_data or fact_intraday_price)')
    parser.add_argument('--learning-rate', type=float, default=0.05)
    parser.add_argument('--n-estimators', type=int, default=400)
    parser.add_argument('--max-depth', type=int, default=5)
    parser.add_argument('--subsample', type=float, default=0.9)
    parser.add_argument('--colsample-bytree', type=float, default=0.8)
    parser.add_argument('--gamma', type=float, default=0.0)
    parser.add_argument('--min-child-weight', type=float, default=1.0)
    parser.add_argument('--reg-alpha', type=float, default=0.0)
    parser.add_argument('--reg-lambda', type=float, default=1.0)
    parser.add_argument('--out-dir', default='artifacts/models', help='Where to save the model')
    args = parser.parse_args()

    logger.info(f"Loading data for symbols={args.symbols} days={args.days} table={args.table}")
    raw = load_intraday(args.symbols, args.days, args.table)
    if raw.empty:
        logger.error('No data available. Exiting.')
        return
    feats = engineer_features(raw)
    if feats.empty:
        logger.error('No engineered feature rows available. Exiting.')
        return

    params = {
        'learning_rate': args.learning_rate,
        'n_estimators': args.n_estimators,
        'max_depth': args.max_depth,
        'subsample': args.subsample,
        'colsample_bytree': args.colsample_bytree,
        'gamma': args.gamma,
        'min_child_weight': args.min_child_weight,
        'reg_alpha': args.reg_alpha,
        'reg_lambda': args.reg_lambda,
        'objective': 'reg:squarederror',
        'tree_method': 'hist',
        'n_jobs': 4,
        'random_state': 42
    }
    model, meta = train_xgb(feats, params)
    logger.info(f"Training complete: {meta['metrics']}")

    # Add top feature importances
    importances = model.feature_importances_
    top_imp = sorted(zip(meta['features'], importances), key=lambda x: x[1], reverse=True)[:15]
    meta['top_feature_importances'] = [{'feature': f, 'importance': float(i)} for f, i in top_imp]
    save_model(model, meta, args.out_dir)

    # Quick print of feature importance
    logger.info('Top feature importances:')
    for item in meta['top_feature_importances']:
        logger.info(f"  {item['feature']}: {item['importance']:.4f}")

if __name__ == '__main__':
    main()
