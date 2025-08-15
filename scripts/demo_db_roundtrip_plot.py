"""Demo: Extract 6 months (configurable) of selected symbols, load into DB, then query back for plotting.

Usage examples (PowerShell):
  # Extract + load + plot from DB
  python scripts/demo_db_roundtrip_plot.py --stocks AAPL MSFT GOOG --months 6 --load

  # Only query & plot (assumes data already loaded)
  python scripts/demo_db_roundtrip_plot.py --stocks AAPL MSFT GOOG --months 6

Produces plots in artifacts/demo_plots_db/*.png
"""
from __future__ import annotations
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from extract.stock_extractor import StockExtractor
from extract.index_extractor import IndexExtractor
from extract.commodity_extractor import CommodityExtractor
from load.data_warehouse_loader import DataWarehouseLoader
from config import INDEX_SYMBOLS, COMMODITY_SYMBOLS
from utils import get_db_connection, setup_logging

logger = setup_logging('demo_db_roundtrip')


def extract_data(stocks: list[str], months: int):
    end = datetime.now()
    start_stocks = end - timedelta(days=30 * months)
    index_days = 30  # limit for demo speed
    commod_days = 30
    start_indexes = end - timedelta(days=index_days)
    start_commod = end - timedelta(days=commod_days)

    stock_ex = StockExtractor()
    logger.info(f"Extracting {months} months of stock data for {stocks} ...")
    stock_data = stock_ex.extract_historical_data(stocks, start_stocks, end)

    index_ex = IndexExtractor()
    logger.info(f"Extracting {index_days} days of index data (aggregated) ...")
    index_data = index_ex.extract_historical_data(INDEX_SYMBOLS, start_indexes, end, interval_minutes=15)

    commod_ex = CommodityExtractor()
    logger.info(f"Extracting {commod_days} days of commodity data (aggregated) ...")
    commod_data = commod_ex.extract_historical_data(COMMODITY_SYMBOLS, start_commod, end)

    return stock_data, index_data, commod_data


def load_into_db(stock_data, index_data, commod_data):
    loader = DataWarehouseLoader()
    payload = {}
    if stock_data:
        payload['stocks'] = stock_data
    if index_data:
        payload['indexes'] = index_data
    if commod_data:
        payload['commodities'] = commod_data
    if not payload:
        logger.warning("Nothing to load.")
        return {}
    logger.info("Loading extracted data into warehouse tables ...")
    return loader.load_extracted_data(payload, stock_table='stock_data', index_table='index_data', commodity_table='commodity_data')


def query_from_db(stocks: list[str], months: int):
    """Query previously loaded data. Returns three DataFrames (stocks, indexes, commodities)."""
    end = datetime.now()
    start = end - timedelta(days=30 * months)
    with get_db_connection() as conn:
        stock_df = pd.DataFrame()
        if stocks:
            placeholders = ','.join(['%s'] * len(stocks))
            sql = f"""
                SELECT symbol, datetime, open, high, low, close, volume
                FROM stock_data
                WHERE symbol IN ({placeholders}) AND datetime BETWEEN %s AND %s
                ORDER BY datetime
            """
            stock_df = pd.read_sql(sql, conn, params=[*stocks, start, end])
        idx_end = end
        idx_start = end - timedelta(days=30)
        idx_df = pd.read_sql(
            """
            SELECT symbol, datetime, open, high, low, close, volume
            FROM index_data
            WHERE datetime BETWEEN %s AND %s
            ORDER BY datetime
            """, conn, params=[idx_start, idx_end]
        )
        commod_df = pd.read_sql(
            """
            SELECT symbol, datetime, open, high, low, close, volume
            FROM commodity_data
            WHERE datetime BETWEEN %s AND %s
            ORDER BY datetime
            """, conn, params=[idx_start, idx_end]
        )
    for df in (stock_df, idx_df, commod_df):
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['datetime'])
    return stock_df, idx_df, commod_df


def _compress_market_time(df: pd.DataFrame, interval_minutes: int | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    w = df.copy().sort_values('datetime')
    w['trading_date'] = w['datetime'].dt.date
    if interval_minutes is None and len(w) > 1:
        deltas = w['datetime'].diff().dropna().dt.total_seconds()
        if not deltas.empty:
            interval_minutes = max(1, int(round(deltas.mode().iloc[0] / 60)))
    if interval_minutes is None:
        interval_minutes = 15
    per_day_counts = w.groupby('trading_date')['datetime'].count()
    offsets = {}
    total = 0
    for d in sorted(per_day_counts.index):
        offsets[d] = total
        total += per_day_counts.loc[d]
    w['slot_in_day'] = w.groupby('trading_date').cumcount()
    w['global_slot'] = w.apply(lambda r: offsets[r['trading_date']] + r['slot_in_day'], axis=1)
    origin = pd.Timestamp(w['datetime'].min().date())
    w['compressed_time'] = origin + pd.to_timedelta(w['global_slot'] * interval_minutes, unit='m')
    return w


def _fill_uniform_grid(df: pd.DataFrame, interval: str = '15min') -> pd.DataFrame:
    if df.empty:
        return df
    w = df.copy().sort_values('datetime').set_index('datetime')
    full_idx = pd.date_range(w.index.min().floor(interval), w.index.max().ceil(interval), freq=interval)
    w = w.reindex(full_idx)
    # Forward fill OHLCV (keep symbol static per segment)
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in w.columns:
            w[col] = w[col].ffill()
    if 'symbol' in w.columns:
        w['symbol'] = w['symbol'].ffill()
    w = w.reset_index().rename(columns={'index': 'datetime'})
    return w


def plot_from_db(stock_df, idx_df, commod_df, months: int, compress: bool = False, fill_gaps: bool = False):
    sns.set_theme(style='whitegrid')
    out_dir = 'artifacts/demo_plots_db'
    os.makedirs(out_dir, exist_ok=True)

    stock_proc = stock_df.copy()
    if compress:
        stock_proc = _compress_market_time(stock_proc)
    if fill_gaps:
        stock_proc = _fill_uniform_grid(stock_proc)

    if not stock_proc.empty:
        plt.figure(figsize=(10, 5))
        for sym, sub in stock_proc.groupby('symbol'):
            if compress and 'compressed_time' in sub.columns:
                plt.plot(sub.sort_values('compressed_time')['compressed_time'], sub.sort_values('compressed_time')['close'], label=sym, linewidth=0.9)
            else:
                daily = sub.set_index('datetime')['close'].resample('1D').last().dropna()
                if len(daily) > 1:
                    plt.plot(daily.index, daily.values, label=sym)
        title_suffix = ' (compressed)' if compress else ''
    plt.title(f'Stock Close{title_suffix} (Last {months} Months) - DB')
    plt.ylabel('Close Price')
    plt.xlabel('Date' if not compress else 'Compressed Time')
    plt.legend(); plt.tight_layout(); plt.savefig(os.path.join(out_dir, 'stocks_daily_close_db.png'), dpi=150); plt.show()

    if not idx_df.empty:
        plt.figure(figsize=(10, 5))
        for sym, sub in idx_df.groupby('symbol'):
            daily = sub.set_index('datetime')['close'].resample('1D').last().dropna()
            if not daily.empty:
                norm = daily / daily.iloc[0] * 100
                plt.plot(norm.index, norm.values, label=sym)
    plt.title('Index Normalized Performance (Base=100) - DB')
    plt.ylabel('Normalized Close (Base=100)')
    plt.xlabel('Date')
    plt.legend(); plt.tight_layout(); plt.savefig(os.path.join(out_dir, 'indexes_normalized_db.png'), dpi=150); plt.show()

    if not commod_df.empty:
        tmp = commod_df.copy()
        tmp['range'] = tmp['high'] - tmp['low']
        plt.figure(figsize=(6, 4))
        sns.boxplot(data=tmp, x='symbol', y='range')
    plt.title('Commodity 15m Range Distribution - DB')
    plt.ylabel('High-Low Range')
    plt.xlabel('Symbol')
    plt.tight_layout(); plt.savefig(os.path.join(out_dir, 'commodities_range_boxplot_db.png'), dpi=150); plt.show()


def main():
    parser = argparse.ArgumentParser(description='Demo DB roundtrip plotter.')
    parser.add_argument('--stocks', nargs='+', default=['AAPL', 'MSFT', 'GOOG'], help='Stock symbols to include')
    parser.add_argument('--months', type=int, default=6, help='How many months of stock data to consider')
    parser.add_argument('--load', action='store_true', help='Extract & load before querying')
    parser.add_argument('--compress-time', action='store_true', help='Compress intraday time (remove overnight gaps)')
    parser.add_argument('--fill-gaps', action='store_true', help='Fill missing 15m slots with forward-filled values')
    args = parser.parse_args()

    if args.load:
        stock_data, index_data, commod_data = extract_data(args.stocks, args.months)
        load_results = load_into_db(stock_data, index_data, commod_data)
        logger.info(
            f"Load results summary: stocks={load_results.get('stocks',{}).get('records_loaded',0)}, "
            f"indexes={load_results.get('indexes',{}).get('records_loaded',0)}, "
            f"commodities={load_results.get('commodities',{}).get('records_loaded',0)}"
        )

    stock_df, idx_df, commod_df = query_from_db(args.stocks, args.months)
    logger.info(f"Queried records: stocks={len(stock_df)}, indexes={len(idx_df)}, commodities={len(commod_df)}")
    plot_from_db(stock_df, idx_df, commod_df, args.months, compress=args.compress_time, fill_gaps=args.fill_gaps)

if __name__ == '__main__':
    main()
