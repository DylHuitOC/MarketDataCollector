import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from extract.stock_extractor import StockExtractor
from extract.index_extractor import IndexExtractor
from extract.commodity_extractor import CommodityExtractor
from config import API_KEY, INDEX_SYMBOLS, COMMODITY_SYMBOLS

"""Demo script: Pull last 6 months of selected stocks (AAPL, MSFT, GOOG), plus last 30 days of indexes & commodities (to keep runtime reasonable), load nothing (optional), and plot.

Adjust as needed for presentation.
"""

STOCKS_FOR_DEMO = ['AAPL', 'MSFT', 'GOOG']
MONTHS_BACK = 6
INDEX_DAYS = 30
COMMODITY_DAYS = 30


def _flatten(symbol_dict):
    rows = []
    for sym, recs in symbol_dict.items():
        for r in recs:
            row = r.copy()
            row['symbol'] = sym
            rows.append(row)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def fetch_data():
    end = datetime.now()
    start_stocks = end - timedelta(days=30*MONTHS_BACK)
    start_indexes = end - timedelta(days=INDEX_DAYS)
    start_commod = end - timedelta(days=COMMODITY_DAYS)

    stock_ex = StockExtractor()
    print(f"Fetching {MONTHS_BACK} months of 15m stock data for {STOCKS_FOR_DEMO} …")
    stock_data = stock_ex.extract_historical_data(STOCKS_FOR_DEMO, start_stocks, end)
    df_stocks = _flatten(stock_data)

    index_ex = IndexExtractor()
    print(f"Fetching {INDEX_DAYS} days of 5m index data (aggregated to 15m) for {INDEX_SYMBOLS} …")
    index_data = index_ex.extract_historical_data(INDEX_SYMBOLS, start_indexes, end, interval_minutes=15)
    df_indexes = _flatten(index_data)

    commod_ex = CommodityExtractor()
    print(f"Fetching {COMMODITY_DAYS} days of 5m commodity data (aggregated to 15m) for {COMMODITY_SYMBOLS} …")
    commod_data = commod_ex.extract_historical_data(COMMODITY_SYMBOLS, start_commod, end)
    df_commod = _flatten(commod_data)

    for name, df in [('stocks', df_stocks), ('indexes', df_indexes), ('commodities', df_commod)]:
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.sort_values('date', inplace=True)
            df.reset_index(drop=True, inplace=True)
        print(f"{name}: {len(df)} records")

    return df_stocks, df_indexes, df_commod


def plot_prices(df_stocks, df_indexes, df_commod):
    sns.set_theme(style='whitegrid')
    out_dir = 'artifacts/demo_plots'
    os.makedirs(out_dir, exist_ok=True)

    # Stock close prices (resampled daily for clarity)
    if not df_stocks.empty:
        plt.figure(figsize=(10,5))
        for sym in df_stocks.symbol.unique():
            sub = df_stocks[df_stocks.symbol == sym].copy()
            sub.set_index('date', inplace=True)
            daily = sub['close'].resample('1D').last()
            plt.plot(daily.index, daily.values, label=sym)
    plt.title(f'Stock Daily Close (Last {MONTHS_BACK} Months)')
    plt.legend(); plt.tight_layout();
    plt.savefig(os.path.join(out_dir, 'stocks_daily_close.png'), dpi=150)
    plt.show()

    # Index normalized performance
    if not df_indexes.empty:
        plt.figure(figsize=(10,5))
        for sym in df_indexes.symbol.unique():
            sub = df_indexes[df_indexes.symbol == sym].copy()
            sub.set_index('date', inplace=True)
            daily = sub['close'].resample('1D').last().dropna()
            if not daily.empty:
                norm = daily / daily.iloc[0] * 100
                plt.plot(norm.index, norm.values, label=sym)
    plt.title('Index Normalized Performance (Base=100)')
    plt.legend(); plt.tight_layout();
    plt.savefig(os.path.join(out_dir, 'indexes_normalized.png'), dpi=150)
    plt.show()

    # Commodities intraday range boxplot by symbol
    if not df_commod.empty:
        tmp = df_commod.copy()
        tmp['range'] = tmp['high'] - tmp['low']
        plt.figure(figsize=(6,4))
        sns.boxplot(data=tmp, x='symbol', y='range')
    plt.title('Commodity 15m Range Distribution')
    plt.tight_layout();
    plt.savefig(os.path.join(out_dir, 'commodities_range_boxplot.png'), dpi=150)
    plt.show()


def main():
    df_stocks, df_indexes, df_commod = fetch_data()
    plot_prices(df_stocks, df_indexes, df_commod)

if __name__ == '__main__':
    main()
