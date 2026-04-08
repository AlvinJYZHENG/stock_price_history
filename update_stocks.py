import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime

# --- 配置区域 ---
DEFAULT_TICKERS = ["AAPL", "MSFT", "600519.SS", "0700.HK"]
CSV_FILE = "stock_price_history.csv"
START_DATE = "2016-01-01"
# ----------------

def get_stock_info(ticker):
    """获取股票名称和价格数据（新增日期去重）"""
    try:
        tkr = yf.Ticker(ticker)
        time.sleep(random.uniform(0.5, 1.5))
        
        name = tkr.info.get('longName') or tkr.info.get('shortName') or ticker
        hist = tkr.history(start=START_DATE, actions=False)
        
        if hist.empty:
            print(f"⚠️ 警告: {ticker} 无有效数据")
            return name, pd.Series(dtype=float)
        
        # 关键修复：去除重复日期（若存在）
        hist = hist[~hist.index.duplicated(keep='first')]
        return name, hist['Close']
    
    except Exception as e:
        print(f"❌ 获取{ticker}失败: {str(e)}")
        return ticker, pd.Series(dtype=float)

def update_csv():
    # 1. 正确读取现有CSV的股票代码（同前）
    tickers = []
    if os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                if first_line:
                    parts = first_line.split(',')
                    name_idx = parts.index('name') if 'name' in parts else len(parts)
                    valid_codes = parts[1:name_idx]
                    tickers = [code.strip() for code in valid_codes if code.strip() and code != 'ticker']
            print(f"📄 检测到现有股票代码: {tickers}")
        except Exception as e:
            print(f"⚠️ 读取文件头失败: {e}")
    
    # 添加默认股票（去重）
    for t in DEFAULT_TICKERS:
        if t not in tickers:
            tickers.append(t)
    print(f"🎯 最终股票列表: {tickers}")

    # 2. 下载数据（同前）
    stock_names = {}
    price_data = {}
    for ticker in tickers:
        name, prices = get_stock_info(ticker)
        stock_names[ticker] = name
        if not prices.empty:
            price_data[ticker] = prices
        time.sleep(0.3)
    
    if not price_data:
        print("❌ 所有股票下载失败")
        return

    # 3. 合并数据（新增日期去重）
    all_dates = set()
    for ts in price_data.values():
        all_dates.update(ts.index)
    sorted_dates = sorted(all_dates)  # 自动去重并排序
    
    df_prices = pd.DataFrame(index=sorted_dates, columns=tickers)
    for ticker, prices in price_data.items():
        df_prices[ticker] = prices  # 对齐日期，空值为NaN

    # 4. 生成CSV（同前，确保每行一个日期）
    output_lines = [
        ",".join(["ticker"] + tickers),
        ",".join(["name"] + [stock_names[t] for t in tickers])
    ]
    for date in sorted_dates:
        date_str = date.strftime("%Y/%m/%d")
        prices = [f"{df_prices.loc[date, t]:.4f}" if not pd.isna(df_prices.loc[date, t]) else "" for t in tickers]
        output_lines.append(",".join([date_str] + prices))
    
    # 5. 写入文件（同前）
    with open(CSV_FILE, 'w', encoding='utf-8-sig') as f:
        f.write("\n".join(output_lines))
    
    print(f"✅ 成功更新 {CSV_FILE}，包含 {len(sorted_dates)} 个唯一交易日")

if __name__ == "__main__":
    update_csv()
