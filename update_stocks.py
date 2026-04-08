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
    """获取股票名称和价格数据"""
    try:
        tkr = yf.Ticker(ticker)
        time.sleep(random.uniform(0.5, 1.5))  # 随机延迟
        
        # 获取名称
        name = tkr.info.get('longName') or tkr.info.get('shortName') or ticker
        
        # 获取历史数据
        hist = tkr.history(start=START_DATE, actions=False)
        if hist.empty:
            print(f"⚠️ 警告: {ticker} 无有效数据")
            return name, pd.Series(dtype=float)
            
        return name, hist['Close']
    
    except Exception as e:
        print(f"❌ 获取{ticker}失败: {str(e)}")
        return ticker, pd.Series(dtype=float)

def update_csv():
    # 1. 确定目标股票列表（保留原CSV顺序）
    tickers = []
    
    # 读取现有文件中的股票代码（如果存在）
    if os.path.exists(CSV_FILE):
        try:
            # 读取第一行获取股票代码
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                if first_line:
                    # 分割第一行：跳过"A1"位置的"ticker"
                    codes = first_line.split(',')[1:]
                    tickers = [code for code in codes if code.strip()]
                    
            print(f"📄 检测到现有股票代码: {tickers}")
        except Exception as e:
            print(f"⚠️ 读取文件头失败: {e}")
    
    # 添加默认股票代码（如果不存在）
    for t in DEFAULT_TICKERS:
        if t not in tickers:
            tickers.append(t)
    
    print(f"🎯 最终股票列表: {tickers}")

    # 2. 下载数据
    stock_names = {}
    price_data = {}
    
    for ticker in tickers:
        name, prices = get_stock_info(ticker)
        stock_names[ticker] = name
        if not prices.empty:
            price_data[ticker] = prices
        time.sleep(0.3)  # 基础延迟
    
    # 3. 合并价格数据（**核心修改：删除填充逻辑**）
    if not price_data:
        print("❌ 所有股票下载失败")
        return
    
    # 创建包含所有日期的DataFrame（仅保留各股票实际有数据的日期）
    all_dates = set()
    for ts in price_data.values():
        all_dates.update(ts.index)
    sorted_dates = sorted(all_dates)
    df_prices = pd.DataFrame(index=sorted_dates, columns=tickers)
    
    # 填充价格数据（**仅将存在的价格填入对应位置，不主动填充缺失值**）
    for ticker, prices in price_data.items():
        df_prices[ticker] = prices  # 直接赋值，保留原数据的NaN（空值）
    
    # 4. 构建输出（保持原空值留白逻辑）
    output_lines = []
    
    # 第一行: ticker + 股票代码
    header = ["ticker"] + tickers
    output_lines.append(",".join(header))
    
    # 第二行: name + 股票名称
    names_row = ["name"] + [stock_names[t] for t in tickers]
    output_lines.append(",".join(names_row))
    
    # 数据行: 日期 + 价格（**空值保持留白**）
    for date in sorted_dates:
        date_str = date.strftime("%Y/%m/%d")
        # 若价格为NaN则留空，否则保留4位小数
        prices = [f"{df_prices.loc[date, t]:.4f}" if not pd.isna(df_prices.loc[date, t]) else "" 
                 for t in tickers]
        output_lines.append(",".join([date_str] + prices))
    
    # 5. 写入CSV
    with open(CSV_FILE, 'w', encoding='utf-8-sig') as f:
        f.write("\n".join(output_lines))
    
    print(f"✅ 成功更新 {CSV_FILE}, 包含 {len(sorted_dates)} 个交易日")

if __name__ == "__main__":
    update_csv()
