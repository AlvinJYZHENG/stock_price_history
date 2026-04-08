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

def get_stock_name_yf(symbol):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        return info.get('longName') or info.get('shortName') or symbol
    except Exception as e:
        print(f"获取{symbol}名称失败（不影响数据）: {e}")
        return symbol

def download_single_ticker(ticker, start_date=START_DATE):
    end_date = datetime.now().strftime("%Y-%m-%d")
    try:
        time.sleep(random.uniform(1, 3))
        print(f"下载中: {ticker}...")
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        
        if data.empty:
            print(f"警告: {ticker}无有效数据")
            return None
            
        close_series = data["Close"].copy()
        close_series.name = ticker
        return close_series
        
    except Exception as e:
        print(f"下载{ticker}失败: {e}")
        return None

def update_stock_data():
    target_tickers = DEFAULT_TICKERS.copy()
    
    if os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, "r", encoding="utf-8-sig") as f:
                headers = f.readline().strip().split(",")
            file_tickers = headers[1:] if headers and headers[0] == "" else headers
            target_tickers = list(set(file_tickers + DEFAULT_TICKERS))
            print(f"检测到历史代码: {file_tickers}")
        except Exception as e:
            print(f"读取文件头失败（用默认列表）: {e}")
    
    print(f"本次更新股票: {target_tickers}")

    all_series = []
    for ticker in target_tickers:
        series = download_single_ticker(ticker)
        if series is not None:
            all_series.append(series)
        time.sleep(1)
    
    if not all_series:
        print("所有股票下载失败，终止运行")
        return

    # 合并数据
    combined_df = pd.concat(all_series, axis=1).sort_index()
    final_columns = combined_df.columns.tolist()

    # 获取股票名称
    print("获取最新股票名称...")
    stock_names = [get_stock_name_yf(code) for code in final_columns]
    time.sleep(0.5)

    # 准备价格数据（四舍五入到4位小数）
    price_df = combined_df[final_columns].round(4)
    
    # 创建最终报表（符合要求的格式）
    # 1. 创建空列表存储所有行
    all_rows = []
    
    # 2. 添加第一行：股票代码
    all_rows.append([""] + final_columns)  # 第一列留空（日期位置）
    
    # 3. 添加第二行：股票名称
    all_rows.append([""] + stock_names)   # 第一列留空（日期位置）
    
    # 4. 添加价格数据（从第三行开始）
    for date_idx, row_data in price_df.iterrows():
        # 格式化日期为 YYYY/MM/DD
        date_str = date_idx.strftime("%Y/%m/%d")
        # 添加日期和价格数据
        all_rows.append([date_str] + row_data.tolist())
    
    # 5. 创建DataFrame
    final_df = pd.DataFrame(all_rows, columns=["日期"] + final_columns)
    
    # 6. 保存为CSV（不包含索引）
    final_df.to_csv(CSV_FILE, encoding="utf-8-sig", index=False)
    print(f"成功更新{CSV_FILE}，总行数: {len(final_df)}")

if __name__ == "__main__":
    update_stock_data()
