import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime

# --- 配置区域 ---
DEFAULT_TICKERS = ["AAPL", "MSFT", "600519.SS", "0700.HK"]
CSV_FILE = "stock_price_history.csv"
# ----------------

def get_stock_name_yf(symbol):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        return info.get('longName') or info.get('shortName') or symbol
    except Exception as e:
        print(f"获取 {symbol} 名称失败: {e}")
        return symbol

def download_full_history(ticker, start_date="2016-01-01"):
    end_date = datetime.now().strftime("%Y-%m-%d")
    try:
        time.sleep(random.uniform(1, 3))
        print(f"下载: {ticker}...")
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        
        if data.empty:
            print(f"警告: {ticker} 数据为空")
            return None
            
        return data['Close'].rename(ticker)  # 直接返回收盘价序列
        
    except Exception as e:
        print(f"下载 {ticker} 失败: {str(e)}")
        return None

def get_stock_data():
    target_tickers = DEFAULT_TICKERS.copy()
    
    # 修复列名读取逻辑
    if os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                headers = f.readline().strip().split(',')
            file_tickers = headers[1:] if headers and headers[0] == '' else headers
            target_tickers = list(set(file_tickers + DEFAULT_TICKERS))
            print(f"检测到代码: {file_tickers}")
        except Exception as e:
            print(f"读取文件头失败: {e}")
    
    print(f"更新股票: {target_tickers}")

    all_data_series = []
    for ticker in target_tickers:
        series_data = download_full_history(ticker)
        if series_data is not None:
            all_data_series.append(series_data)
        time.sleep(1)  # 防封锁延迟

    if not all_data_series:
        print("所有股票下载失败")
        return

    # 合并数据
    combined_df = pd.concat(all_data_series, axis=1).sort_index()
    final_columns = combined_df.columns.tolist()

    # 获取股票名称
    print("获取股票名称...")
    names = [get_stock_name_yf(t) for t in final_columns]
    time.sleep(0.5)

    # 构建结果
    result_df = pd.DataFrame({
        "代码": final_columns,
        "名称": names
    })
    
    # 添加价格数据
    price_df = combined_df[final_columns].round(4)
    full_df = pd.concat([
        pd.DataFrame([final_columns], columns=final_columns),
        pd.DataFrame([names], columns=final_columns),
        price_df
    ])
    
    # 设置索引
    date_index = price_df.index.strftime('%Y/%m/%d')
    full_df.index = ["股票代码", "股票名称"] + date_index.tolist()
    
    # 保存结果
    full_df.to_csv(CSV_FILE, encoding='utf-8-sig')
    print(f"已更新 {CSV_FILE}, 共{len(full_df)}行")

if __name__ == "__main__":
    get_stock_data()
