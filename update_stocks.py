import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime, timedelta

# --- 配置区域 ---
# 你可以在这里定义默认要监控的股票代码 (Yahoo Finance 格式)
DEFAULT_TICKERS = ["AAPL", "MSFT", "600519.SS", "0700.HK"]
CSV_FILE = "stock_price_history.csv"  # 保持原文件名
# ----------------

def get_stock_name_yf(symbol):
    """
    使用 yfinance 获取股票名称
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        # 优先获取 longName (全称)，如果没有则获取 shortName (简称)
        name = info.get('longName') or info.get('shortName')
        return name
    except Exception as e:
        # print(f"获取 {symbol} 名称失败: {e}") # 静默失败，避免刷屏
        return symbol # 失败则返回代码本身

def download_with_retry(ticker, start_date, end_date, max_retries=5):
    """
    下载股票数据，带重试机制
    """
    for retry in range(max_retries):
        try:
            # 延迟策略
            wait_time = min(300, (2 ** retry) * 2 + random.uniform(2, 5))
            if retry > 0:
                print(f"重试 {retry}/{max_retries}: {ticker}，等待 {wait_time:.1f} 秒...")
            else:
                print(f"开始下载 {ticker}，等待 {wait_time:.1f} 秒...")
            time.sleep(wait_time)

            data = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)

            if data.empty:
                print(f"警告: {ticker} 返回空数据")
                if retry < max_retries - 1:
                    time.sleep(min(60, 30 * (retry + 1)))
                continue

            # 提取收盘价
            if isinstance(data.columns, pd.MultiIndex):
                if ('Close', ticker) in data.columns:
                    close_price = data['Close']
                else:
                    close_price = data['Adj Close'] if ('Adj Close', ticker) in data.columns else data['Close']
                if isinstance(close_price.columns, pd.MultiIndex):
                    close_price = close_price[ticker]
            else:
                if 'Close' in data.columns:
                    close_price = data['Close']
                else:
                    close_price = data['Adj Close']

            if close_price.empty:
                if retry < max_retries - 1:
                    time.sleep(min(60, 30 * (retry + 1)))
                continue

            print(f"成功下载 {ticker}，数据行数: {len(close_price)}")
            return close_price

        except Exception as e:
            print(f"下载 {ticker} 失败 (尝试 {retry+1}/{max_retries}): {str(e)}")
            if retry == max_retries - 1:
                return None
            time.sleep(min(5, 2 * (retry + 1)))

    return None

def get_stock_data():
    # 1. 确定日期范围
    start_date = "2016-01-01"
    existing_df = pd.DataFrame()
    
    # 检查文件是否存在并读取旧数据
    if os.path.exists(CSV_FILE):
        # 注意：这里读取的是包含表头行（代码、名称）的文件
        # 我们需要跳过前两行来读取实际的日期索引
        try:
            existing_df = pd.read_csv(CSV_FILE, index_col=0, skiprows=2, parse_dates=True)
            if not isinstance(existing_df.index, pd.DatetimeIndex):
                existing_df.index = pd.to_datetime(existing_df.index)
            
            if not existing_df.empty:
                last_date = existing_df.index.max().date()
                yesterday = datetime.now().date() - timedelta(days=1)
                
                if last_date >= yesterday:
                    print(f"数据已是最新 (最新日期: {last_date})，跳过更新。")
                    return
                
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"检测到已有数据，将从 {start_date} 开始增量更新...")
        except Exception as e:
            print(f"读取旧文件出错: {e}，将重新创建。")

    # 2. 确定股票列表
    # 从旧数据的列名获取，如果没有则用默认
    tickers_to_check = list(existing_df.columns) if not existing_df.empty else DEFAULT_TICKERS
    all_tickers = list(set(tickers_to_check + DEFAULT_TICKERS))
    print(f"准备更新以下股票: {all_tickers}")

    end_date = datetime.now().strftime("%Y-%m-%d")

    # 3. 下载数据
    adj_close_list = []
    successful_tickers = []

    for i, ticker in enumerate(all_tickers):
        print(f"正在下载 {ticker} ({i+1}/{len(all_tickers)})...")
        data = download_with_retry(ticker, start_date, end_date)

        if data is not None:
            if isinstance(data, pd.Series):
                data_df = pd.DataFrame({ticker: data})
            else:
                data_df = data
            adj_close_list.append(data_df)
            successful_tickers.append(ticker)
        
        if i < len(all_tickers) - 1:
            time.sleep(random.uniform(1, 3))

    if not adj_close_list:
        print("所有股票下载都失败，无数据可保存")
        return

    # 4. 合并数据
    try:
        if len(adj_close_list) == 1:
            adj_close_combined = adj_close_list[0]
        else:
            adj_close_combined = pd.concat(adj_close_list, axis=1, join='outer')
    except Exception as e:
        print(f"合并数据时出错: {e}")
        return

    # 5. 合并到主 DataFrame
    # 将新下载的数据追加到原有数据
    final_raw_df = pd.concat([existing_df, adj_close_combined])

    # 去重和排序
    final_raw_df = final_raw_df[~final_raw_df.index.duplicated(keep='last')]
    final_raw_df = final_raw_df.sort_index()

    # --- 核心修改部分：格式化并保存 ---
    
    print("正在生成带名称的最终报表...")
    
    # 确保列顺序一致
    final_columns = list(final_raw_df.columns)
    
    # 第一行：股票代码
    header_row = pd.Series(final_columns, index=final_columns)
    
    # 第二行：股票名称
    name_row_data = []
    for ticker in final_columns:
        name = get_stock_name_yf(ticker)
        name_row_data.append(name)
    
    name_row = pd.Series(name_row_data, index=final_columns)
    
    # 数据行：四舍五入到小数点后4位
    data_part = final_raw_df[final_columns].copy()
    data_part = data_part.applymap(lambda x: round(x, 4))
    
    # 拼接：表头 + 名称 + 数据
    final_output = pd.concat([header_row.to_frame().T, name_row.to_frame().T, data_part])
    
    # 设置索引名称（日期格式）
    final_output.index = ["股票代码", "股票名称"] + [d.strftime('%Y/%m/%d') for d in data_part.index]
    
    # 6. 保存 CSV (直接覆盖原文件)
    final_output.to_csv(CSV_FILE, encoding='utf-8-sig')
    print(f"成功更新并保存数据到 {CSV_FILE}，最新行数: {len(final_output)}")

if __name__ == "__main__":
    get_stock_data()
