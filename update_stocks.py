import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime, timedelta

# --- 配置区域 ---
# 你可以在这里定义默认要监控的股票代码 (Yahoo Finance 格式)
# 例如: 苹果(AAPL), 微软(MSFT), 腾讯(0700.HK), 茅台(600519.SS)
# 暂时只测试AAPL，避免速率限制
DEFAULT_TICKERS = ["AAPL"]
# DEFAULT_TICKERS = ["AAPL", "MSFT", "0700.HK", "600519.SS"]
CSV_FILE = "stock_price_history.csv"
# ----------------

def download_with_retry(ticker, start_date, end_date, max_retries=5):
    """
    下载股票数据，带重试机制和速率限制
    """
    for retry in range(max_retries):
        try:
            # 添加延迟避免过于频繁的请求（包括第一次尝试）
            # 使用指数退避策略，初始延迟更长
            wait_time = min(300, (2 ** retry) * 5 + random.uniform(5, 10))
            if retry > 0:
                print(f"重试 {retry}/{max_retries}: {ticker}，等待 {wait_time:.1f} 秒...")
            else:
                print(f"开始下载 {ticker}，等待 {wait_time:.1f} 秒以避免速率限制...")
            time.sleep(wait_time)

            data = yf.download(ticker, start=start_date, end=end_date, progress=False)

            # 检查数据是否为空
            if data.empty:
                print(f"警告: {ticker} 返回空数据，可能是速率限制")
                # 如果是空数据，等待更长时间再重试
                if retry < max_retries - 1:
                    extra_wait = min(180, 60 * (retry + 1))
                    print(f"空数据，额外等待 {extra_wait} 秒后重试...")
                    time.sleep(extra_wait)
                continue  # 继续重试

            # 提取复权收盘价
            if isinstance(data.columns, pd.MultiIndex):
                adj_close = data['Adj Close']
                # 对于多只股票返回MultiIndex的情况（实际不会发生在此函数，但保留）
                if isinstance(adj_close.columns, pd.MultiIndex):
                    adj_close = adj_close[ticker]
            else:
                adj_close = data

            # 再次检查提取的数据是否为空
            if adj_close.empty:
                print(f"警告: {ticker} 提取的收盘价数据为空")
                if retry < max_retries - 1:
                    extra_wait = min(180, 60 * (retry + 1))
                    print(f"空数据，额外等待 {extra_wait} 秒后重试...")
                    time.sleep(extra_wait)
                continue  # 继续重试

            print(f"成功下载 {ticker}，数据行数: {len(adj_close)}")
            return adj_close

        except Exception as e:
            error_msg = str(e)
            print(f"下载 {ticker} 失败 (尝试 {retry+1}/{max_retries}): {error_msg}")

            # 如果是速率限制错误，等待更长时间
            if "Rate limited" in error_msg or "Too Many Requests" in error_msg:
                extra_wait = min(300, 60 * (retry + 1))
                print(f"速率限制检测到，额外等待 {extra_wait} 秒...")
                time.sleep(extra_wait)

            if retry == max_retries - 1:
                print(f"重试 {max_retries} 次后仍失败，跳过 {ticker}")
                return None
            # 继续重试

    return None

def get_stock_data():
    # 1. 确定日期范围
    # 目标：从 2016/1/1 开始，或者从 CSV 中最后一行日期的下一天开始
    start_date = "2016-01-01"
    
    # 检查文件是否存在
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE, index_col=0, parse_dates=True)
        # 确保索引是 datetime 类型
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
            
        # 获取 CSV 中最新的日期
        if not df.empty:
            last_date = df.index.max().date()
            yesterday = datetime.now().date() - timedelta(days=1)
            
            # 如果最新数据已经是昨天，则无需更新
            if last_date >= yesterday:
                print(f"数据已是最新 (最新日期: {last_date})，跳过更新。")
                return
            
            # 从 CSV 最后一天之后开始下载
            start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"检测到已有数据，将从 {start_date} 开始增量更新...")
    else:
        # 文件不存在，创建一个新的 DataFrame
        df = pd.DataFrame()
        print("文件不存在，将创建新文件。")

    # 2. 确定需要下载的股票列表
    # 如果文件存在，获取现有的列名（股票代码）；否则使用默认列表
    tickers_to_check = list(df.columns) if not df.empty else DEFAULT_TICKERS
    
    # 合并默认列表和现有列表，防止代码丢失
    all_tickers = list(set(tickers_to_check + DEFAULT_TICKERS))
    print(f"准备更新以下股票: {all_tickers}")

    # 3. 下载数据
    # 结束日期设为今天（yfinance 会自动处理，通常不包含当天实时数据，取到昨天收盘）
    end_date = datetime.now().strftime("%Y-%m-%d")

    # 逐个下载股票数据，避免速率限制
    adj_close_list = []
    successful_tickers = []

    for i, ticker in enumerate(all_tickers):
        print(f"正在下载 {ticker} ({i+1}/{len(all_tickers)})...")

        # 使用重试机制下载单只股票数据
        data = download_with_retry(ticker, start_date, end_date)

        if data is not None:
            # 确保数据是Series或DataFrame格式
            if isinstance(data, pd.Series):
                # Series转换为DataFrame，列名为股票代码
                data_df = pd.DataFrame({ticker: data})
            else:
                # 已经是DataFrame
                data_df = data

            adj_close_list.append(data_df)
            successful_tickers.append(ticker)
        else:
            print(f"跳过 {ticker}，下载失败")

        # 添加随机延迟避免速率限制（除非是最后一只股票）
        if i < len(all_tickers) - 1:
            delay = random.uniform(1, 3)  # 1-3秒随机延迟
            time.sleep(delay)

    if not adj_close_list:
        print("所有股票下载都失败，无数据可保存")
        return

    # 4. 合并所有股票数据
    # 使用pd.concat一次性合并所有DataFrame，确保外连接包含所有日期
    try:
        if len(adj_close_list) == 1:
            adj_close_combined = adj_close_list[0]
        else:
            adj_close_combined = pd.concat(adj_close_list, axis=1, join='outer')
    except Exception as e:
        print(f"合并数据时出错: {e}")
        # 回退到逐列合并
        adj_close_combined = pd.DataFrame()
        for data_df in adj_close_list:
            if adj_close_combined.empty:
                adj_close_combined = data_df
            else:
                adj_close_combined = pd.concat([adj_close_combined, data_df], axis=1, join='outer')

    print(f"成功下载 {len(successful_tickers)}/{len(all_tickers)} 只股票: {successful_tickers}")

    # 5. 合并数据到主 DataFrame
    # 将新下载的数据追加到原有数据
    df = pd.concat([df, adj_close_combined])

    # 去重（以防万一日期重叠）
    df = df[~df.index.duplicated(keep='last')]

    # 按日期排序
    df = df.sort_index()

    # 6. 保存 CSV
    # 格式化为 YYYY/MM/DD
    df.index = df.index.strftime('%Y/%m/%d')
    df.to_csv(CSV_FILE)
    print(f"成功更新并保存数据到 {CSV_FILE}，最新行数: {len(df)}")

if __name__ == "__main__":
    get_stock_data()