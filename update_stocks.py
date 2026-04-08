import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime

# --- 配置区域 ---
DEFAULT_TICKERS = ["AAPL", "MSFT", "600519.SS", "0700.HK"]  # 默认跟踪的股票代码
CSV_FILE = "stock_price_history.csv"                       # 输出文件路径
START_DATE = "2016-01-01"                                   # 历史数据起始日
# ----------------

def get_stock_name_yf(symbol):
    """用yfinance获取股票名称（优先长名称， fallback到短名称/代码）"""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        return info.get('longName') or info.get('shortName') or symbol
    except Exception as e:
        print(f"获取{symbol}名称失败（不影响数据）：{e}")
        return symbol  # 失败时返回代码本身

def download_single_ticker(ticker, start_date=START_DATE):
    """下载单只股票的历史收盘价（修复冗余逻辑+规范命名）"""
    end_date = datetime.now().strftime("%Y-%m-%d")
    try:
        # 随机延迟防封禁（1-3秒）
        time.sleep(random.uniform(1, 3))
        print(f"下载中：{ticker}...")
        
        # 下载数据（关闭进度条）
        data = yf.download(
            tickers=ticker,
            start=start_date,
            end=end_date,
            progress=False
        )
        
        if data.empty:
            print(f"警告：{ticker}无有效数据")
            return None
        
        # 提取收盘价并重命名Series（直接用赋值更直观）
        close_series = data["Close"].copy()
        close_series.name = ticker  # 明确设置Series名称为股票代码
        return close_series
        
    except Exception as e:
        # 关键修复：不用str(e)，避免str被覆盖的风险
        print(f"下载{ticker}失败：{e}")
        return None

def update_stock_data():
    """主流程：合并新旧股票代码→下载数据→生成报表"""
    # 1. 确定需更新的股票列表（合并文件现有代码与默认代码）
    target_tickers = DEFAULT_TICKERS.copy()
    if os.path.exists(CSV_FILE):
        try:
            # 直接读CSV首行获取列名（跳过索引列）
            with open(CSV_FILE, "r", encoding="utf-8-sig") as f:
                headers = f.readline().strip().split(",")
            # 若首列是空字符串（索引列），则取后续列名
            file_tickers = headers[1:] if headers and headers[0] == "" else headers
            target_tickers = list(set(file_tickers + DEFAULT_TICKERS))
            print(f"检测到历史代码：{file_tickers}")
        except Exception as e:
            print(f"读取文件头失败（用默认列表）：{e}")
    
    print(f"本次更新股票：{target_tickers}")

    # 2. 批量下载数据（过滤失败项）
    all_series = []
    for ticker in target_tickers:
        series = download_single_ticker(ticker)
        if series is not None:
            all_series.append(series)
        time.sleep(1)  # 额外延迟防封禁
    
    if not all_series:
        print("所有股票下载失败，终止运行")
        return

    # 3. 合并数据（外连接保留所有日期，按日期排序）
    combined_df = pd.concat(all_series, axis=1, join="outer").sort_index()
    final_columns = combined_df.columns.tolist()  # 最终保留的股票代码顺序

    # 4. 获取最新股票名称（每行对应一个代码）
    print("获取最新股票名称...")
    stock_names = [get_stock_name_yf(code) for code in final_columns]
    time.sleep(0.5)  # 延迟避免频繁请求

    # 5. 构建最终报表（结构：代码行→名称行→每日价格）
    # 5.1 价格数据（四舍五入到4位小数，向量化操作更高效）
    price_df = combined_df[final_columns].round(4)
    # 5.2 代码行与名称行（转为DataFrame保证对齐）
    code_row = pd.DataFrame([final_columns], columns=final_columns)
    name_row = pd.DataFrame([stock_names], columns=final_columns)
    # 5.3 拼接所有行（代码→名称→价格）
    final_report = pd.concat([code_row, name_row, price_df], ignore_index=True)

    # 6. 设置索引（前两行是表头，后续是日期）
    date_index = price_df.index.strftime("%Y/%m/%d").tolist()
    final_report.index = ["股票代码", "股票名称"] + date_index

    # 7. 强制覆盖写入CSV（UTF-8-SIG避免中文乱码）
    final_report.to_csv(CSV_FILE, encoding="utf-8-sig")
    print(f"成功更新{CSV_FILE}，总行数：{len(final_report)}")

if __name__ == "__main__":
    update_stock_data()
