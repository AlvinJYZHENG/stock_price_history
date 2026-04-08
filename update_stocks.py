import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime

# --- 配置区域 ---
CSV_FILE = "stock_price_history.csv"
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
        print(f"获取 {symbol} 名称失败: {e}")
        return symbol # 失败则返回代码本身

def download_full_history(ticker, start_date="2016-01-01"):
    """
    下载股票的全部历史数据
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    try:
        # 随机延迟，防止被 Yahoo 封锁
        time.sleep(random.uniform(1, 3))
        print(f"正在全量下载: {ticker}...")
        
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        
        if data.empty:
            print(f"警告: {ticker} 数据为空")
            return None

        # 提取收盘价
        if isinstance(data.columns, pd.MultiIndex):
            close_price = data['Close'][ticker]
        else:
            close_price = data['Close']
            
        return close_price

    except Exception as e:
        print(f"下载 {ticker} 失败: {str(e)}")
        return None

def get_stock_data():
    # 1. 获取股票代码列表 (只读第一行)
    if not os.path.exists(CSV_FILE):
        print(f"错误: 文件 {CSV_FILE} 不存在。请先创建包含股票代码的文件。")
        return

    try:
        # 关键修改：nrows=0 表示只读取表头行（第一行），不读取数据
        # 这样 df.columns 就是 ["AAPL", "MSFT", ...]
        df_header = pd.read_csv(CSV_FILE, nrows=0)
        target_tickers = list(df_header.columns)
        
        if not target_tickers:
            print("错误: CSV 文件第一行为空。")
            return
            
        print(f"检测到股票代码: {target_tickers}")
    except Exception as e:
        print(f"读取文件失败: {e}")
        return

    # 2. 全量下载数据
    all_data_series = []
    
    for ticker in target_tickers:
        series_data = download_full_history(ticker)
        if series_data is not None:
            series_data.name = ticker # 确保 Series 有名字
            all_data_series.append(series_data)
        
        # 简单的防封锁延迟
        time.sleep(1)

    if not all_data_series:
        print("所有股票下载失败，停止运行。")
        return

    # 3. 合并数据
    combined_df = pd.concat(all_data_series, axis=1, join='outer')
    combined_df = combined_df.sort_index() # 按日期排序

    # 4. 格式化输出
    print("正在生成最终报表...")
    
    # --- 第一部分：读取已有的代码行 ---
    # 我们重新读取第一行，保持原样
    # skiprows=[1] 是为了跳过旧文件中的"名称行"，只取"代码行"作为列名
    # 但这里我们直接用 pd.read_csv 读第一行作为 Series
    code_row = pd.read_csv(CSV_FILE, nrows=1).iloc[0]
    # 确保索引（列名）顺序一致
    code_row = code_row[target_tickers] 

    # --- 第二部分：生成新的名称行 ---
    print("正在获取最新股票名称...")
    name_row_list = []
    for ticker in target_tickers:
        name = get_stock_name_yf(ticker)
        name_row_list.append(name)
        time.sleep(0.5) 
    name_row = pd.Series(name_row_list, index=target_tickers)
    
    # --- 第三部分：处理股价数据 ---
    # 确保列顺序与代码行一致
    df_data = combined_df[target_tickers].copy()
    # 四舍五入到小数点后4位
    df_data = df_data.applymap(lambda x: round(x, 4))
    
    # 5. 组装最终 DataFrame
    # 结构：[代码行, 名称行, 数据行...]
    final_output = pd.concat([code_row.to_frame().T, name_row.to_frame().T, df_data])
    
    # 设置索引
    date_index = [d.strftime('%Y/%m/%d') for d in df_data.index]
    final_output.index = ["股票代码", "股票名称"] + date_index

    # 6. 强制覆盖写入
    final_output.to_csv(CSV_FILE, encoding='utf-8-sig')
    print(f"成功覆盖写入 {CSV_FILE}，总行数: {len(final_output)}")

if __name__ == "__main__":
    get_stock_data()
