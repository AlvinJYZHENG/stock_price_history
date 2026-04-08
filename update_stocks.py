import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime, timedelta

# --- 配置区域 ---
DEFAULT_TICKERS = ["AAPL", "MSFT", "600519.SS", "0700.HK"]
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
    # 1. 获取股票代码列表
    # 策略：只读取第一行，忽略后面的所有内容
    target_tickers = DEFAULT_TICKERS.copy()
    
    if os.path.exists(CSV_FILE):
        try:
            # 读取第一行作为 header，nrows=0 表示不读取数据行
            # 这样可以只获取列名（股票代码）
            temp_df = pd.read_csv(CSV_FILE, nrows=0)
            # 将文件中的代码和默认代码合并，防止遗漏
            file_tickers = list(temp_df.columns)
            target_tickers = list(set(file_tickers + DEFAULT_TICKERS))
            print(f"从文件中检测到代码: {file_tickers}")
        except Exception as e:
            print(f"读取文件头失败，使用默认列表: {e}")
    
    print(f"本次任务将更新以下股票: {target_tickers}")

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
    # 使用 outer join 确保所有日期都被包含
    combined_df = pd.concat(all_data_series, axis=1, join='outer')
    combined_df = combined_df.sort_index() # 按日期排序

    # 4. 格式化输出
    print("正在生成最终报表...")
    
    final_columns = list(combined_df.columns)
    
    # 第一行：股票代码 (保持不变)
    row_codes = pd.Series(final_columns, index=final_columns)
    
    # 第二行：股票名称 (每次重新匹配)
    print("正在获取最新股票名称...")
    row_names_list = []
    for ticker in final_columns:
        name = get_stock_name_yf(ticker)
        row_names_list.append(name)
        time.sleep(0.5) # 获取名称也稍微延迟一下
    row_names = pd.Series(row_names_list, index=final_columns)
    
    # 数据部分：四舍五入到小数点后4位
    df_data = combined_df[final_columns].copy()
    # 处理可能的 NaN 值，虽然 yfinance 通常返回连续数据，但以防万一
    # 注意：applymap 在 Pandas 新版本中推荐使用 map，但 applymap 对 DataFrame 更通用
    df_data = df_data.applymap(lambda x: round(x, 4))
    
    # 拼接最终结果
    # 结构：[代码行, 名称行, 数据行...]
    final_output = pd.concat([row_codes.to_frame().T, row_names.to_frame().T, df_data])
    
    # 设置索引：前两行是表头，后面是日期
    date_index = [d.strftime('%Y/%m/%d') for d in df_data.index]
    final_output.index = ["股票代码", "股票名称"] + date_index

    # 5. 强制覆盖写入
    final_output.to_csv(CSV_FILE, encoding='utf-8-sig')
    print(f"成功覆盖写入 {CSV_FILE}，总行数: {len(final_output)}")

if __name__ == "__main__":
    get_stock_data()
