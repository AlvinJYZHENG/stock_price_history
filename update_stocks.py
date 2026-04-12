import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime

# --- 配置区域 ---
DEFAULT_TICKERS = ["AAPL", "MSFT", "0700.HK", "600519.SS"]  # 补充示例港股/A股代码
CSV_FILES = ["stock_price_history.csv", "stock_price_history_hk.csv", "stock_price_history_us.csv"]
START_DATE = "2016-01-01"  # 注意：若需真实数据请修改为历史日期（如"2018-01-01"）
# ----------------

def get_stock_info(ticker):
    """获取股票名称（去逗号）和价格数据（修复时区问题）"""
    try:
        tkr = yf.Ticker(ticker)
        time.sleep(random.uniform(0.5, 1.5))  # 随机延迟避免API限流
        
        # 获取公司名称（优先长名称， fallback到短名称或ticker）
        raw_name = tkr.info.get('longName') or tkr.info.get('shortName') or ticker
        clean_name = raw_name.replace(',', '')  # 移除名称中的逗号（避免CSV错位）
        
        # 获取历史收盘价（仅保留START_DATE后的数据）
        hist = tkr.history(start=START_DATE, actions=False)
        if hist.empty:
            print(f"⚠️ 警告: {ticker} 无有效历史数据")
            return clean_name, pd.Series(dtype=float)
            
        # 关键修复：移除日期索引的时区信息（解决跨市场日期重复问题）
        hist.index = hist.index.tz_localize(None)  # 转为无时区的"朴素日期"
            
        return clean_name, hist['Close']
    
    except Exception as e:
        print(f"❌ 获取{ticker}失败: {str(e)}")
        return ticker, pd.Series(dtype=float)  # 异常时用ticker作为名称

def update_csv(csv_file):
    print(f"📊 开始更新 {csv_file}...")
    # 1. 读取现有CSV的股票代码（提取有效代码）
    tickers = []
    
    if os.path.exists(csv_file):
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                if first_line:
                    parts = first_line.split(',')
                    if 'name' in parts:
                        name_idx = parts.index('name')
                        valid_codes = parts[1:name_idx]  # 跳过第一个"ticker"列
                    else:
                        valid_codes = parts[1:]  # 无"name"时取所有后续列
                    tickers = [code.strip() for code in valid_codes if code.strip() and code != 'ticker']
                    
            print(f"📄 检测到现有股票代码 ({csv_file}): {tickers}")
        except Exception as e:
            print(f"⚠️ 读取文件头失败: {e}")
    
    # 如果CSV中没有代码，则使用默认股票
    if not tickers:
        tickers = DEFAULT_TICKERS.copy()
    # 否则直接使用CSV中的代码，不添加默认股票
    
    print(f"🎯 最终股票列表 ({csv_file}): {tickers}")

    # 2. 下载股票数据（名称+价格，名称已去逗号）
    stock_names = {}  # 存储处理后的公司名称（无逗号）
    price_data = {}   # 存储价格数据（键：ticker，值：无时区的Close序列）
    
    for ticker in tickers:
        name, prices = get_stock_info(ticker)
        stock_names[ticker] = name  # 名称已自动去逗号
        if not prices.empty:
            price_data[ticker] = prices
        time.sleep(0.3)  # 基础延迟
    
    if not price_data:
        print("❌ 所有股票下载失败，终止更新")
        return
    
    # 3. 合并价格数据（按日期对齐，空值留白）
    all_dates = set()
    for ts in price_data.values():
        all_dates.update(ts.index)  # 此时日期已无时区，同一日历日仅存一个对象
    sorted_dates = sorted(all_dates)  # 日期去重并排序
    
    # 构建DataFrame（行：日期，列：股票代码）
    df_prices = pd.DataFrame(index=sorted_dates, columns=tickers)
    for ticker, prices in price_data.items():
        df_prices[ticker] = prices  # 自动对齐日期，无数据则为NaN
    
    # 4. 生成CSV内容（第二行恢复公司名称，已去逗号）
    output_lines = []
    
    # 第1行：表头（ticker + 股票代码）
    header = ["ticker"] + tickers
    output_lines.append(",".join(header))
    
    # 第2行：name + 处理后的公司名称（已去逗号）
    names_row = ["name"] + [stock_names[t] for t in tickers]
    output_lines.append(",".join(names_row))
    
    # 第3行及以后：日期 + 各股票收盘价（每行一个日期，同一日期仅一行）
    for date in sorted_dates:
        date_str = date.strftime("%Y/%m/%d")  # 统一日期格式为YYYY/MM/DD
        price_values = []
        for t in tickers:
            val = df_prices.loc[date, t]
            price_values.append(f"{val:.4f}" if not pd.isna(val) else "")  # 保留4位小数，空值留白
        output_lines.append(",".join([date_str] + price_values))
    
    # 5. 写入CSV（用utf-8-sig避免乱码）
    with open(csv_file, 'w', encoding='utf-8-sig') as f:
        f.write("\n".join(output_lines))
    
    print(f"✅ 成功更新 {csv_file}，包含 {len(sorted_dates)} 个交易日数据（同一日期仅一行）")

if __name__ == "__main__":
    for csv_file in CSV_FILES:
        print(f"\n📂 正在处理文件: {csv_file}")
        update_csv(csv_file)
