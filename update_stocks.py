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
    """获取股票名称和价格数据（含异常处理与随机延迟）"""
    try:
        tkr = yf.Ticker(ticker)
        time.sleep(random.uniform(0.5, 1.5))  # 随机延迟避免API限流
        
        # 获取公司名称（优先长名称， fallback到短名称或 ticker）
        name = tkr.info.get('longName') or tkr.info.get('shortName') or ticker
        
        # 获取历史收盘价（仅保留START_DATE后的数据）
        hist = tkr.history(start=START_DATE, actions=False)
        if hist.empty:
            print(f"⚠️ 警告: {ticker} 无有效历史数据")
            return name, pd.Series(dtype=float)
            
        return name, hist['Close']
    
    except Exception as e:
        print(f"❌ 获取{ticker}失败: {str(e)}")
        return ticker, pd.Series(dtype=float)

def update_csv():
    # 1. 正确读取现有CSV的股票代码（仅提取"ticker"到"name"之间的有效代码）
    tickers = []
    
    if os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                if first_line:
                    parts = first_line.split(',')
                    # 关键修复：找到"name"列位置，仅提取其之前的股票代码
                    if 'name' in parts:
                        name_idx = parts.index('name')
                        valid_codes = parts[1:name_idx]  # 跳过第一个"ticker"列
                    else:
                        valid_codes = parts[1:]  # 无"name"时取所有后续列
                    # 过滤空值与无效项
                    tickers = [code.strip() for code in valid_codes if code.strip() and code != 'ticker']
                    
            print(f"📄 检测到现有股票代码: {tickers}")
        except Exception as e:
            print(f"⚠️ 读取文件头失败: {e}")
    
    # 添加默认股票（去重）
    for t in DEFAULT_TICKERS:
        if t not in tickers:
            tickers.append(t)
    
    print(f"🎯 最终股票列表: {tickers}")

    # 2. 下载股票数据（名称+收盘价）
    stock_names = {}
    price_data = {}
    
    for ticker in tickers:
        name, prices = get_stock_info(ticker)
        stock_names[ticker] = name
        if not prices.empty:
            price_data[ticker] = prices
        time.sleep(0.3)  # 基础延迟
    
    if not price_data:
        print("❌ 所有股票下载失败，终止更新")
        return
    
    # 3. 合并价格数据（按日期对齐，空值留白）
    all_dates = set()
    for ts in price_data.values():
        all_dates.update(ts.index)
    sorted_dates = sorted(all_dates)
    
    # 构建DataFrame（行：日期，列：股票代码）
    df_prices = pd.DataFrame(index=sorted_dates, columns=tickers)
    for ticker, prices in price_data.items():
        df_prices[ticker] = prices  # 自动对齐日期，无数据则为NaN
    
    # 4. 生成CSV内容（规范格式：表头→名称行→数据行）
    output_lines = []
    
    # 第1行：表头（ticker + 股票代码）
    header = ["ticker"] + tickers
    output_lines.append(",".join(header))
    
    # 第2行：公司名称（name + 对应名称）
    names_row = ["name"] + [stock_names[t] for t in tickers]
    output_lines.append(",".join(names_row))
    
    # 数据行：日期 + 各股票收盘价（空值留白）
    for date in sorted_dates:
        date_str = date.strftime("%Y/%m/%d")  # 统一日期格式
        price_values = []
        for t in tickers:
            val = df_prices.loc[date, t]
            price_values.append(f"{val:.4f}" if not pd.isna(val) else "")  # 保留4位小数
        output_lines.append(",".join([date_str] + price_values))
    
    # 5. 写入CSV（用utf-8-sig避免乱码）
    with open(CSV_FILE, 'w', encoding='utf-8-sig') as f:
        f.write("\n".join(output_lines))
    
    print(f"✅ 成功更新 {CSV_FILE}，包含 {len(sorted_dates)} 个交易日数据")

if __name__ == "__main__":
    update_csv()
