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
        
        # 获取历史数据（仅收盘价）
        hist = tkr.history(start=START_DATE, actions=False)
        if hist.empty:
            print(f"⚠️ 警告: {ticker} 无有效数据")
            return name, pd.Series(dtype=float)
            
        return name, hist['Close']
    
    except Exception as e:
        print(f"❌ 获取{ticker}失败: {str(e)}")
        return ticker, pd.Series(dtype=float)

def update_csv():
    # 1. 确定目标股票列表（修复：正确读取现有CSV的股票代码）
    tickers = []
    
    if os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                if first_line:
                    parts = first_line.split(',')
                    # 关键修复：仅提取“ticker”到“name”之间的股票代码（忽略“name”及之后的内容）
                    if 'name' in parts:
                        name_idx = parts.index('name')
                        codes = parts[1:name_idx]  # 取“ticker”后到“name”前的列
                    else:
                        codes = parts[1:]  # 无“name”时取所有后续列
                    # 过滤空值和无效项
                    tickers = [code.strip() for code in codes if code.strip() and code != 'ticker']
                    
            print(f"📄 检测到现有股票代码: {tickers}")
        except Exception as e:
            print(f"⚠️ 读取文件头失败: {e}")
    
    # 添加默认股票（避免重复）
    for t in DEFAULT_TICKERS:
        if t not in tickers:
            tickers.append(t)
    
    print(f"🎯 最终股票列表: {tickers}")

    # 2. 下载数据（保持不变）
    stock_names = {}
    price_data = {}
    
    for ticker in tickers:
        name, prices = get_stock_info(ticker)
        stock_names[ticker] = name
        if not prices.empty:
            price_data[ticker] = prices
        time.sleep(0.3)  # 基础延迟
    
    if not price_data:
        print("❌ 所有股票下载失败")
        return
    
    # 3. 合并价格数据（保持不变，但仅保留有效日期）
    all_dates = set()
    for ts in price_data.values():
        all_dates.update(ts.index)
    sorted_dates = sorted(all_dates)
    df_prices = pd.DataFrame(index=sorted_dates, columns=tickers)
    
    for ticker, prices in price_data.items():
        df_prices[ticker] = prices  # 仅填充有效价格，保留NaN
    
    # 4. 构建输出（优化：确保空值留白，用逗号分隔）
    output_lines = []
    
    # 第一行：ticker + 有效股票代码
    header = ["ticker"] + tickers
    output_lines.append(",".join(header))
    
    # 第二行：name + 对应公司名称
    names_row = ["name"] + [stock_names[t] for t in tickers]
    output_lines.append(",".join(names_row))
    
    # 数据行：日期 + 价格（空值留白）
    for date in sorted_dates:
        date_str = date.strftime("%Y/%m/%d")
        prices = []
        for t in tickers:
            val = df_prices.loc[date, t]
            prices.append(f"{val:.4f}" if not pd.isna(val) else "")
        output_lines.append(",".join([date_str] + prices))
    
    # 5. 写入CSV（用utf-8-sig避免乱码）
    with open(CSV_FILE, 'w', encoding='utf-8-sig') as f:
        f.write("\n".join(output_lines))
    
    print(f"✅ 成功更新 {CSV_FILE}, 包含 {len(sorted_dates)} 个交易日")

if __name__ == "__main__":
    update_csv()
