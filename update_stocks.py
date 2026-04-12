import pandas as pd
import yfinance as yf
import os
import time
import random
from datetime import datetime

# --- 配置区域 ---
DEFAULT_TICKERS = ["AAPL", "MSFT", "0700.HK", "600519.SS"]  # 三个文件共用相同默认代码
CSV_FILES = ["stock_price_history.csv", "stock_price_history_hk.csv", "stock_price_history_us.csv"]
START_DATE = "2016-01-01"  # 数据起始时间
# ----------------

def get_stock_info(ticker):
    """获取股票名称和价格数据（使用原始时区处理）"""
    try:
        tkr = yf.Ticker(ticker)
        time.sleep(random.uniform(0.5, 1.5))  # 随机延迟避免API限流
        
        # 获取公司名称（去逗号）
        raw_name = tkr.info.get('longName') or tkr.info.get('shortName') or ticker
        clean_name = raw_name.replace(',', '')
        
        # 获取历史收盘价
        hist = tkr.history(start=START_DATE, actions=False)
        if hist.empty:
            print(f"⚠️ 警告: {ticker} 无有效历史数据")
            return clean_name, pd.Series(dtype=float)
            
        # 原始时区处理（移除时区信息）
        hist.index = hist.index.tz_localize(None)
            
        return clean_name, hist['Close']
    
    except Exception as e:
        print(f"❌ 获取{ticker}失败: {str(e)}")
        return ticker, pd.Series(dtype=float)

def update_csv(csv_file):
    print(f"\n📊 开始更新 {csv_file}...")
    tickers = []
    
    # 1. 读取现有CSV的股票代码（首行是ticker，第二行是name）
    if os.path.exists(csv_file):
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                # 读取首行（ticker行）
                header_line = f.readline().strip()
                if header_line:
                    header_parts = header_line.split(',')
                    # 第一个元素是"ticker"，后面是股票代码
                    if header_parts[0] == "ticker" and len(header_parts) > 1:
                        tickers = [code.strip() for code in header_parts[1:] if code.strip()]
                    
            print(f"📄 检测到现有股票代码: {tickers}")
        except Exception as e:
            print(f"⚠️ 读取文件头失败: {e}")
    
    # 如果未获取到代码，使用默认代码
    if not tickers:
        tickers = DEFAULT_TICKERS.copy()
        print(f"ℹ️ 使用默认股票代码: {tickers}")
    else:
        print(f"🎯 最终股票列表: {tickers}")

    # 2. 下载股票数据
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
    
    # 3. 合并价格数据
    all_dates = set()
    for ts in price_data.values():
        all_dates.update(ts.index)
    sorted_dates = sorted(all_dates)
    
    # 构建DataFrame
    df_prices = pd.DataFrame(index=sorted_dates, columns=tickers)
    for ticker, prices in price_data.items():
        df_prices[ticker] = prices
    
    # 4. 生成CSV内容（首行ticker，次行name）
    output_lines = []
    
    # 第1行：表头（ticker + 股票代码）
    header = ["ticker"] + tickers
    output_lines.append(",".join(header))
    
    # 第2行：name + 公司名称
    names_row = ["name"] + [stock_names[t] for t in tickers]
    output_lines.append(",".join(names_row))
    
    # 数据行：日期 + 价格
    for date in sorted_dates:
        date_str = date.strftime("%Y/%m/%d")
        price_values = []
        for t in tickers:
            val = df_prices.loc[date, t]
            price_values.append(f"{val:.4f}" if not pd.isna(val) else "")
        output_lines.append(",".join([date_str] + price_values))
    
    # 5. 写入CSV（全覆盖模式）
    with open(csv_file, 'w', encoding='utf-8-sig') as f:
        f.write("\n".join(output_lines))
    
    print(f"✅ 成功更新 {csv_file}，包含 {len(sorted_dates)} 个交易日数据")

if __name__ == "__main__":
    for csv_file in CSV_FILES:
        update_csv(csv_file)
    print("\n🎉 所有文件更新完成！")
