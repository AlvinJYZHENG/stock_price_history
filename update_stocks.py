import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta
import traceback # 新增：用于打印详细错误堆栈

# --- 配置区域 ---
DEFAULT_TICKERS = ["AAPL", "MSFT", "0700.HK", "600519.SS"] 
CSV_FILE = "stock_price_history.csv"
# ----------------

def get_stock_data():
    try:
        # 1. 确定日期范围
        start_date = "2026-01-01"
        
        if os.path.exists(CSV_FILE):
            df = pd.read_csv(CSV_FILE, index_col=0, parse_dates=True)
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
                
            if not df.empty:
                last_date = df.index.max().date()
                yesterday = datetime.now().date() - timedelta(days=1)
                
                if last_date >= yesterday:
                    print(f"数据已是最新 (最新日期: {last_date})，跳过更新。")
                    return
                
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"检测到已有数据，将从 {start_date} 开始增量更新...")
        else:
            df = pd.DataFrame()
            print("文件不存在，将创建新文件。")

        tickers_to_check = list(df.columns) if not df.empty else DEFAULT_TICKERS
        all_tickers = list(set(tickers_to_check + DEFAULT_TICKERS))
        print(f"准备更新以下股票: {all_tickers}")

        end_date = datetime.now().strftime("%Y-%m-%d")
        
        try:
            data = yf.download(all_tickers, start=start_date, end=end_date, progress=False)
            
            if len(all_tickers) == 1:
                data.columns = pd.Index([all_tickers[0]])
            
            if isinstance(data.columns, pd.MultiIndex):
                adj_close = data['Adj Close']
            else:
                adj_close = data

            df = pd.concat([df, adj_close])
            df = df[~df.index.duplicated(keep='last')]
            df = df.sort_index()

            df.index = df.index.strftime('%Y/%m/%d')
            df.to_csv(CSV_FILE)
            print(f"成功更新并保存数据到 {CSV_FILE}，最新行数: {len(df)}")

        except Exception as e:
            print(f"下载数据时出错: {e}")
            # 打印详细堆栈信息
            traceback.print_exc()
            raise e

    except Exception as e:
        print(f"脚本执行过程中发生未知错误: {e}")
        # 打印详细堆栈信息
        traceback.print_exc()
        raise e

if __name__ == "__main__":
    get_stock_data()
