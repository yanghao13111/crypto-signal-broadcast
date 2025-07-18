import ccxt
import pandas as pd
import time
from datetime import datetime, timedelta
import os

class OKXDailyFetcher:
    def __init__(self, market_type="spot", quote_currency="USDT", limit=30, sleep_sec=0):
        assert market_type in ("spot", "swap"), "market_type must be 'spot' or 'swap'"
        self.exchange = ccxt.okx()
        self.market_type = market_type
        self.quote_currency = quote_currency
        self.limit = limit
        self.sleep_sec = sleep_sec
        self.symbols = self._load_symbols()
        self.filename = f"okx_{self.market_type}_1d.csv"
        self.existing_df = self._load_existing()
        print(f"✅ 共有 {len(self.symbols)} 個 {market_type} 標的")

    def _load_symbols(self):
        markets = self.exchange.load_markets()
        if self.market_type == "spot":
            return [
                s for s, v in markets.items()
                if v.get("spot") and s.endswith(f"/{self.quote_currency}")
            ]
        else:  # swap
            return [
                s for s, v in markets.items()
                if v.get("contract") and v.get("type") == "swap" and s.endswith(f":{self.quote_currency}")
            ]

    def _load_existing(self):
        if os.path.exists(self.filename):
            df = pd.read_csv(self.filename, dtype={"timestamp": str})
            print(f"📂 已讀取現有資料：{len(df)} 筆")
            return df
        return pd.DataFrame()

    def _get_last_timestamp(self, symbol):
        """回傳 symbol 的最後一天 timestamp（字串格式 YYYY-MM-DD）"""
        if self.existing_df.empty or symbol not in self.existing_df["symbol"].unique():
            return None  # 新幣種
        last_time = self.existing_df[self.existing_df["symbol"] == symbol]["timestamp"].max()
        last_dt = datetime.strptime(last_time, "%Y-%m-%d")
        return last_dt - timedelta(days=2)

    def _fetch_ohlcv(self, symbol, since_ts=None):
        try:
            params = {
                "timeframe": "1d",
                "limit": self.limit,
            }
            if since_ts:
                params["since"] = int(since_ts.timestamp() * 1000)

            raw = self.exchange.fetch_ohlcv(symbol, **params)
            if not raw:
                return pd.DataFrame()

            df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = df["timestamp"].apply(
                lambda ts: datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            )
            df["symbol"] = symbol
            df["k_type"] = df.apply(lambda row: 1 if row["close"] > row["open"] else -1, axis=1)
            return df[["timestamp", "symbol", "open", "high", "low", "close", "k_type"]]
        except Exception as e:
            print(f"❌ 抓取 {symbol} 失敗: {e}")
            return pd.DataFrame()

    def fetch_all(self):
        new_data = []
        for idx, symbol in enumerate(self.symbols):
            print(f"[{idx+1}/{len(self.symbols)}] 處理 {symbol} ...")
            last_ts = self._get_last_timestamp(symbol)
            if last_ts:
                print(f"  🔄 有歷史資料，從 {last_ts.date()} 開始更新")
            else:
                print(f"  🆕 新幣種，抓最近 {self.limit} 天")

            df = self._fetch_ohlcv(symbol, since_ts=last_ts)
            if not df.empty:
                new_data.append(df)
            time.sleep(self.sleep_sec)
        return new_data

    def save_updated_csv(self):
        new_data = self.fetch_all()
        if not new_data:
            print("⚠️ 沒有任何新資料")
            return

        new_df = pd.concat(new_data, ignore_index=True)

        # 合併舊資料並去重：以 symbol + timestamp 做主鍵，保留最新（覆蓋今日資料）
        combined = pd.concat([self.existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["timestamp", "symbol"], keep="last")
        combined = combined.sort_values(by=["symbol", "timestamp"])
        combined.to_csv(self.filename, index=False)
        print(f"\n✅ 資料已更新並儲存至 {self.filename}，總共 {len(combined)} 筆資料")


# 執行用
if __name__ == "__main__":
    print("=== ✅ 抓取 OKX Spot ===")
    spot = OKXDailyFetcher(market_type="spot")
    spot.save_updated_csv()

    print("\n=== ✅ 抓取 OKX Swap ===")
    swap = OKXDailyFetcher(market_type="swap")
    swap.save_updated_csv()
