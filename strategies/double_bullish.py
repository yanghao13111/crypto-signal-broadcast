import pandas as pd

class DoubleBullishStrategy:
    def __init__(self, csv_path: str, label: str):
        self.csv_path = csv_path
        self.label = label
        self.df = self._load_data()

    def _load_data(self):
        df = pd.read_csv(self.csv_path)
        df = df.sort_values(by=["symbol", "timestamp"])
        return df

    def generate_signals(self, top_n=10):
        signals = []
        for symbol, group in self.df.groupby("symbol"):
            last_two = group.tail(2)
            if len(last_two) == 2 and (last_two["k_type"] == 1).all():
                volume = last_two.iloc[-1]["volume"]  # 最新一根的成交量
                signals.append((symbol, volume))
        
        # 根據 volume 排序，取前 top_n 名
        signals.sort(key=lambda x: x[1], reverse=True)
        return signals[:top_n], len(signals)


    def print_signals(self):
        top_signals, total = self.generate_signals()
        print(f"\n📊 【{self.label}】策略選出 {total} 個符合條件的標的，以下是成交量前 {len(top_signals)} 名：\n")
        for symbol, volume in top_signals:
            print(f"🔔 Symbol: {symbol}｜Volume: {volume}")


# test
if __name__ == "__main__":
    spot_strategy = DoubleBullishStrategy(csv_path="okx_spot_1d.csv", label="Spot DoubleBullish")
    spot_strategy.print_signals()

    swap_strategy = DoubleBullishStrategy(csv_path="okx_swap_1d.csv", label="Swap DoubleBullish")
    swap_strategy.print_signals()
