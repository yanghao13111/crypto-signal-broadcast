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

    def generate_signals(self):
        signals = []
        for symbol, group in self.df.groupby("symbol"):
            last_two = group.tail(2)
            if len(last_two) == 2 and (last_two["k_type"] == 1).all():
                signals.append(symbol)
        return signals

    def print_signals(self):
        signals = self.generate_signals()
        print(f"\n📊 【{self.label}】策略選出 {len(signals)} 個符合條件的標的：\n")
        for symbol in signals:
            print(f"🔔 Symbol: {symbol}")

# test
if __name__ == "__main__":
    spot_strategy = DoubleBullishStrategy(csv_path="okx_spot_1d.csv", label="Spot DoubleBullish")
    spot_strategy.print_signals()

    swap_strategy = DoubleBullishStrategy(csv_path="okx_swap_1d.csv", label="Swap DoubleBullish")
    swap_strategy.print_signals()
