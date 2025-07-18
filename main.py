from data.okx_data_feed import OKXDailyFetcher
from strategies.double_bullish import DoubleBullishStrategy

class Pipeline:
    def __init__(self):
        self.spot_csv = "okx_spot_1d.csv"
        self.swap_csv = "okx_swap_1d.csv"

    def update_data(self):
        print("=== ✅ 更新 OKX Spot 資料 ===")
        spot_fetcher = OKXDailyFetcher(market_type="spot")
        spot_fetcher.save_updated_csv()

        print("\n=== ✅ 更新 OKX Swap 資料 ===")
        swap_fetcher = OKXDailyFetcher(market_type="swap")
        swap_fetcher.save_updated_csv()

    def run_strategies(self):
        print("\n=== 📈 執行策略：兩陽線 DoubleBullishStrategy ===")

        spot_strategy = DoubleBullishStrategy(csv_path=self.spot_csv, label="Spot DoubleBullish")
        spot_strategy.print_signals()

        swap_strategy = DoubleBullishStrategy(csv_path=self.swap_csv, label="Swap DoubleBullish")
        swap_strategy.print_signals()

    def run(self):
        self.update_data()
        self.run_strategies()

if __name__ == "__main__":
    Pipeline().run()