class FinanceData:
    def __init__(self):
        import FinanceDataReader as fdr

        self.data_reader = fdr.DataReader


    def get_stock_prices(self, symbol):
        try:
            df = self.data_reader(symbol, '2015')
            df["Date"] = df.index.to_series()
            return df

        except ValueError:
            raise ValueError("Invalid ticker symbol")


    def get_USDKRW_rate(self):
        df = self.data_reader('USD/KRW', '2015')
        df["Date"] = df.index.to_series()
        return df
