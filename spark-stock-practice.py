from pyspark.sql import SparkSession
from FinanceData import FinanceData
import sys

spark = SparkSession.builder.master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
fd = FinanceData()

stock_price_pdf = fd.get_stock_prices(sys.argv[1])
stock_price_df = spark.createDataFrame(stock_price_pdf)
stock_price_df.createOrReplaceTempView("stockPriceTable")


### 시가, 종가 기준 하루 등락폭 ###
stock_price_cmo = stock_price_df.selectExpr(
	"*",
	"(CLOSE - OPEN) as CMO",
	"((CLOSE - OPEN) / OPEN) as CMOR")
stock_price_cmo.explain()
stock_price_cmo.show(5)

cmo_sql = spark.sql("""
					SELECT *, (CLOSE - OPEN) as CMO, (CLOSE - OPEN) / OPEN as CMOR
					FROM stockPriceTable
					LIMIT 5""")
cmo_sql.explain()
cmo_sql.show(5)


### 하루 등락폭 기준 정렬 ###
stock_price_cmor_sort = stock_price_cmo.filter(stock_price_cmo["CMOR"] \
									   .isNotNull()) \
									   .sort(stock_price_cmo["CMOR"] \
									   .desc())
stock_price_cmor_sort.explain()
stock_price_cmor_sort.show(5)


### 종가를 USD 기준으로 변경 ###
USDKRW_pdf = fd.get_USDKRW_rate()
USDKRW_df = spark.createDataFrame(USDKRW_pdf)
USDKRW_df.createOrReplaceTempView("USDKRWTable")

joinExpr = stock_price_df["Date"] == USDKRW_df["Date"]
# stock_price_df.join(USDKRW_df, joinExpr).selectExpr(
# 	"*",
# 	"CLOSE / CLOSE") <--- 컬럼명 중복

# 컬럼명 중복 회피하기 위해 rename 해야 함
stock_price_df2 = stock_price_df.withColumnRenamed("CLOSE", "CLOSE_stock")
stock_price_df2.createOrReplaceTempView("stockPriceTable2")
USDKRW_df2 = USDKRW_df.withColumnRenamed("CLOSE", "CLOSE_USDKRW")
USDKRW_df2.createOrReplaceTempView("USDKRWTable2")

stock_price_df2_usd = stock_price_df2.join(USDKRW_df2, joinExpr) \
									 .selectExpr("CLOSE_stock / CLOSE_USDKRW as CLOSE_USD")
stock_price_df2_usd.explain()
stock_price_df2_usd.show(5)

USD_sql = spark.sql("""
					SELECT (CLOSE_stock / CLOSE_USDKRW) as CLOSE_USD
					FROM stockPriceTable2
					JOIN USDKRWTable2
					ON stockPriceTable2.Date = USDKRWTable2.Date
					""")
USD_sql.explain()
USD_sql.show(5)

