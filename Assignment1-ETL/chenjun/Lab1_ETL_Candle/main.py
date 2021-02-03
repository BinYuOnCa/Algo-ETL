from ETL_Runner import us_equity_candles_ETL_run
import finnhub

if __name__ == '__main__':

   us_equity_candles_ETL_run("1m")
   us_equity_candles_ETL_run("daily")

