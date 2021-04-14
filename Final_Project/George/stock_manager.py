from utils.three_plus_one import *
from utils.sql_func import *





if __name__ == "__main__":
    today = int(datetime.now().strftime("%Y%m%d"))
    # today = 20210326
    ticker_list = get_buy_ticker(today, validate_days=270, slice_size=10, top_buy=3)
    print("check to buy list", "\n", ticker_list)

    ticker_list.to_csv("logs/ticker_to_buy.csv")

    insert_purchase(ticker_list)

    df_sold_intervals = get_all_slice_size(conn)
    stock_sell_list = pd.read_sql("select * from stock_purchase where 0=1", conn)
    stock_sell_list.to_csv("logs/ticker_to_sell.csv")
    for index, row in df_sold_intervals.iterrows():
        sold_interval = row.sale_interval
        buy_date = get_buy_date_from_date_and_slice(slice_size=sold_interval, today=datetime.now().strftime("%Y-%m-%d"))
        if not buy_date is None:
            stock_df = get_stock_buy_df_by_buy_date(conn, buy_date)
            if not len(stock_df) == 0:
                stock_sell_list = stock_sell_list.append(stock_df, ignore_index=True, sort=False)

                print("Check and sell folowing", "\n", stock_sell_list)
            else:
                print("Nothing to sell today")

        else:
            print("not yet to sell")


