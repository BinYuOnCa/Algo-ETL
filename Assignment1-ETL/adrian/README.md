# ETL Assignment 

## RDS server info for checking assignment
>algotrading-db.cjvjda9idzvk.ca-central-1.rds.amazonaws.com

>port: 5432

## Notes

- Due to very slow connection speed from China to US, I did not extract bulk quantity of stocks from finnhub.
  There are only 14 stocks in my database. Here is a list of the stocks:
    ```
    TSLA
    APT
    PLTR
    NIO
    FAST
    ROKU
    TDOC
    CRSP
    SQ
    NVTA
    Z
    BIDU
    SPOT
    PRLB
    ```
- I did not add in SMS notification since Twilio seems to require a US or Canada phone number, which I currently do not have.
  I checked some of the SMS API providers in China but none of them is free. So I just insert the code as comment for demo.
  You can find it under classes/notice.py/class Notice(), defined as send_sms() method.


## What the code does
1. It extracts the candle data of listed stocks in 'stock_symbol.csv' according to the settings in the code (daily and 1 minute data).
2. It removes the status column from the response.
3. It converts the Unix timestamp into human readable datetime formats and add them to the dataframe. The converted datetime uses US Eastern time.
4. It then transfer the data to tables named 'us_stock_daily' and 'us_stock_1m' for daily candle and 1 minute candle repectively into the designated Amazon RDS server.
5. It will send an email notification to the recipient set in .myenv, notifying the transfer was done. If any error occurs an **'error.log'** file will be attached to the email.

## Getting Started
1. Before start, create './utils/.myenv' file to store all needed credentials
What you need:
   * The '.myenv' file shall include following credentials:
    ```
    # RDS Database Credentials
    DB_NAME=database_name
    DB_USERNAME=your_username
    DB_PWD=your_password
    DB_HOST=your_postgres_server_address
    DB_PORT=your_database_port

    # Finnhub API
    FINN_API=your_finnhub_api
    FINN_API_SANDBOX=your_finnhub_api_for_sandbox
    FINN_WEBHOOK=your_finnhub_webhook

    # Email config
    MAIL_ACCOUNT=your_email_address
    MAIL_AUTH_CODE=your_email_login_password
    MAIL_SMTP_SERVER=smtp.youremail.com
    MAIL_SMTP_PORT=SSL_port_of_smtp_server
    MAIL_SENDER=sender's_email_address
    MAIL_RECEIVER=receiver's_email_address
    ```
2. Create your own stock list as 'stock_symbol.csv' with the header 'symbol'. An example file is given. These are the stocks that are going to be analyzed.
The csv file should look like this:
    ```
    symbol
    AAPL
    AMZN
    ADBE
    BABA
    SPOT
    DBX
    GM
    ```
3. You are ready to go.


## To be continued...