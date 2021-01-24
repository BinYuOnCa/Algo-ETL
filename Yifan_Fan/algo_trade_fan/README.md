# ETL_Documentaion

> This ETL process is designed for extracting stack candles data from [Finnhub.io](http://finnhub.io) in **Algorithmic Trading Project**.
The process will firstly download the daily-level and minute-level data from the web API, and then upload them into AWS RDS database after passing the initial consistency and integrity check.

# Requirements

1. `ds_env.yml` — Required Packages List
Please make sure your have installed all of them before executing.
2. `etl_info.env` — The document to save your confidential information.
Please fill up this document as following:

    ```
    FINNHUB_API_KEY = 'your finnhub private API key'

    RDS_USER = 'RDS user name'
    RDS_PASSWORD = 'RDS password'
    RDS_HOST = 'rds_name.featurecodehere.us-east.rds.amazonaws.com'
    RDS_DATABASE = 'database name'
    RDS_NAME = 'rds name'
    RDS_PORT = 'rds port, default 5432'

    TWILIO_ACCOUNT_SID = 'twilio account sid'
    TWILIO_AUTH_TOKEN = 'twilio authorization token'
    TWILIO_PHONE = '+1234567890'
    USER_PHONE = '+1987654321'

    EMAIL_SENDER_NAME = 'use_this_to_send@email.com'
    EMAIL_SENDER_PWD = 'sender_password'
    EMAIL_RECEIVER = 'use_this_to_reiceive@email.com'
    ```

    and then save it as `./etl_utils/etl_info.env`

3. `sec_list_100.csv` — The csv file to store the specific stacks abbreviation

    ```
    **name**
    ABC
    DEF
    ...
    ```

    Please make sure you have the column name "**name**" which will be used for list reading.

    Save it as `./stack_info/sec_list_1000.csv`

# 1. Start Up

## Table Design

The ETL process is linked with 3 tables on remote database. You could customize the table names in `./etl_utils/etl_config.py` under the catalog RDS_CONFIG.

- DAILY_TABLE: Store the 20 years daily stack candles.
- INTRADAY_TABLE: Store the 1 year minute-level stack candles.

| close_price | high_price | low_price | open_price | status | timestamp           | volume | symbol |
| ----------- | ---------- | --------- | ---------- | ------ | ------------------- | ------ | ------ |
| 10.13       | 15.62      | 7.23      | 9.1        | ok     | 2002-01-21 00:00:00 | 100    | AAA    |
| 22          | 30.33      | 15        | 20.4       | ok     | 2021-01-22 00:11:00 | 200    | BBB    |
| 33.21       | 40         | 25.12     | 30         | ok     | 2020-12-15 12:20:00 | 300    | CCC    |

- SPLIT_TABLE: Store the detected (according to historical value pair) and extracted (directly extracted from Finnhub API) splits information.

| symbol | date       | fromFactor | toFactor | source |
| ------ | ---------- | ---------- | -------- | ------ |
| AAA    | 2021-01-20 | 1          | 5        | api    |
| BBB    | 2020-12-19 | 32855      | 117734   | detect |
| CCC    | 2021-01-01 | 0          | 1121     | detect |

`api` : this split information is extracted from finnhub.io. 

`detect`: this record is detected by data comparing, the `fromFactor` is the value stored in database last time, and the `toFactor` is the newest value extracted from finnhub.io. You can customize the detect thresholds in `./etl_utils/etl_config.py` under the catalog USER_CUSTOM. See more details in [section 2.3]().

## Pre-load Stack List

At beginning, the process will build up a stack list from database, which contains

- `stack`: The stack symbol
- `last_time`: The latest datetime of the stack in the table
- `volume`: The corresponding volume of the last time.

This pre-load table will help the main process to check and compare stacks faster. It may take ~4 mins if you are processing the intraday data extraction.

Please set the USER_CUSTOM["FIRST_RUN"] as True if you are using the this process first time, then the pre-load step will be skipped, and all the stacks will be reloaded.

# 2. Main Process

## 2.1 Check if the stack existed already

For each stack, if it is not in the pre-load stack list, the process will download all available data for them.

- For daily data: the range is 20 years to today, daily-level; (cost ~1s for each)
- For intraday data: the range is 1 year to today, minute-level. (cost ~12s for each)
*(When downloading the intraday data, because the API will only return one month minute-level for each call, the one-year period will be divided into 12 months time-series at first, and then  upload them to database separately. )*

If the stack is already in database table, go next.

## 2.2 Check the gap period data

For the existed stack, the process will firstly check the **length of gap period**: latest datetime in database `last_time` — current datetime `current_time`) 

If the gap period is longer than USER_CUSTOM["CHECK_HOUR"] (default 1 hour), the process will then download the data for the gap period , and then compare the corresponding **volumes**: the database volume of the last time (`db_vol`) and the new extracted data from API (`api_vol`).

- if `db_vol` equals to `api_vol`:
"Perfect match", the process will then upload the rest part of the data to database;

Considering that the data from finnub.io, especially the minute-level data, are keep jumping each time because of the API adjustment. 

The partial match is also acceptable:

- if the rate of change (`api_vol`-`db_vol`)/`db_vol` is less than USER_CUSTOM["T_LEVEL"] (default 0.005);

or

- if the difference (`api_vol`-`db_vol`) is less than USER_CUSTOM["T_NUMBER"] (default 1000);

These two scenarios will be considered as "Partial match", and the process will also upload the rest of the data to database as in the perfect match scenario.

If the stack data are neither perfect nor partial matched, go next.

## 2.3 Check the splits information

If the volume data cannot be matched, the split/merge behavior will be considered.

Firstly, the unmatched situation will be recorded as "detected split" (see table example), and then all the information will be saved to RDS_CONFIG["SPLIT_TABLE"] for future inspection: the `fromFactor` will be replaced by `db_vol`, and the `toFactor` will save the `api_vol` value.

After recording the detection, the process will then extract the gap period splits information from finnhub.io. If there is no evidence that shows the split/merge occurs, the rest of the data will be uploaded. Otherwise, the API splits behavior will be marked, and the stack will be reloaded: all data of this stack will be removed, and its historical (20 yrs for daily, 1 yr for intraday) data will be redownloaded.

# 3. Customized Content

> All the customized configurations can be modified in `./etl_utils/etl_config.py`

- FINNHUB_CONFIG["API_LIMIT"]
The [finnhub.io](http://finnhub.io) API has an limit request time as 60/min to avoid exceeding the limit. The API revisit time interval can be set. Default interval is 1.25 seconds.
This setting is also available when working in multiple threads processes.
- FINNHUB_CONFIG["INTRADAY_LIMIT"]
Because the API will only return the minute-level data for 30 days interval, longer period will be divided into several shorter time series. This setting will set the length of time interval. Default is 30 days.
- USER_CUSTOM["POSTPONE"]
In order to keep data consistency and avoid data jumping, users can set a postponed time interval to download data in the past. 
For example, the default setting is 24 hours, which means the data will be updated up to yesterday (24 hours ago) if the process executes now.
- USER_CUSTOM["ALERT"]
- If the process is successfully executed, send an email to assigned receiver;
- If the process exits with any exceptions, message the error information immediately to assigned phone number.
Default is True.
- USER_CUSTOM["MULTILINE"]
Run the "daily-level" and "minute-level" data ETL process in parallel.
Won't exceed the API request limit.
Default is True.
- `./auto_mission.sh`
Set to your own directory, and then use `Crontab` to schedule the mission.