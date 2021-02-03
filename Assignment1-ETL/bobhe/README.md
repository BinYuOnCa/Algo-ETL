**Written by Bob HE**<br />
Assignment1 ETL<br />

**Part 1 - python program**<br />
main.py<br />
main program to manage one-off historical data load or daily incremental data load<br />
the stock data including the symbols listed in S&P500<br />
- daily bar data<br />
- minute bar data<br />

etl_daily.py<br />
etl process for daily incremental data load<br />

etl_one_off.py<br />
etl process for one-off historical data load<br />

utils<br />
utility package <br />

utils/db_util.py<br />
Various database utilities <br />

utils/log.py<br />
logging feature interface<br />

utils/mail.py<br />
gmail api interface<br />

uitls/sms.py<br />
twilio sms interface<br /> 

utils/param.py<br />
configuration , environment set up, constant etc...<br />

convert_date_time_format.py<br />
convert data time format interface<br />

**Part 2 -database design**<br />
Daily candle stick(bar) prices table: daily_prices<br />
DDL:<br />
-- public.daily_prices definition<br />
-- Drop table<br />
-- DROP TABLE public.daily_prices;<br />

CREATE TABLE public.daily_prices (<br />
	symbol text NOT NULL,<br />
	"close" float8 NULL,<br />
	high float8 NULL,<br />
	low float8 NULL,<br />
	"open" float8 NULL,<br />
	status text NOT NULL,<br />
	utc_unix_time int4 NOT NULL,<br />
	utc_readable_time text NOT NULL,<br />
	volume float8 NULL,<br />
	CONSTRAINT daily_prices_pkey PRIMARY KEY (symbol, utc_unix_time)<br />
);<br />

Minute candle stick(bar) prices table: min_prices<br />
-- public.min_prices definition<br />
-- Drop table<br />
-- DROP TABLE public.min_prices;<br />
CREATE TABLE public.min_prices (<br />
	symbol text NOT NULL,<br />
	"close" float8 NULL,<br />
	high float8 NULL,<br />
	low float8 NULL,<br />
	"open" float8 NULL,<br />
	status text NOT NULL,<br />
	utc_unix_time int4 NOT NULL,<br />
	local_time text NOT NULL,<br />
	volume float8 NULL,<br />
	CONSTRAINT min_prices_pkey PRIMARY KEY (symbol, utc_unix_time)<br />
);<br />

**Part 3 - ETL scheduling by Crontab**<br />
1. daily 22:15 to execute the main.py <br />
15 22 * * * /root/anaconda3/bin/python3.8 /root/algotrade/main.py <br />   

2. every Monday 21:30 to execute the archive.py <br />
30 21 * * 1 /root/anaconda3/bin/python3.8 /root/algotrade/archive.py
   










