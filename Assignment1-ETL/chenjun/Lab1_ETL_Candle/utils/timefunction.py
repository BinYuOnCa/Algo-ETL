import datetime as dt
end_date = dt.datetime.today()
start_date = end_date + dt.timedelta(minutes=1)

print (start_date)

start = int(start_date.timestamp())

print(start)

stock = 'SQ'
resolution = 'D'
end_date = dt.datetime.now()

start_date = end_date - dt.timedelta(days=365)
end = int(end_date.timestamp())
start = int(start_date.timestamp())

