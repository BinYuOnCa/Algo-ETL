import datetime
import pytz


# Global timezone classes
cst_tz = pytz.timezone('Asia/Shanghai')
est_tz = pytz.timezone('US/Eastern')


def usest_str_to_ts(dt_str):
    # convert time str to UNIX timestamp base on US east time
    # input time str format 'yyyymmdd' or 'yyyymmdd hh:mm:ss' as US eastern time
    if len(dt_str) < 9:
        dt_str = dt_str + ' 00:00:00'
    dt = est_tz.localize(datetime.datetime.strptime(dt_str, '%Y%m%d %H:%M:%S'))
    # print(dt)
    return int(dt.timestamp())


def utc_str_to_ts(dt_str):
    dt = datetime.datetime.strptime('%Y%m%d')
    ts = dt.timestamp()
    return int(ts)


def utc_dt_from_timestamp(unix_timestamp):
    utc_dt = datetime.datetime.utcfromtimestamp(unix_timestamp)
    date_key_int = int(utc_dt.strftime('%Y%m%d'))
    time_key = utc_dt.strftime('%H:%M:%S')
    return date_key_int, time_key


def usest_str_from_ts(unix_timestamp):
    naive_utc_dt = datetime.datetime.utcfromtimestamp(unix_timestamp)
    utc_dt = pytz.utc.localize(naive_utc_dt)
    usest_dt = utc_dt.astimezone(est_tz)
    return usest_dt.strftime('%Y-%m-%d %H:%M:%S')



if __name__ == '__main__':
    # now = datetime.datetime.strptime('20210110 10:00:00', '%Y%m%d %H:%M:%S')
    # print('raw now:', now, now.timestamp(), usest_str_to_ts('20210110 10:00:00'))
    # est_localized_now = est_tz.localize(now)
    # est_astimezone_now = now.astimezone(est_tz)
    # cst_localized_now = cst_tz.localize(now)
    # ts1 = datetime.datetime.timestamp(est_localized_now)
    # ts2 = datetime.datetime.timestamp(est_astimezone_now)
    # ts3 = datetime.datetime.timestamp(cst_localized_now)
    # print('est localized now:', est_localized_now, ts1)
    # print('est astimezone now:', est_astimezone_now, ts2)
    # print('cst localized now:', cst_localized_now, ts3)
    now_ts = datetime.datetime.now().timestamp()
    print(now_ts)
    unix_ts = now_ts
    us_east_time = usest_str_from_ts(unix_ts)
    print(us_east_time)


