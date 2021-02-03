import config
from utils.smtp_mail import send_email

import argparse
import os
from datetime import date

# This script takes logs and emails them
# logs are stored in /home/ubuntu/logs in name like "log_[TIMEFRAME]"


def main(timeframes):
    # sends logs
    body = """
        ETL Job complete for today.

        Attached are logs reporting the day's activities
    """

    files = [f"{config.logs_folder}/log_{t}" for t in timeframes]

    send_email(subject="Daily Automated ETL Job Update", body=body, files=files)

    # Rename log
    for f in files:
        os.rename(f, f"{f}_{date.today()}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Email job that sends logs')
    parser.add_argument('timeframes', action="extend", metavar='timeframes', nargs='+', choices=config.available_timeframes,
            help=f'allowed values = {config.available_timeframes}')

    args = parser.parse_args()
    main(timeframes=args.timeframes)
