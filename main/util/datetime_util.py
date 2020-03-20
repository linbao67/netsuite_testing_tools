from datetime import datetime,timedelta

import pytz

DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'  # 2019-09-01T00:00:00.000-07:00
DUE_DATE_FORMAT = '%Y-%m-%d'


def add_days(date_string, days, date_format=DATE_FORMAT):
    utc_date = datetime.fromisoformat(date_string).astimezone(pytz.utc)
    calc_date = utc_date + timedelta(days=days)
    calc_date_str = datetime.strftime(calc_date, date_format)
    return calc_date_str


def getUTCDateTime(date_string, date_format=DATE_FORMAT):
    utc_date = datetime.fromisoformat(date_string).astimezone(pytz.utc)

    return datetime.strftime(utc_date, date_format)

