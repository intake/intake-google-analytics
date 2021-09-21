import datetime as dt
import pandas as pd


def as_day(timestamp):
    return timestamp.strftime('%Y-%m-%d')


def is_dt(value):
    return isinstance(value, (dt.datetime, dt.date, pd.Timestamp))
