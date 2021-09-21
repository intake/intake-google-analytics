import pandas as pd
import datetime as dt
from intake_google_analytics.utils import is_dt, as_day


def test_is_dt():
    assert is_dt(dt.date(2020, 3, 19))
    assert is_dt(dt.datetime(2020, 3, 19, 16, 4, 0))
    assert is_dt(pd.to_datetime('2020-03-19'))
    assert is_dt(pd.Timestamp(2020, 3, 19))

    assert not is_dt('2020-03-19')
    assert not is_dt(dt.timedelta(days=1))
    assert not is_dt(pd.DateOffset(months=2))


def test_as_day():
    assert as_day(dt.date(2020, 3, 19)) == '2020-03-19'
    assert as_day(dt.datetime(2020, 3, 19, 16, 4, 0)) == '2020-03-19'
    assert as_day(pd.to_datetime('2020-03-19')) == '2020-03-19'
    assert as_day(pd.to_datetime('2020-03-19 16:04:00')) == '2020-03-19'
    assert as_day(pd.Timestamp(2020, 3, 19)) == '2020-03-19'
