import datetime as dt
import re
from collections import OrderedDict

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient import discovery
from pandas.api.types import is_string_dtype

DTYPES = {
    "INTEGER": int,
    "TIME": float,
    "PERCENT": float,
    "STRING": str,
    "CURRENCY": float
}

DATETIME_FORMATS = OrderedDict([
    ('%Y%m', re.compile(r'^(?P<year>[0-9]{4})(?P<month>1[0-2]|0[1-9])$')),
    ('%Y%m%d', re.compile(r'^(?P<year>[0-9]{4})(?P<month>1[0-2]|0[1-9])(?P<day>3[01]|0[1-9]|[12][0-9])$')),
    ('%Y%m%d%H', re.compile(r'^(?P<year>[0-9]{4})(?P<month>1[0-2]|0[1-9])(?P<day>3[01]|0[1-9]|[12][0-9])(?P<hour>2[0-3]|[01][0-9])$')),
    ('%Y%m%d%H%M', re.compile(r'^(?P<year>[0-9]{4})(?P<month>1[0-2]|0[1-9])(?P<day>3[01]|0[1-9]|[12][0-9])(?P<hour>2[0-3]|[01][0-9])(?P<minute>[0-5][0-9])$'))
])


def to_dataframe(report, parse_dates=True):
    headers = report['columnHeader']

    columns = headers.get('dimensions', [])
    metric_columns = headers['metricHeader']['metricHeaderEntries']

    dtypes = {}
    for c in metric_columns:
        name = c['name']
        columns.append(name)
        dtypes[name] = DTYPES[c['type']]

    data = []
    for row in report['data']['rows']:

        dim_values = row.get('dimensions', [])

        this_row = []
        metric_values = row['metrics'][0]['values']

        this_row.extend(dim_values)
        this_row.extend(metric_values)

        data.append(this_row)

    df = pd.DataFrame(data=data, columns=columns)
    for c, dtype in dtypes.items():
        df[c] = df[c].astype(dtype)

    if parse_dates:
        for c, v in df.iloc[[0]].iteritems():
            if is_string_dtype(v):
                for fmt,regex in DATETIME_FORMATS.items():
                    if v.str.fullmatch(regex).all():
                        df[c] = pd.to_datetime(df[c], format=fmt)
                        break

    return df


def as_day(timestamp):
    return timestamp.strftime('%Y-%m-%d')


def is_dt(value):
    return isinstance(value, (dt.datetime, dt.date, pd.Timestamp))


def ua_query(view_id, start_date, end_date, metrics, dimensions=None, filters=None):
    ga = discovery.build('analyticsreporting', 'v4',
                         cache_discovery=False).reports()

    date_range = {'startDate': start_date, 'endDate': end_date}
    for key, value in date_range.items():
        if is_dt(value):
            date_range[key] = as_day(value)
        elif value.lower() in ['yesterday', 'today']:
            date_range[key] = value.lower()
        elif re.match(r'\d+DaysAgo', value):
            pass
        else:
            raise ValueError(f'{key}={value} is not a supported date.\n'
                             f'Please use a date/datetime object.')

    body = {
        'reportRequests': []
    }
    request = {
        'viewId': view_id,
        'dateRanges': [date_range],
        'metrics': metrics
    }

    if dimensions:
        request['dimensions'] = dimensions

    if filters:
        request['filtersExpression'] = filters

    body['reportRequests'].append(request)

    result = ga.batchGet(body=body).execute()
    report = result['reports'][0]

    dfs = [to_dataframe(report)]
    print(report.keys())
    while report.get('nextPageToken'):
        body['reportRequests'][0]['pageToken'] = report.get('nextPageToken')
        result = ga.batchGet(body=body).execute()
        report = result['reports'][0]
        dfs.append(to_dataframe(report))

    df = pd.concat(dfs, ignore_index=True)
    return df
