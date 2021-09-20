import datetime as dt
import re
from collections import OrderedDict
from typing import Union

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient import discovery
from intake.source.base import DataSource, Schema
from pandas.api.types import is_string_dtype

from . import __version__

DTYPES = {
    "INTEGER": int,
    "TIME": float,
    "PERCENT": float,
    "STRING": str,
    "CURRENCY": float
}

DateTypes = Union[str, dt.date, dt.datetime, pd.Timestamp]

DATETIME_FORMATS = OrderedDict([
    ('%Y%m', re.compile(r'^(?P<year>[0-9]{4})(?P<month>1[0-2]|0[1-9])$')),
    ('%Y%m%d', re.compile(r'^(?P<year>[0-9]{4})(?P<month>1[0-2]|0[1-9])(?P<day>3[01]|0[1-9]|[12][0-9])$')),
    ('%Y%m%d%H', re.compile(r'^(?P<year>[0-9]{4})(?P<month>1[0-2]|0[1-9])(?P<day>3[01]|0[1-9]|[12][0-9])(?P<hour>2[0-3]|[01][0-9])$')),
    ('%Y%m%d%H%M', re.compile(r'^(?P<year>[0-9]{4})(?P<month>1[0-2]|0[1-9])(?P<day>3[01]|0[1-9]|[12][0-9])(?P<hour>2[0-3]|[01][0-9])(?P<minute>[0-5][0-9])$'))
])


class GoogleAnalyticsQuerySource(DataSource):
    """
    Run a Google Analytics (Universal Analytics) query and return a Data Frame
    """

    name = 'google-analytics-query'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, view_id, start_date, end_date,
                 metrics, dimensions=None, filters=None, include_empty=False,
                 credentials_path=None,
                 metadata=None):

        self._df = None

        self._view_id = view_id
        self._start_date = start_date
        self._end_date = end_date
        self._metrics = metrics
        self._dimensions = dimensions
        self._filters = filters
        self._include_empty = include_empty
        self._credentials_path = credentials_path

        self._client = GoogleAnalyticsAPI(credentials_path=credentials_path)

        super(GoogleAnalyticsQuerySource, self).__init__(metadata=metadata)

    def _get_schema(self):
        if self._df is None:
            self._df = self._client.query(
                view_id=self._view_id,
                start_date=self._start_date, end_date=self._end_date,
                metrics=self._metrics,
                dimensions=self._dimensions,
                filters=self._filters,
                include_empty=self._include_empty
            )

        return Schema(datashape=None,
                      dtype=self._df.dtypes,
                      shape=(None, len(self._df.columns)),
                      npartitions=1,
                      extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._df

    def read(self):
        self._get_schema()
        return self._df

    def to_dask(self):
        raise NotImplementedError()

    def _close(self):
        self._dataframe = None


class GoogleAnalyticsAPI(object):
    def __init__(self, credentials_path=None):
        credentials = None

        if credentials_path:
            credentials = Credentials.from_service_account_file(credentials_path)

        self.client = discovery.build('analyticsreporting', 'v4',
                                      credentials=credentials,
                                      cache_discovery=False).reports()

    def query(self, view_id: str, start_date: DateTypes, end_date: DateTypes, metrics: list,
              dimensions: list = None, filters: list = None, include_empty: bool = False):

        date_range = {'startDate': start_date, 'endDate': end_date}
        for key, value in date_range.items():
            if self._is_dt(value):
                date_range[key] = self._as_day(value)
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
            'includeEmptyRows': include_empty
        }

        request['metrics'] = self._parse_fields(metrics, style='metrics')

        if dimensions:
            request['dimensions'] = self._parse_fields(dimensions, style='dimensions')

        if filters:
            request['filtersExpression'] = filters

        body['reportRequests'].append(request)

        result = self.client.batchGet(body=body).execute()
        report = result['reports'][0]

        dfs = [self._to_dataframe(report)]
        while report.get('nextPageToken'):
            body['reportRequests'][0]['pageToken'] = report.get('nextPageToken')
            result = self.client.batchGet(body=body).execute()
            report = result['reports'][0]
            dfs.append(self._to_dataframe(report))

        df = pd.concat(dfs, ignore_index=True)
        return df

    def _to_dataframe(self, report, parse_dates=True):
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
            first_row = df.iloc[[0]]
            string_columns = first_row.dtypes[first_row.dtypes.apply(is_string_dtype)].index
            for column in string_columns:
                for format, regex in DATETIME_FORMATS.items():
                    if first_row[column].str.fullmatch(regex).all():
                        df[column] = pd.to_datetime(df[column], format=format)
                        break  # continue to next column

        return df

    def _as_day(self, timestamp):
        return timestamp.strftime('%Y-%m-%d')

    def _is_dt(self, value):
        return isinstance(value, (dt.datetime, dt.date, pd.Timestamp))

    def _parse_fields(self, fields, style):
        if style not in ['metrics', 'dimensions', 'filters']:
            raise ValueError(f'{fields} is not supported')

        key = {
            'metrics': 'expression',
            'dimensions': 'name',
            'filters': ''
        }
        parsed = []
        errors = []
        for f in fields:
            if isinstance(f, str):
                parsed.append({key[style]:f})
            elif isinstance(f, dict):
                if key[style] in f:
                    parsed.append(f)
                else:
                    errors.append(f"""{f} does not contain "{key[style]}" key.""")
            else:
                errors.append(f'{f} is not a valid field string or dict')

        if errors:
            raise ValueError('\n'.join(errors))

        return parsed
