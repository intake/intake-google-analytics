import datetime as dt
import re
from collections import OrderedDict
from typing import Union
from numpy import exp

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient import discovery
from intake.source.base import DataSource, Schema
from pandas.api.types import is_string_dtype

from . import __version__
from .utils import as_day, is_dt

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

YYYY_MM_DD = re.compile(r'^(?P<year>[0-9]{4})-(?P<month>1[0-2]|0[1-9])-(?P<day>3[01]|0[1-9]|[12][0-9])$')


class GoogleAnalyticsQuerySource(DataSource):
    """
    Run a Google Analytics (Universal Analytics) query and return a Data Frame
    """

    name = 'google_analytics_query'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, view_id, start_date, end_date,
                 metrics, dimensions=None, filters=None,
                 credentials_path=None,
                 metadata=None):

        self._df = None

        self._view_id = view_id
        self._start_date = start_date
        self._end_date = end_date
        self._metrics = metrics
        self._dimensions = dimensions
        self._filters = filters
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
              dimensions: list = None, filters: list = None):
        result = self._query(
            view_id=view_id, start_date=start_date, end_date=end_date,
            metrics=metrics, dimensions=dimensions, filters=filters
        )

        df = self._to_dataframe(result)

        return df

    def _query(self, view_id: str, start_date: DateTypes, end_date: DateTypes, metrics: list,
              dimensions: list = None, filters: list = None):

        date_range = {'startDate': start_date, 'endDate': end_date}
        for key, value in date_range.items():
            if is_dt(value):
                date_range[key] = as_day(value)
            elif value.lower() in ['yesterday', 'today']:
                date_range[key] = value.lower()
            elif re.match(YYYY_MM_DD, value):
                pass
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
            'includeEmptyRows': True,
            'hideTotals': True,
            'hideValueRanges': True
        }

        request['metrics'] = self._parse_fields(metrics, style='metrics')

        if dimensions:
            request['dimensions'] = self._parse_fields(dimensions, style='dimensions')

        if filters:
            request['filtersExpression'] = filters

        body['reportRequests'].append(request)

        result = self.client.batchGet(body=body).execute()
        report = result['reports'][0]
        expected_rows = report['data']['rowCount']

        while result['reports'][0].get('nextPageToken'):
            body['reportRequests'][0]['pageToken'] = result['reports'][0].get('nextPageToken')
            result = self.client.batchGet(body=body).execute()
            report['data']['rows'].extend(result['reports'][0]['data']['rows'])

        gathered_rows = len(report['data']['rows'])
        if gathered_rows != expected_rows:
            raise RuntimeError(f'The query was expected to return {expected_rows} rows, '
                               f'but {gathered_rows} rows were retrieved.')

        return report

    @staticmethod
    def _to_dataframe(report, parse_dates=True):
        headers = report['columnHeader']

        columns = headers.get('dimensions', [])
        metric_columns = headers['metricHeader']['metricHeaderEntries']

        dtypes = {}
        for c in metric_columns:
            name = c['name']
            columns.append(name)
            dtypes[name] = DTYPES[c['type']]

        data = []
        rows = report['data'].get('rows', [])
        for row in rows:

            dim_values = row.get('dimensions', [])

            this_row = []
            metric_values = row.get('metrics', [{'values': [0] * len(metric_columns)}])[0]['values']

            this_row.extend(dim_values)
            this_row.extend(metric_values)

            data.append(this_row)

        df = pd.DataFrame(data=data, columns=columns)
        for c, dtype in dtypes.items():
            df[c] = df[c].astype(dtype)

        if df.any(axis=None) and parse_dates:
            first_row = df.iloc[[0]]
            string_columns = first_row.dtypes[first_row.dtypes.apply(is_string_dtype)].index
            for column in string_columns:
                for format, regex in DATETIME_FORMATS.items():
                    if first_row[column].str.fullmatch(regex).all():
                        df[column] = pd.to_datetime(df[column], format=format)
                        break  # continue to next column

        return df

    @staticmethod
    def _parse_fields(fields, style):
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
                parsed.append({key[style]: f})
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
