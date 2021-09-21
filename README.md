# Intake Google Analytics driver

[Intake][intake.readthedocs.io] driver for [Google Analytics Queries](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py)

## Install

```
conda install -c defusco -c conda-forge intake-google-analytics
```

## Sample query

All queries require
* the `view_id` for your Google Analytics account
* a start date and end date
* at least one [metric](https://ga-dev-tools.web.app/dimensions-metrics-explorer/) to compute
* Service account credentials JSON file

The easiest way to get the `view_id` is to use the
[Analytics Explorer](https://ga-dev-tools.web.app/account-explorer/).

To authenticate to the Core Reporting API v4 you will need a [service account]() and JSON credentials file.
Follow the steps [here](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py#1_enable_the_api) to prepare your account and download the credentials. Typically, this
file is called `client_secrets.json`.

To compute the total number of visitors between 5 days ago and yesterday the following query
is constructed.

```python
import intake

ds = intake.open_google_analytics_query(
    view_id='<view_id>',
    start_date='5DaysAgo',
    end_date='yesterday',
    metrics=['ga:users'],
    credentials_path='client_secrets.json'
)

df = ds.read()
print(df)
```

The output is

```
   ga:users
0     301012
```

## Date ranges

This driver accepts a number of formats for both `start_date` and `end_date`

* Specific string values: `'yesterday'`, `'today'`, `'<N>DaysAgo'`
* Datetime-parsable strings in format `'YYYY-MM-DD'`
* `pd.TimeStamp`, `datetime.date`, or `datetime.datetime` objects

These data types can be mixed. Here's a few examples

GA strings only:

```python
ds = intake.open_google_analytics_query(
    view_id='<view_id>',
    start_date='30DaysAgo',
    end_date='yesterday',
    metrics=['ga:users'],
    credentials_path='client_secrets.json'
)
```


Datetime objects and GA strings:

```python
import pandas as pd

ds = intake.open_google_analytics_query(
    view_id='<view_id>',
    start_date=pd.to_datetime('March 19, 2020'),
    end_date='30DaysAgo',
    metrics=['ga:users'],
    credentials_path='client_secrets.json'
)
```

Datetime objects with differencing:

```python
import pandas as pd
import datetime as dt

ds = intake.open_google_analytics_query(
    view_id='<view_id>',
    start_date=dt.datetime.now() - pd.DateOffset(months=6),
    end_date=pd.to_datetime('today') - dt.timedelta(days=1),
    metrics=['ga:users'],
    credentials_path='client_secrets.json'
)
```


## Metrics, Dimensions, and Filters

The easiest way to learn how to build queries and gather the required information is to use the
[Query Explorer](https://ga-dev-tools.web.app/query-explorer/). Refer to the
[UA Dimensions and Metrics explorer](https://ga-dev-tools.web.app/dimensions-metrics-explorer/)
for documentation on all available fields.


### Metric expressions

Metrics are specified as a list of strings or dictionaries and required for all queries.
The output DataFrame will have columns that match the metric expressions.
When using the dictionary format the `'expression'` key is required and the `'alias'` key is
optional to change the column name in the DataFrame.
GA [metric expressions](https://developers.google.com/analytics/devguides/reporting/core/v4/basics#expressions)
are supported for both string and dictionary inputs. Here's an example
of the supported types of `metrics`.

```python
ds = intake.open_google_analytics_query(
    view_id='<view_id>',
    start_date='5DaysAgo',
    end_date='yesterday',
    metrics=[
        'ga:users',                      # simple string value
        'ga:goal1completions/ga:users',  # metric expression as string
        {'expression': 'ga:sessions'},   # the expression key is required for dicts
        {
            'expression': 'ga:sessionDuration/ga:sessions', # expression
            'alias': 'time-per-session'                     # rename the column
        }
    ],
    credentials_path='client_secrets.json'
)
```

### Dimensions

In addition to metrics an optional list of fields can be provided as dimensions.
See the [UA Dimensions and Metrics Explorer](https://ga-dev-tools.web.app/dimensions-metrics-explorer/)
for the full list of available dimensions. Dimensions are provided as a list of strings.
Unlike metrics dimensions do not support aliasing.

```python
ds = intake.open_google_analytics_query(
    view_id='<view_id>',
    start_date='5DaysAgo',
    end_date='yesterday',
    metrics=[
        'ga:users',
        {
            'expression': 'ga:sessionDuration/ga:sessions', 
            'alias': 'time-per-session'
        }
    ],
    dimensions = ['ga:userType', 'ga:browser']
    credentials_path='client_secrets.json'
)
```

### Filtering

Filters can be applied on metrics or dimensions. See the 
[filters documentation](https://developers.google.com/analytics/devguides/reporting/core/v3/reference#filters) 
for more details.
