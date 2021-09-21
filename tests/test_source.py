import pytest
from intake_google_analytics.source import GoogleAnalyticsAPI


def test_parse_fields_wrong_style():
    with pytest.raises(ValueError):
        GoogleAnalyticsAPI._parse_fields(['ga:users'], style='nope')


def test_parse_metrics():
    metrics = ['ga:users']
    parsed = GoogleAnalyticsAPI._parse_fields(metrics, style='metrics')
    assert parsed == [{'expression': 'ga:users'}]

    metrics = ['ga:users', 'ga:session']
    parsed = GoogleAnalyticsAPI._parse_fields(metrics, style='metrics')
    assert parsed == [{'expression': 'ga:users'}, {'expression': 'ga:session'}]

    metrics = ['ga:users', {"expression": 'ga:session', 'alias': 'Session'}]
    parsed = GoogleAnalyticsAPI._parse_fields(metrics, style='metrics')
    assert parsed == [
        {'expression': 'ga:users'},
        {"expression": 'ga:session', 'alias': 'Session'}
    ]

    metrics = [{"expression": 'ga:session'}]
    parsed = GoogleAnalyticsAPI._parse_fields(metrics, style='metrics')
    assert parsed == metrics

    metrics = [{"expression": 'ga:session', 'alias': 'Session'}]
    parsed = GoogleAnalyticsAPI._parse_fields(metrics, style='metrics')
    assert parsed == metrics


def test_parse_dimensions():
    dimensions = ['ga:userType']
    parsed = GoogleAnalyticsAPI._parse_fields(dimensions, style='dimensions')
    assert parsed == [{'name': 'ga:userType'}]

    dimensions = ['ga:userType', 'ga:date']
    parsed = GoogleAnalyticsAPI._parse_fields(dimensions, style='dimensions')
    assert parsed == [{'name': 'ga:userType'}, {'name': 'ga:date'}]

    dimensions = ['ga:userType', {'name': 'ga:date'}]
    parsed = GoogleAnalyticsAPI._parse_fields(dimensions, style='dimensions')
    assert parsed == [{'name': 'ga:userType'}, {'name': 'ga:date'}]

    dimensions = [{'name': 'ga:date'}]
    parsed = GoogleAnalyticsAPI._parse_fields(dimensions, style='dimensions')
    assert parsed == dimensions
