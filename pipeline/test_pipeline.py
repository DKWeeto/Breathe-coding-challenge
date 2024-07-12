"""Unit tests for the pipeline scripts."""

from datetime import date, time
import pandas as pd
import pytest

from extract import get_history
from transform import transform_record, transform_all_records


def test_get_request_ok(requests_mock):
    """Test get_history when the response is ok."""
    requests_mock.get("https://pm25.lass-net.org/API-1.0.0/device/08BEAC0A08AE/history/", json={
        "source": "history by IIS-NRL",
        "device_id": "08BEAC0A08AE",
        "version": "2020-07-01T13:30:50Z",
        "num_of_records": 1,
        "feeds": {
            "Proj_Name": [
                {
                    "Timestamp_value": {
                        "device_id": "08BEAC0A08AE",
                        "s_t0": 25,
                        "s_h0": 50,
                        "s_d0": 20,
                        "gps_lat": 24.251,
                        "gps_lon": 120.539,
                        "timestamp": "2020-07-01T13:30:50Z"
                    }
                }
            ]
        }
    })
    test_get_request = get_history('08BEAC0A08AE')
    assert isinstance(test_get_request, dict)
    assert test_get_request == {
        "Proj_Name": [
            {
                "Timestamp_value": {
                    "device_id": "08BEAC0A08AE",
                    "s_t0": 25,
                    "s_h0": 50,
                    "s_d0": 20,
                    "gps_lat": 24.251,
                    "gps_lon": 120.539,
                    "timestamp": "2020-07-01T13:30:50Z"
                }
            }
        ]
    }


def test_response_empty(requests_mock):
    """Test get_history when the response is has no records."""
    requests_mock.get("https://pm25.lass-net.org/API-1.0.0/device/08BEAC0A08AE/history/", json={
        "source": "history by IIS-NRL",
        "device_id": "08BEAC0A08AE",
        "version": "2020-07-01T13:30:50Z",
        "num_of_records": 0,
        "feeds": []
    })
    test_empty_request = get_history('08BEAC0A08AE')
    assert 'Error' in test_empty_request
    assert test_empty_request['Error'] == "No records for this device."


def test_get_request_bad(requests_mock):
    """Test get_history when the response is not ok."""
    requests_mock.get(
        "https://pm25.lass-net.org/API-1.0.0/device/08BEAC0A08AE/history/",
        status_code=404, json={})
    test_bad_request = get_history('08BEAC0A08AE')
    assert 'Error' in test_bad_request
    assert test_bad_request['Error'] == "Status code not ok."


def test_valid_record():
    """Test transform_record with valid record."""
    test_record = transform_record({'2024-07-12T13:32:04Z': {'time': '13:32:04', 'SiteAddr': 'N/A',
                                                             'SiteName': '新竹縣縣立花園國小竹林分校(2020)',
                                                             'app': 'AirBox',
                                                             'area': 'hsinchu_county',
                                                             'date': '2024-07-12',
                                                             'device_id': '08BEAC252B9A',
                                                             'fw_ver': 'v1.00', 'gps_alt': 2.0,
                                                             'gps_fix': 1.0, 'gps_lat': 24.6249909,
                                                             'gps_lon': 121.1483419, 'gps_num': 9.0,
                                                             'hcho': 0, 'model': 'AI-1004WN',
                                                             'name': '竹林Takunan', 's_d0': 6.0,
                                                             's_d1': 6.0, 's_d2': 5.0,
                                                             's_h0': 100.0, 's_t0': 25.6,
                                                             'timestamp': '2024-07-12T13:32:04Z'}})
    assert isinstance(test_record, tuple)
    assert test_record == (date(2024, 7, 12), time(13, 32, 4), 6.0)


def test_invalid_record():
    """Test errors raised in transform_record with invalid records."""
    with pytest.raises(IndexError):
        transform_record({})
    with pytest.raises(KeyError):
        transform_record({'Time': {'invalid': 'time'}})


def test_valid_records():
    """Test transform_all_record with valid record dict."""
    test_records = transform_all_records({
        "Proj_Name": [
            {
                "Timestamp_value": {
                    "device_id": "08BEAC0A08AE",
                    "s_t0": 25,
                    "s_h0": 50,
                    "s_d0": 20,
                    "gps_lat": 24.251,
                    "gps_lon": 120.539,
                    "timestamp": "2020-07-01T13:30:50Z"
                }
            }
        ]
    })
    assert isinstance(test_records, pd.DataFrame)
    assert test_records.to_dict(orient='split', index=False) == {'columns': [
        'date', 'time', 'pm25'], 'data': [[date(2020, 7, 1), time(13, 30, 50), 20.0]]}
