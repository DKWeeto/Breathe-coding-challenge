"""Transform and clean JSON data into pandas DataFrames."""

from datetime import datetime
import pandas as pd


def transform_record(record: dict) -> tuple:
    """Transform datatypes for a time, date and pm2.5 record."""
    record_data = list(record.values())[0]
    pm25 = float(record_data['s_d0'])
    timestamp = datetime.strptime(
        record_data['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
    return timestamp.date(), timestamp.time(), pm25


def transform_all_records(feeds) -> pd.DataFrame:
    """Transform and clean all data into a pandas DataFrame"""
    all_records = []
    if isinstance(feeds, dict):
        feeds = [feeds]
    for feed in feeds:
        project_name = list(feed.keys())[0]
        for record in feed[project_name]:
            transformed_data = {}
            transformed_data["date"], transformed_data["time"], transformed_data["pm25"] = transform_record(
                record)
            all_records.append(transformed_data)
    return pd.DataFrame(all_records).drop_duplicates().dropna()
