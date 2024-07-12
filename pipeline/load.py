"""Load pandas DataFrames into relevant csv files, locally."""

import pandas as pd
import os


def create_csv_dump(device_id: str, data: pd.DataFrame) -> None:
    """Create a data directory and csv files for all the given data."""
    try:
        os.mkdir(f'{device_id}/data')
    except FileExistsError:
        pass
    data_days = [(i, df) for i, df in data.groupby('date')]
    for date in data_days:
        file_name = f'{device_id}/data/{date[0]}'
        if os.path.isfile(file_name):
            df = pd.concat([pd.read_csv(file_name), date[1]]).drop_duplicates()
        else:
            df = date[1]
        df.to_csv(file_name, index=False)


def record_daily_stats(device_id: str, data: pd.DataFrame) -> None:
    """Create a report for each date in the data, modifying any existing report file."""
    try:
        os.mkdir(f'{device_id}/reports')
    except FileExistsError:
        pass
    stats = data[['date', 'pm25']].groupby(
        'date').agg(['min', 'max', 'mean']).reset_index()
    stats.columns = stats.columns.droplevel(0)
    stats = stats.rename(columns={"": "date"})
    file_name = f'{device_id}/reports/daily_stats'
    if os.path.isfile(file_name):
        stats = pd.concat([pd.read_csv(file_name), stats]).drop_duplicates()
        stats = data[['date', 'pm25']].groupby(
            'date').agg(['min', 'max', 'mean']).reset_index()
        stats.columns = stats.columns.droplevel(0)
        stats = stats.rename(columns={"": "date"})
    stats.to_csv(file_name, index=False)


def record_dangerous_levels(device_id: str, data: pd.DataFrame, threshold: float = 30) -> None:
    """Create a report of dangerous pollution times, modifying any existing report file."""
    try:
        os.mkdir(f'{device_id}/reports')
    except FileExistsError:
        pass
    dangers = data[data['pm25'] > threshold]
    file_name = f'{device_id}/reports/dangers'
    if os.path.isfile(file_name):
        df = pd.concat([pd.read_csv(file_name), dangers]).drop_duplicates()
    else:
        df = dangers
    df.to_csv(file_name, index=False)


def load_data(device_id: str, data: pd.DataFrame, danger_threshold: float = 30) -> None:
    """Load all the data into files, locally."""
    try:
        os.mkdir(device_id)
    except FileExistsError:
        pass
    create_csv_dump(device_id, data)
    record_daily_stats(device_id, data)
    record_dangerous_levels(device_id, data, danger_threshold)
