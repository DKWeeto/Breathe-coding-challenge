"""Run an ETL pipeline for PM2.5 data for a single device, using the device_id."""

from argparse import ArgumentParser

from extract import get_history
from transform import transform_all_records
from load import load_data


def pipeline(device_id: str) -> None:
    """Run the pipeline for a single device"""
    load_data(device_id, transform_all_records(get_history(device_id)))


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-d", "--device_id", help="Optional: Give the id of a specific device to load data from.",
        type=str, required=False)
    args = parser.parse_args()

    if args.device_id:
        pipeline(args.device_id)
    else:
        pipeline('08BEAC07D3FE')  # Example
