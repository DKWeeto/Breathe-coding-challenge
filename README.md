# Breathe coding challenge - Dana Weetman

## Outputs
* Outputs will be found in a directory named after the device_id entered.
* The `data` folder contains a data dump of all relevant info, sorted per day
* The `reports` folder contains two files: one for daily statistics - `daily_stats`, and one for times pollution exceeded the danger threshold - `dangers`.

## Getting Started

### Installing

* Install requirements:
```
pip install -r requirements.txt
```
* Run the program from the `/pipeline` directory

### Executing program

* Use the `pipeline` function in `main.py` using the device_id(s) needed
* The `pipeline` runs for a single device
* Modify under `if __name__ == "__main__":` as suited or run an example pipeline with:
```
python3 main.py
```

##### OR

* Run the pipeline for a specific device from the command line:
```
python3 main.py -d <device_id>
```

## (Given instructions)

* We would like you to implement a small program to read live data from the public API PM2.5 Open Data Portal , perform some simple analysis on it, and generate a report.
* Your program should read the data for a device using the /device/<device_id>/history/ endpoint
* Save the data into local persistent storage (what solution you use is up to you)
* detect periods where the PM2.5 level goes above the threshold of 30
* output a report containing:
  * a list of times when the level when above the danger threshold
  * the daily maximum, daily minimum, and daily average pollution value
* If new data becomes available from the API, your solution should insert any new data into the local storage, maintaining any data that is already there.
* Please deliver the source code in a git repository (it is fine to deliver it in a zip archive).
* We should be able to run your code without any changes, and see the results.
* Your solution should be to the quality you would expect for a production application, inasmuch as the time allows.