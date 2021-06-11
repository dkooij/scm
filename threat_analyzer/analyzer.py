"""
Analyze crawler logs for threats.
Author: Daan Kooij
Last modified: June 11th, 2021
"""

import csv
from datetime import datetime, timedelta
import os
import pandas as pd


LOGS_PATH = "input/logs"
THREATS_PATH = "input/evidence-1.xlsx"
OUTPUT_PATH = "output/flagged_urls.txt"
THREAT_RANGE = 10


threat_timestamps = set()
flagged_urls = []

df = pd.read_excel(THREATS_PATH)
timestamp_column = df["Occured on"]
for timestamp_str in timestamp_column:
    timestamp = datetime.strptime(timestamp_str, "%d-%m-%Y %H:%M:%S")
    for delta in range(0, THREAT_RANGE + 1):
        threat_timestamps.add(timestamp + timedelta(seconds=delta))

for filename in os.listdir(LOGS_PATH):
    filepath = LOGS_PATH + "/" + filename
    with open(filepath) as log_file:
        log_reader = csv.reader(log_file)
        next(log_reader, None)  # Skip header
        for row in log_reader:
            timestamp_str, url = row[3], row[5]
            timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
            if timestamp in threat_timestamps:
                flagged_urls.append(url)

with open(OUTPUT_PATH, "w") as output_file:
    for url in flagged_urls:
        output_file.write(url + "\n")
