"""
Web Crawler.
Author: Daan Kooij
Last modified: May 18th, 2021
"""

import csv
from datetime import datetime
import os
import queue
import requests
import threading
import time


NUM_THREADS = 16
MAX_QUEUE_SIZE = 256
INPUT_DIR = "input/stages"
OUTPUT_DIR = "output"


timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
q = queue.Queue(maxsize=MAX_QUEUE_SIZE)
format_str = "{:0" + str(len(str(NUM_THREADS - 1))) + "d}"


def worker(tid):
    tid_str = format_str.format(tid)
    os.makedirs(OUTPUT_DIR + "/" + timestamp + "/" + tid_str)
    log_path = OUTPUT_DIR + "/" + timestamp + "/log." + tid_str + ".csv"
    with open(log_path, "w", newline="") as log_file:
        log_writer = csv.writer(log_file)
        index = 0
        while True:
            url = q.get().strip()
            download(tid_str, index, url, log_writer)
            log_file.flush()
            q.task_done()
            index += 1


def download(tid_str, index, url, log_writer):
    status_code = -1
    try:
        response = requests.get(url, allow_redirects=True)
        status_code = response.status_code
        with open(OUTPUT_DIR + "/" + timestamp + "/" + tid_str + "/" + str(index), "wb") as output_file:
            output_file.write(response.content)
    except requests.RequestException:
        pass
    log_writer.writerow([index, status_code, url])


# Prepare output directory
output_directory = OUTPUT_DIR + "/" + timestamp
os.makedirs(output_directory)

# Start threads
for tid in range(NUM_THREADS):
    threading.Thread(target=worker, args=(tid,), daemon=True).start()


for stage_filename in os.listdir(INPUT_DIR):
    stage_path = INPUT_DIR + "/" + stage_filename
    print("START STAGE", stage_path)
    with open(stage_path) as stage_file:
        for url in stage_file:
            q.put(url)
    q.join()
    print("END STAGE", stage_path)
    time.sleep(1)

with open(OUTPUT_DIR + "/" + timestamp + "/done.txt", "w") as done_file:
    done_file.write(datetime.now().strftime("%Y%m%d%H%M%S"))
