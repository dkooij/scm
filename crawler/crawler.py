"""
Web Crawler.
Author: Daan Kooij
Last modified: June 1st, 2021
"""

import csv
from datetime import datetime
import downloader
import os
import queue
import threading
import time


NUM_THREADS = 16
MAX_QUEUE_SIZE = 256
INPUT_DIR = "input/stages"
OUTPUT_DIR = "output"
DONE_FILE = "done.txt"


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
    status_code, content = downloader.download_simple(url)

    log_writer.writerow([index, status_code, url])
    if content is not None:
        with open(OUTPUT_DIR + "/" + timestamp + "/" + tid_str + "/" + str(index), "wb") as output_file:
            output_file.write(content)


# Prepare output directory
output_directory = OUTPUT_DIR + "/" + timestamp
os.makedirs(output_directory)

# Start threads
for tid in range(NUM_THREADS):
    threading.Thread(target=worker, args=(tid,), daemon=True).start()

# Add crawl jobs to queue, pause between stages
for stage_filename in os.listdir(INPUT_DIR):
    stage_path = INPUT_DIR + "/" + stage_filename
    with open(stage_path) as stage_file:
        for url in stage_file:
            q.put(url)
    q.join()
    time.sleep(1)

# Indicate that crawl has finished
with open(OUTPUT_DIR + "/" + timestamp + "/" + DONE_FILE, "w") as done_file:
    done_file.write(datetime.now().strftime("%Y%m%d%H%M%S"))
