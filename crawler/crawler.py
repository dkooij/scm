"""
Web Crawler.
Author: Daan Kooij
Last modified: June 3rd, 2021
"""

import csv
from datetime import datetime
import downloader
import os
import queue
import threading
import time


NUM_THREADS = 8
MAX_QUEUE_SIZE = 256
INPUT_DIR = "input/stages"
OUTPUT_DIR = "output"
DONE_FILE = "done.txt"
DOWNLOAD_HEADLESS = True


timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
q = queue.Queue(maxsize=MAX_QUEUE_SIZE)
format_str = "{:0" + str(len(str(NUM_THREADS - 1))) + "d}"


def worker(tid_int):
    tid = format_str.format(tid_int)
    os.makedirs(OUTPUT_DIR + "/" + timestamp + "/" + tid)
    log_path = OUTPUT_DIR + "/" + timestamp + "/log." + tid + ".csv"
    with open(log_path, "w", newline="") as log_file:
        log_writer = csv.writer(log_file)
        index = 0
        while True:
            url = q.get().strip()
            download(tid, index, url, log_writer)
            log_file.flush()
            q.task_done()
            index += 1


def download(tid, index, url, log_writer):
    dl_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if DOWNLOAD_HEADLESS:
        status_code, content, write_directive = downloader.download_headless(url, tid)
    else:
        status_code, content, write_directive = downloader.download_simple(url)

    log_writer.writerow([index, status_code, dl_timestamp, url])
    if content is not None:
        with open(OUTPUT_DIR + "/" + timestamp + "/" + tid + "/" + str(index), write_directive) as output_file:
            output_file.write(content)


# Prepare output directory
output_directory = OUTPUT_DIR + "/" + timestamp
os.makedirs(output_directory)

# Start threads
for tid_int in range(NUM_THREADS):
    threading.Thread(target=worker, args=(tid_int,), daemon=True).start()

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

# Close all the browsers (clean up resources)
downloader.close_browsers()
