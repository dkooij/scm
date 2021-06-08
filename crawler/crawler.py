"""
Web Crawler.
Author: Daan Kooij
Last modified: June 7th, 2021
"""

import csv
from datetime import datetime
import downloader
import os
import queue
import threading
import time

from crawler.request_status import RequestStatus

NUM_THREADS = 8
MAX_QUEUE_SIZE = 256
INPUT_DIR = "input/stages"
OUTPUT_DIR = "output"
DONE_FILE = "done.txt"
DOWNLOAD_HEADLESS = True


timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
q = queue.Queue(maxsize=MAX_QUEUE_SIZE)


def worker(tid_int):
    prev_status_code = None
    tid = str(tid_int)
    log_path = OUTPUT_DIR + "/" + timestamp + "/log-thread-" + tid + ".csv"
    with open(log_path, "w", newline="") as log_file:
        log_writer = csv.writer(log_file)
        while True:
            (stage_filename, line_index, url) = q.get()
            url = url.strip()
            status_code = download(tid, stage_filename, line_index, url, log_writer)
            if status_code == prev_status_code == RequestStatus.HEADLESS_ERROR:
                # For the (rare) event that there are consecutive headless errors:
                # Restart the browser instance (maybe it is broken)
                downloader.close_browser(tid)
            prev_status_code = status_code
            log_file.flush()
            q.task_done()


def download(tid, stage_filename, line_index, url, log_writer):
    dl_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if DOWNLOAD_HEADLESS:
        status_code, content, write_directive = downloader.download_headless(url, tid)
    else:
        status_code, content, write_directive = downloader.download_simple(url)

    log_writer.writerow([stage_filename, line_index, status_code, dl_timestamp, url])
    if content is not None:
        with open(OUTPUT_DIR + "/" + timestamp + "/pages/" + stage_filename + "-" + str(line_index),
                  write_directive) as output_file:
            output_file.write(content)

    return status_code


# Prepare output directory
output_directory = OUTPUT_DIR + "/" + timestamp + "/pages"
os.makedirs(output_directory)

# Start threads
for tid_int in range(NUM_THREADS):
    threading.Thread(target=worker, args=(tid_int,), daemon=True).start()

# Add crawl jobs to queue, pause between stages
for stage_filename in sorted(os.listdir(INPUT_DIR)):
    stage_path = INPUT_DIR + "/" + stage_filename
    line_index = 0
    with open(stage_path) as stage_file:
        for url in stage_file:
            line_index += 1
            q.put((stage_filename.split(".")[0], line_index, url))
    q.join()
    time.sleep(1)

# Indicate that crawl has finished
with open(OUTPUT_DIR + "/" + timestamp + "/" + DONE_FILE, "w") as done_file:
    done_file.write(datetime.now().strftime("%Y%m%d%H%M%S"))

# Close all the browsers (clean up resources)
downloader.close_browsers()
