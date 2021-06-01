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


NUM_THREADS = 1
MAX_QUEUE_SIZE = 256
INPUT_DIR = "input/stages"
OUTPUT_DIR = "output"
DONE_FILE = "done.txt"
DOWNLOAD_HEADLESS = True


timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
q = queue.Queue(maxsize=MAX_QUEUE_SIZE)
format_str = "{:0" + str(len(str(NUM_THREADS - 1))) + "d}"


def worker(tid):
    prev_status_code = 0
    tid_str = format_str.format(tid)
    os.makedirs(OUTPUT_DIR + "/" + timestamp + "/" + tid_str)
    log_path = OUTPUT_DIR + "/" + timestamp + "/log." + tid_str + ".csv"
    with open(log_path, "w", newline="") as log_file:
        log_writer = csv.writer(log_file)
        index = 0
        while True:
            url = q.get().strip()
            status_code = download(tid_str, index, url, log_writer)
            if status_code == prev_status_code and status_code == 1:
                print("sequential timeout", tid)
            prev_status_code = status_code
            log_file.flush()
            q.task_done()
            index += 1


def download(tid_str, index, url, log_writer):
    if DOWNLOAD_HEADLESS:
        status_code, content, write_directive = downloader.download_headless(url, int(tid_str))
    else:
        status_code, content, write_directive = downloader.download_simple(url)

    log_writer.writerow([index, status_code, url])
    if content is not None:
        with open(OUTPUT_DIR + "/" + timestamp + "/" + tid_str + "/" + str(index), write_directive) as output_file:
            output_file.write(content)

    return status_code


# Prepare output directory
output_directory = OUTPUT_DIR + "/" + timestamp
os.makedirs(output_directory)

# Prepare headless browsers
# if DOWNLOAD_HEADLESS:
#     for tid in range(NUM_THREADS):
#         tid_str = format_str.format(tid)
#         downloader.get_browser(tid_str)

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

# Close all the browsers (clean up resources)
downloader.close_browsers()
