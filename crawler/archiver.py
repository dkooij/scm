"""
Crawl result archiver.
Moves crawl results from local file system to DFS.
Author: Daan Kooij
Last modified: July 27th, 2021
"""

import os


KEYTAB_FILE = "/home/s1839047/keytabs/username.keytab"
KEYTAB_USER = "s1839047@AD.UTWENTE.NL"
CRAWL_DIR = "output"
CRAWL_BACKLOG_DIR = "output_backlog"
DFS_DIR = "/user/s1839047/crawls"
DONE_FILE = "done.txt"


os.system("kinit -kt " + KEYTAB_FILE + " " + KEYTAB_USER)
for crawl_name in os.listdir(CRAWL_DIR):
    crawl_path = CRAWL_DIR + "/" + crawl_name
    if os.path.isdir(crawl_path) and os.path.exists(crawl_path + "/" + DONE_FILE):
        os.system("hdfs dfs -moveFromLocal " + crawl_path + " " + DFS_DIR + "/" + crawl_name)

    # If the move to HDFS was unsuccessful, move the completed crawl to the crawl backlog
    if os.path.isdir(crawl_path) and os.path.exists(crawl_path + "/" + DONE_FILE):
        crawl_backlog_path = CRAWL_BACKLOG_DIR + "/" + crawl_name
        os.system("mv " + crawl_path + " " + crawl_backlog_path)
