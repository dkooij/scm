"""
Crawl result archiver.
Moves crawl results from local file system to DFS.
Author: Daan Kooij
Last modified: June 4th, 2021
"""

import os


KEYTAB_FILE = "/home/s1839047/keytabs/username.keytab"
KEYTAB_USER = "s1839047@AD.UTWENTE.NL"
CRAWL_DIR = "output"
DFS_DIR = "/user/s1839047/crawls"
DONE_FILE = "done.txt"


os.system("kinit -kt " + KEYTAB_FILE + " " + KEYTAB_USER)
for crawl_name in os.listdir(CRAWL_DIR):
    crawl_path = CRAWL_DIR + "/" + crawl_name
    if os.path.isdir(crawl_path):
        if os.path.exists(crawl_path + "/" + DONE_FILE):
            os.system("hdfs dfs -moveFromLocal " + crawl_path + " " + DFS_DIR + "/" + crawl_name)
