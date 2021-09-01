"""
Fetches crawl result of a single day from HDFS,
and store as .zip archive on local file system.
Author: Daan Kooij
Last modified: September 1st, 2021
"""

from datetime import datetime
import os
import sys


KEYTAB_FILE = "/home/s1839047/keytabs/username.keytab"
KEYTAB_USER = "s1839047@AD.UTWENTE.NL"
DFS_DIR = "/user/s1839047/crawls"
LOCAL_DESTINATION = "/home/s1839047/crawl-downloads"


def get_timestamp():
    return datetime.now().strftime("%H:%M:%S") + " - "


if len(sys.argv) < 2:
    print("USAGE: python3 fetcher.py <source folders>")
    exit()

source_folders = sys.argv[-1].split(",")

for source_folder in source_folders:
    print()
    dfs_location = DFS_DIR + "/" + source_folder
    destination_folder = source_folder[:8]
    destination = LOCAL_DESTINATION + "/" + destination_folder

    os.system("kinit -kt " + KEYTAB_FILE + " " + KEYTAB_USER)
    print(get_timestamp() + "Copying " + source_folder + " to local file system...")
    os.system("hdfs dfs -copyToLocal " + dfs_location + " " + destination)
    print(get_timestamp() + "Compressing " + destination_folder + "...")
    os.system("cd " + LOCAL_DESTINATION + " && zip -r -q " + destination_folder + ".zip " + destination_folder)
    print(get_timestamp() + "Removing " + destination_folder + " uncompressed data...")
    os.system("rm -r " + destination)

print("\nDone!\n")
