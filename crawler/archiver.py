import os


CRAWL_DIR = "output"
DFS_DIR = "/user/s1839047/crawls"
DONE_FILE = "done.txt"


for crawl_name in os.listdir(CRAWL_DIR):
    crawl_path = CRAWL_DIR + "/" + crawl_name
    if os.path.isdir(crawl_path):
        if os.path.exists(crawl_path + "/" + DONE_FILE):
            os.system("hdfs dfs -moveFromLocal " + crawl_path + " " + DFS_DIR + "/" + crawl_name)
