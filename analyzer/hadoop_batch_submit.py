"""
Submit a batch of crawl processing jobs.
Author: Daan Kooij
Last modified: November 24th, 2021
"""

import os


KEYTAB_FILE = "/home/s1839047/keytabs/username.keytab"
KEYTAB_USER = "s1839047@AD.UTWENTE.NL"


crawl_dirs = ["20210612000004", "20210613000001", "20210614000002", "20210615000002", "20210616000003",
              "20210617000002", "20210618000003", "20210619000003", "20210620000004", "20210621000004",
              "20210622000003", "20210623000004", "20210624000003", "20210625000003", "20210626000003",
              "20210627000004", "20210628000003", "20210629000002", "20210630000002", "20210701000004",
              "20210702000003", "20210703000003", "20210704000002", "20210705000003", "20210706000003",
              "20210707000004", "20210708000003", "20210709000003", "20210710000002", "20210711000004",
              "20210712000003", "20210713000005", "20210714000003", "20210715000003", "20210716000003",
              "20210717000003", "20210718000001", "20210719000003", "20210720000002", "20210721000002",
              "20210722000003", "20210723000001", "20210724000004", "20210725000002", "20210726000002",
              "20210727000003", "20210728000003", "20210729000005", "20210730000003", "20210731000003",
              "20210801000002", "20210802000002", "20210803000004", "20210804000003", "20210805000004",
              "20210806000005", "20210807000003", "20210808000003", "20210809000002", "20210810000003",
              "20210811000003", "20210812000004", "20210813000004", "20210814000004", "20210815000003",
              "20210816000002", "20210817000005", "20210818000004", "20210819000004", "20210820000003",
              "20210821000007", "20210822000003", "20210823000003", "20210824000003", "20210825000004",
              "20210826000003", "20210827000003", "20210828000003", "20210829000004", "20210830000003",
              "20210831000004", "20210901000010", "20210902000008", "20210903000003", "20210904000003",
              "20210905000003", "20210906000002", "20210907000006", "20210908000002", "20210909000003"]

# for dir1, dir2 in zip(crawl_dirs, crawl_dirs[1:]):
#     os.system("kinit -kt " + KEYTAB_FILE + " " + KEYTAB_USER)
#     os.system("cd /home/s1839047/SCM/analyzer && "
#               "time spark-submit \
#                --master yarn \
#                --deploy-mode cluster \
#                --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
#                --conf spark.dynamicAllocation.maxExecutors=100 \
#                --executor-cores 4 \
#                --executor-memory 7G \
#                --archives /home/s1839047/pyspark-envs/v3.tar.gz#environment \
#                --py-files get_page_text.py,text_overlap.py \
#                /home/s1839047/SCM/analyzer/hadoop_process_crawls.py " + dir1 + " " + dir2)

for directory in crawl_dirs:
    os.system("kinit -kt " + KEYTAB_FILE + " " + KEYTAB_USER)
    os.system("cd /home/s1839047/SCM/analyzer && "
              "time spark-submit \
               --master yarn \
               --deploy-mode cluster \
               --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
               --conf spark.dynamicAllocation.maxExecutors=100 \
               --executor-cores 4 \
               --executor-memory 7G \
               --archives /home/s1839047/pyspark-envs/v3.tar.gz#environment \
               --py-files get_page_text.py,text_overlap.py \
               /home/s1839047/SCM/analyzer/hadoop_compress.py " + directory)

print("\nDone!\n")
