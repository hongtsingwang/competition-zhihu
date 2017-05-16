#!/bin/bash

hadoop fs -rm -r -skipTrash  hdfs://letvbg-cluster/data/rec/wanghongqing/competition-zhihu/transform_dir/word_topic_count.txt
spark-submit \
   first_script.py 
