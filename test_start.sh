#!/bin/bash

export SPARK_LOCAL_DIRS=/letv/home/rec/data/wanghongqing/competition/competition-zhihu/tmp
hadoop fs -rm -r -skipTrash  hdfs://letvbg-cluster/data/rec/wanghongqing/competition-zhihu/transform_dir/word_topic_count.txt
hadoop fs -rm -r -skipTrash  /data/rec/wanghongqing/competition-zhihu/result_dir/* 
spark-submit \
    --driver-memory 50G \
   first_script.py 
