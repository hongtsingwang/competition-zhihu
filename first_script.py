# -*- coding:utf-8 -*-
# =======================================================
# 
# @FileName  : first_script.py
# @Author    : Wang Hongqing
# @Date      : 2017-05-16 18:12
# 
# =======================================================

import os
import sys
import logging
from pyspark import SparkContext

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    level=logging.DEBUG,
    datefmt='%a, %d %b %Y %H:%M:%S'
)

sc = SparkContext("yarn-client", "zhihu first predict")

hadoop_base_dir = "/data/rec/wanghongqing/competition-zhihu"
original_data_dir = os.path.join(hadoop_base_dir, "original_data")
assert os.path.exists(original_data_dir)
print "234"

sc.stop()
