# -*- coding:utf-8 -*-
# =======================================================
# 
# @FileName  : word_topic_analysis.py
# @Author    : Wang Hongqing
# @Date      : 2017-05-22 13:50
# 
# =======================================================

import os
import sys
import argparse
import logging
from collections import defaultdict
reload(sys)
sys.setdefaultencoding('utf-8')

# parser = argparse.ArgumentParser()
# parser.add_argument()
# args = parser.parse_args()

# output = args.output
logging.basicConfig(
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    level=logging.DEBUG,
    datefmt='%a, %d %b %Y %H:%M:%S'
)   


"""
分析Word 和 topic 的关系
"""

# 把Word 和 topic 整成 word topic1,count1 topic2,count2........ 的形式
# 输入形式 word\ttopic\tcount

input_file = open("word_topic_count", "r")
output_file = open("word_topic_analysis", "w")
result_dict = defaultdict(list)
for index, line in enumerate(input_file):
    line_list = line.strip().split("\t")
    if len(line_list) != 3:
        logging.info(line)
        continue
    if index % 100000 == 1:
        logging.info("%d lines" % index)
    word = line_list[0]
    topic = line_list[1]
    count = int(line_list[2])
    if count < 5:
        continue
    result_dict[word].append((topic, count))

for key, value in result_dict.iteritems():
    word_name = key
    sorted_topic_count_pair = sorted(value, key=lambda x:x[1], reverse=True)
    output_file.write(word_name + "\t" + "\t".join([x[0] + "," + str(x[1]) for x in sorted_topic_count_pair]))
    output_file.write("\n")


output_file.close()
