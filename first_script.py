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
from pyspark import SparkConf
from collections import defaultdict

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    level=logging.INFO,
    datefmt='%a, %d %b %Y %H:%M:%S'
)

conf = SparkConf().setAppName("zhihu first predict").setMaster("yarn-client")
sc = SparkContext(conf)

hadoop_base_dir = "/data/rec/wanghongqing/competition-zhihu"
original_data_dir = os.path.join(hadoop_base_dir, "original_data")
transform_dir = os.path.join(hadoop_base_dir, "transform_dir")

question_topic_train_set_file = os.path.join(original_data_dir, "question_topic_train_set.txt")
question_topic_train = sc.textFile(question_topic_train_set_file)
question_word_train_file = os.path.join(original_data_dir, "question_train_set.txt")
question_word_train = sc.textFile(question_word_train_file)

question_word = sc.textFile()


def get_question_topic_map(row):
    row_list = row.split("\t")
    question_id = row_list[0]
    topic_list_str = row_list[1]
    topic_list_set = topic_list_str.split(",")
    return question_id, topic_list_set


def get_question_word_map(row):
    """
    
    :param row: 
    :return: 
    """
    row_list = row.split("\t")
    question_id = row_list[0]
    word_list_set = set(row_list[2].split(","))
    return question_id, word_list_set


question_topic_map = question_topic_train.map(get_question_topic_map).collectAsMap()
question_word_map = question_word_train.map(get_question_word_map()).collectAsMap()

word_topic_count_dict = defaultdict(dict)

for item in question_topic_map:
    topic_set = question_topic_map[item]
    word_set = question_word_map[item]
    for topic in topic_set:
        for word in word_set:
            word_topic_count_dict[word] = word_topic_count_dict[word].get(topic, 0) + 1

word_topic_count = tuple()
for word in word_topic_count_dict:
    for topic in word_topic_count_dict[word]:
        word_topic_count = [word, topic, word_topic_count_dict[word][topic]]

result_path = os.path.join(transform_dir, "word_topic_count.txt")
result_file = sc.parallelize(word_topic_count)
result_file.saveAsTextFile(result_path)

# debug 语句
print "line number of the input file is %d" % (question_topic_train.count())

sc.stop()
