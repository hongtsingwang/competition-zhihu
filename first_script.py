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
from operator import add
reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    level=logging.INFO,
    datefmt='%a, %d %b %Y %H:%M:%S'
)

conf = SparkConf().setAppName("zhihu first predict").setMaster("yarn-client")
sc = SparkContext(conf=conf)
hadoop_base_dir = "/data/rec/wanghongqing/competition-zhihu"
original_data_dir = os.path.join(hadoop_base_dir, "original_data")
transform_dir = os.path.join(hadoop_base_dir, "transform_dir")

question_topic_train_set_file = os.path.join(original_data_dir, "question_topic_train_set.txt")
question_topic_train = sc.textFile(question_topic_train_set_file,minPartitions=10)
question_word_train_file = os.path.join(original_data_dir, "question_train_set.txt")
question_word_train = sc.textFile(question_word_train_file,minPartitions=10)

def get_question_topic_map(row):
    row_list = row.split("\t")
    question_id = row_list[0]
    topic_list_str = row_list[1]
    topic_list_set = set(topic_list_str.split(","))
    return  question_id, topic_list_set


def get_question_word_map(row):
    """
    
    :param row: 
    :return: 
    """
    row_list = row.split("\t")
    question_id = row_list[0]
    word_list_set = set(row_list[2].split(","))
    return question_id, word_list_set

def get_word_topic_count(row):
    question_id = row[0]
    word_set = row[1][0]
    topic_set = row[1][1]
    result_list = []
    for word in word_set:
        for topic in topic_set:
            word_topic = (word, topic)
            result_list.append(word_topic)
    return result_list
        

question_topic_map = question_topic_train.map(get_question_topic_map)
question_word_map = question_word_train.map(get_question_word_map)

word_topic = question_word_map.join(question_topic_map, numPartitions=10)
#word_topic_count = word_topic.flatMap(get_word_topic_count).flatMap(lambda x: (x,1)).reduceByKey(add)
word_topic_count = word_topic.flatMap(get_word_topic_count).map(lambda x: (x,1)).reduceByKey(add)
print word_topic_count.take(5)
# result_path = os.path.join(transform_dir, "word_topic_count.txt")
# print word_topic_count.first()
# word_topic_count.saveAsTextFile(result_path)
# 
# # debug 语句
# print "line number of the input file is %d" % (question_topic_train.count())

sc.stop()
