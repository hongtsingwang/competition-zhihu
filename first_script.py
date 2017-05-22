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
result_dir = os.path.join(hadoop_base_dir, "result_dir")

question_topic_train_set_file = os.path.join(original_data_dir, "question_topic_train_set.txt")
question_topic_train = sc.textFile(question_topic_train_set_file, minPartitions=10)
question_word_train_file = os.path.join(original_data_dir, "question_train_set.txt")
question_word_train = sc.textFile(question_word_train_file, minPartitions=10)

question_word_eval_file = os.path.join(original_data_dir, "question_eval_set.txt")
question_word_eval = sc.textFile(question_word_eval_file, minPartitions=10)


def get_question_topic_map(row):
    row_list = row.split("\t")
    question_id = row_list[0]
    topic_list_str = row_list[1]
    topic_list_set = set(topic_list_str.split(","))
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


# qestion 和 topic的对应关系获得
question_topic_map = question_topic_train.map(get_question_topic_map)
# question 和 Word 的对应关系
question_word_map = question_word_train.map(get_question_word_map)

word_topic = question_word_map.join(question_topic_map, numPartitions=10)
# 获取Word 和 topic的共现关系
word_topic_count = word_topic.flatMap(get_word_topic_count).map(lambda x: (x, 1)).reduceByKey(add).filter(
    lambda x: x[1] != 1)
word_top_topic = word_topic_count.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(list).collectAsMap()


# word_topic_count_output = word_topic_count.map(lambda x: "\t".join([x[0][0], x[0][1], str(x[1])]))


# 计算每个词的IDF情况
def get_word_count(row):
    question_id = row[0]
    word_list_set = row[1]
    return word_list_set


word_count = question_word_map.flatMap(get_word_count).map(lambda x: (x, 1))
word_count = word_count.reduceByKey(add).collectAsMap()
# word_count = sc.broadcast(word_count)
word_count_path = os.path.join(transform_dir, "word_count.txt")

word_topic_count_path = os.path.join(transform_dir, "word_topic_count.txt")


# word_topic_count_output.saveAsTextFile(word_topic_count_path)

def get_result(row):
    row_list = row.strip().split("\t")
    question_id = row_list[0]
    word_list = row_list[2].strip().split(",")
    tmp_dict = dict()
    for word in word_list:
        word_frequency = word_count.get(word, 0)
        tmp_dict[word] = word_frequency
    tmp_list = sorted(tmp_dict.items(), key=lambda x: x[1])[:5]
    result_list = []
    for item in tmp_list:
        word_name = item[0]
        topic_list = word_top_topic.get(word_name, [])
        sorted_topic_list = sorted(topic_list, key=lambda x: x[1], reverse=True)
        for item in sorted_topic_list[:3]:
            if item not in result_list:
                result_list.append(item[0])
    return question_id + "," + ",".join(result_list[:5])


result = question_word_eval.map(get_result)
validation_file = os.path.join(result_dir, "result")
result.saveAsTextFile(validation_file)

sc.stop()
