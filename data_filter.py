# -*- coding:utf-8 -*-
# =======================================================
# 
# @FileName  : suibain.py
# @Author    : Wang Hongqing
# @Date      : 2017-06-20 20:03
# 
# =======================================================

import os
import sys
import argparse
import logging
import time

reload(sys)
sys.setdefaultencoding('utf-8')

# parser = argparse.ArgumentParser()
# parser.add_argument()
# args = parser.parse_args()

# output = args.output

start_time = time.time()

logging.basicConfig(
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    level=logging.DEBUG,
    datefmt='%a, %d %b %Y %H:%M:%S'
)

home_dir = os.getcwd()
data_dir = os.path.join(home_dir, "data")
transform_dir = os.path.join(home_dir,"transform_data")

train_input_file = os.path.join(data_dir, "question_train_set.txt")
predict_input_file = os.path.join(data_dir, "question_eval_set.txt")

train_transorm_file = os.path.join(transform_dir, "question_train_filter.txt")
predict_transorm_file = os.path.join(transform_dir, "question_predict_filter.txt")

def add_filter(input_file_name, target_set):
    """
    获得过滤器
    :param input_file_name
    :param target_set: 
    :return: 
    """
    for line in open(input_file_name):
        line_list = line.strip().split("\t")
        target_id = line_list[0]
        target_set.add(target_id)
    return target_set


filter_char_set = set()
filter_word_set = set()
filter_char_set = add_filter("char_filter_file", filter_char_set)
filter_word_set = add_filter("word_filter_file", filter_word_set)


def process_file(input_file, output_file_name):
    """
    
    :param input_file: 
    :param output_file: 
    :return: 
    """
    output_file = open(output_file_name, mode="w", buffering=0)
    for index, line in enumerate(open(input_file)):
        if index % 10000 == 1:
            logging.info("%d lines has processed!" % index)
        line_list = line.strip("\n").split("\t")
        sample_id = line_list[0]
        char_list = (line_list[1]).split(",")
        word_list = (line_list[2]).split(",")
        description_char_list = (line_list[3]).split(",")
        description_word_list = (line_list[4]).split(",")

        new_char_list = filter(lambda x: x not in filter_char_set, char_list)
        new_word_list = filter(lambda x: x not in filter_word_set, word_list)
        new_description_char_list = filter(lambda x: x not in filter_char_set, description_char_list)
        new_description_word_list = filter(lambda x: x not in filter_word_set, description_word_list)

        output_file.write("\t".join(
            [sample_id, ",".join(new_char_list), ",".join(new_word_list), ",".join(new_description_char_list),
             ",".join(new_description_word_list)]))
        output_file.write("\n")
    output_file.close()

process_file(train_input_file, train_transorm_file)
process_file(predict_input_file, predict_transorm_file)

end_time = time.time()

logging.info("this program has runned for %.f seconds" % (end_time - start_time))
