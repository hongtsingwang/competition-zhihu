#!/usr/bin/env python
# coding=utf-8

import os
import sys
# from collections import Counter
import logging
import time

from itertools import product
reload(sys)
sys.setdefaultencoding("utf-8")


"""
统计每个字在训练样本之中的作用
为什么要统计这个？
    1. 有些出现字可能是无意义的字， 先统计一下，看看词的频率的出现分布情况
    2. 其他可能有用的地方
"""

home_dir = os.getcwd()
data_dir = os.path.join(home_dir, "data")

logging.basicConfig(
            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            level=logging.DEBUG,
            datefmt='%a, %d %b %Y %H:%M:%S'
        )   


"""
c1395   16066
c6340   9
"""

"""
缺少对结果正确性的验证
"""

train_input_file = os.path.join(data_dir, "question_train_set.txt")
predict_input_file = os.path.join(data_dir, "question_eval_set.txt")

char_counts = dict()
word_counts = dict()
char_co_occurrence = dict()


start_time = time.time()

# TODO 此函数还未写完
def file_stat(file_name):
    """
        input: train_input_file 或者 predict_input_file两个文件。
    """
    pass
for index, line in enumerate(open(train_input_file)):
    if index % 10000 == 1:
        logging.info("%d lines has processed!" % index )
    line_list = line.strip("\n").split("\t")
    sample_id = line_list[0]
    char_list = line_list[1].split(",")
    word_list = line_list[2]
    description_char_list = line_list[3]
    description_word_list = line_list[4]
    for single_char in char_list:
        char_counts[single_char] = char_counts.get(single_char, 0) + 1
#     for char1, char2 in product(char_list.split(","), char_list.split(",")):
#         if char1 == char2:
#             continue
#         if char1 < char2:
#             char1, char2 = char2, char1
#         char_co_occurrence[(char1, char2)] = char_co_occurrence.get((char1, char2), 0) + 1
    for i in range(len(char_list) - 1):
        char1 = char_list[i]
        char2 = char_list[i + 1]
        char_co_occurrence[(char1, char2)] = char_co_occurrence.get((char1, char2), 0) + 1
    
    for i in range(len(char_list) - 2):
        char1 = char_list[i]
        char2 = char_list[i+1]
        char3 = char_list[i+2]
        char_co_occurrence[(char1, char2, char3)] = char_co_occurrence.get((char1, char2, char3), 0) + 1

    for single_char in word_list.split(","):
        word_counts[single_char] = word_counts.get(single_char, 0) + 1

sorted_char_counts = sorted(char_counts.items(), key=lambda x:x[1], reverse = True)
sorted_word_counts = sorted(word_counts.items(), key=lambda x:x[1], reverse = True)
# sorted_char_co_occurrence = sorted(char_co_occurrence.items(), key=lambda x:x[1], reverse = True)

output_char_stat_file = open("stat_result/char_list.txt", "w")
output_word_stat_file = open("stat_result/word_list.txt", "w")
output_char_cocurrence_file = open("stat_result/char_cocurrence.txt", "w")


logging.info("start output sorted_char_counts")
for key, value in sorted_char_counts:
    output_char_stat_file.write("\t".join([key, str(value)]))
    output_char_stat_file.write("\n")

logging.info("start output sorted_word_counts")
for key, value in sorted_word_counts:
    output_word_stat_file.write("\t".join([key, str(value)]))
    output_word_stat_file.write("\n")

# TODO 过滤掉只共现一次的两个字
logging.info("start output output_char_cocurrence_file")
for key, value in char_co_occurrence.iteritems():
    if value == 1:
        continue
    output_char_cocurrence_file.write("\t".join(list(key) + [str(value)]))
    output_char_cocurrence_file.write("\n")

output_char_stat_file.close()
output_word_stat_file.close()
output_char_cocurrence_file.close()


end_time = time.time()
logging.info("this program has runned for %.f seconds" % (end_time - start_time))
