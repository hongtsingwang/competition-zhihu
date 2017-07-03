# -*- coding:utf-8 -*-
# =======================================================
# 
# @FileName  : topic_index.py
# @Author    : Wang Hongqing
# @Date      : 2017-06-11 16:55
# 
# =======================================================

import os
import sys
import argparse
import logging
import cPickle as pickle

reload(sys)
sys.setdefaultencoding('utf-8')

parser = argparse.ArgumentParser()
parser.add_argument("--topic_file",default="data/topic_info.txt", help="topic基本信息文件")
args = parser.parse_args()

# output = args.output

"""
这个脚本的功能是给所有topic一个编号， 并以pickle的形式存起来。方便后续使用
"""

logging.basicConfig(
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    level=logging.DEBUG,
    datefmt='%a, %d %b %Y %H:%M:%S'
)   

assert os.path.isfile(args.topic_file)

result = dict()
for index, line in enumerate(open(args.topic_file)):
    line_list = line.strip("\n").split("\t")
    if len(line_list) == 6:
        topic_id = line_list[0]
        parent_id = line_list[1]
        char_index = line_list[2]
        word_index_list = line_list[3]
        desc_char_index_list = line_list[4]
        desc_word_index_list = line_list[5]
    else:
        logging.info("the line index %d content:%s is not length 6 pay attention to it" % (index, line))

    result[index + 1] = topic_id


f = open("topic.pkl", "w")
pickler = pickle.Pickler(f)
pickler.dump(result)
f.close()

