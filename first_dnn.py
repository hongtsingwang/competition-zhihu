# -*- coding:utf-8 -*-
# =======================================================
# 
# @FileName  : first_dnn.py
# @Author    : Wang Hongqing
# @Date      : 2017-06-11 16:43
# 
# =======================================================

import os
import sys
import argparse
import logging
# import pandas as pd
# import numpy as np
# import tensorflow as tf
# from collections import Counter
import cPickle as pickle
from sklearn.model_selection import train_test_split
import numpy


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

home_dir = os.getcwd()
pkl_dir = os.path.join(home_dir, "pkl")
topic_dict_pkl = os.path.join(pkl_dir, "topic.pkl")
topic_file = open(topic_dict_pkl,"r").read()
topic_index_dict = pickle.loads(topic_file)


categories = topic_index_dict.keys()



# 划分测试集和训练集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

