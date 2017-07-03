# coding: utf-8

import os
import pandas as pd
from tqdm import tqdm
import time
import re
import numpy as np
from six.moves import xrange

home_dir = os.getcwd()
data_dir = os.path.join(home_dir, "data")
result_dir = os.path.join(home_dir,"result")
os.mkdirs(result_dir)

topic_info_file_name = os.path.join(data_dir,"topic_info.txt")
topic_info = pd.read_table(topic_info_file_name, sep='\t',header=None)
print(topic_info.iloc[0:5])

# TODO 需要等其他程序能运行成功了才能运行这个程序
# TODO 将TOPIC名称和ID的关系搞成一个pkl文件， 方便反复调用
topic_dict = {}
for i in xrange(topic_info.shape[0]):
    topic_dict[i] = topic_info.iloc[i][0]

now_time = time.time.now().strftime("%Y%m%d_%H%M")
predict_file = os.path.join(result_dir, "predict.%s.txt"% now_time)

# 获取从别的程序获得的predict程序 
predict = open('predict.txt', "r")
examples = predict.readlines()
text = np.array([line.split(" ") for line in examples])

label = []
for line in tqdm(text):
    num2label = []
    for i in xrange(5):
        num2label.append(topic_dict[int(line[i])]) # 把0-1999编号转成原来的id
    label.append(num2label)
label = np.array(label)

np.savetxt("temp.txt",label,fmt='%d')

def clean_str(string):
    string = re.sub(r" ", ",", string)
    return string

file1 = open('temp.txt', "r")
examples = file1.readlines()
examples = [clean_str(line) for line in examples]
file1.close()

file1 = open('temp.txt', "w")
file1.writelines(examples)
file1.close()


# In[8]:

# predict文件导入
predict_file = 'temp.txt'
predict_reader = pd.read_table(predict_file,sep=' ',header=None)
print(predict_reader.iloc[0:5])


# In[9]:

# 导入question_train_set
eval_reader = pd.read_table('./ieee_zhihu_cup/question_eval_set.txt',sep='\t',header=None)
print(eval_reader.iloc[0:3])


# In[10]:

final_predict = pd.concat([eval_reader.ix[:,0],predict_reader],axis=1)
print(final_predict.iloc[0:5])


# In[11]:

final_predict.to_csv('temp.txt', header=None, index=None, sep=',')

final_file = open('temp.txt', "r")
final_examples = final_file.readlines()
final_examples = [re.sub(r'"',"",line) for line in final_examples]
final_file.close()

final_file = open('final_predict.csv', "w")
final_file.writelines(final_examples)
final_file.close()
