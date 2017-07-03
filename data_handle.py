# coding: utf-8

# 脚本功能概述
# 1.title通常来说包含的信息最重要。对于question_train_set.txt文件，为了简单起见，我们只取第三列，title的词语编号序列。
# 2.对于topic_info.txt，为了简单起见，我们不考虑2,3,4,5,6列。只是简单的提取话题id，然后转为0-1998的数字（一共有1999个话题）
# 3.然后合并以上一些数据，得到最后处理后的数据。

import os
import pandas as pd
from tqdm import tqdm
from six.moves import xrange

home_dir = os.getcwd()
data_dir = os.path.join(home_dir, "data")
transform_data_dir = os.path.join(home_dir, "transform_data")
result_dir = os.path.join(home_dir, "result")

train_file = os.path.join(data_dir, "question_train_set.txt")
reader = pd.read_table(train_file, sep='\t', header=None)
print(reader.iloc[0:5])

test_file = os.path.join(data_dir, "question_topic_train_set.txt")
topic_reader = pd.read_table(test_file, sep='\t', header=None)
print(topic_reader.iloc[0:5])


# 合并title 的词语编号序列和话题 id
data_topic = pd.concat(
    [reader.ix[:, 2], topic_reader.ix[:, 1]], axis=1, ignore_index=True)
print(data_topic.iloc[0:5])

# 导入topic_info
topic_info_file_name = os.path.join(data_dir, "topic_info.txt")
label_reader = pd.read_table(topic_info_file_name, sep='\t', header=None)
print(label_reader.iloc[0:5])


# 把标签转为0-1998的编号
labels = list(label_reader.iloc[:, 0])
my_labels = []
for label in labels:
    my_labels.append(label)

# 建立topic字典
topic_dict = {}
for index, label in enumerate(my_labels):
    topic_dict[label] = index

print(topic_dict[7739004195693774975])

for i in tqdm(xrange(data_topic.shape[0])):
    new_label = ''
    # 根据“,”切分话题id
    temp_topic = data_topic.iloc[i][1].split(',')
    for topic in temp_topic:
        label_num = topic_dict[int(topic)]
        new_label = new_label + str(label_num) + ','
    data_topic.iloc[i][1] = new_label[:-1]
print(data_topic.iloc[:5])

data_topic_file = os.path.join(transform_data_dir, "data_topic.txt")
data_topic.to_csv(data_topic_file, header=None, index=None, sep='\t')

# 切分成10块保存
data_topic_block_prefix = os.path.join(
    transform_data_dir, "data_topic_block_%s.txt")
# NOTE: 文件行数一共3000000行左右=10*300000
block_num = 300000
range_num =  int(data_topic.shape[0] *1.0 / block_num) + 1 if data_topic.shape[0] % block_num != 0 else int(data_topic.shape[0] *1.0 / block_num)
for i in xrange(range_num):
    data_topic_filename = data_topic_block_prefix % str(i)
    if (i + 1) * block_num < data_topic.shape[0]:
        data_topic.iloc[i * block_num:(i + 1) * block_num].to_csv(
            data_topic_filename, header=None, index=None, sep='\t')
    else:
        data_topic.iloc[i * block_num:data_topic.shape[0]].to_csv(
            data_topic_filename, header=None, index=None, sep='\t')
