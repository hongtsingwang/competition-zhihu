#!/usr/bin/env python
# coding=utf-8

file_open = open("test","r")
result = open("test.csv","w")

for line in file_open:
    line_list = line.strip("\n,").split(",")
    if len(line_list) < 6:
        while len(line_list) < 6:
            line_list.append("-1")
    result.write(",".join(line_list) + "\n")


