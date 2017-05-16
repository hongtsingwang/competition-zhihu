#!/usr/bin/env python
# coding=utf-8

from pyspark import SparkContext

print "123"
print "123"
print "123"
print "123"
print "123"
print "123"
print "123"
print "123"
print "123"
print "123"
print "123"
print "123"
print "123"
logFile = "/tmp/profit_new.sh"  # Should be some file on your system
sc = SparkContext("yarn-client", "Simple App")
logData = sc.textFile(logFile).cache()

print "456"
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
