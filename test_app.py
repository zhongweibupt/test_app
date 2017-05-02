# encoding: utf-8
from pyspark import SparkContext

sc = SparkContext("local", "TestApp")
lines = sc.textFile("D:\\papers\\FieldsOfStudy.txt")
pairs = lines.map(lambda x: x.split("\t"))

print pairs.collect()[0]

"""
nums = sc.parallelize([1, 2, 3])
s = nums.filter(lambda x: x > 1).collect()
for num in s:
    print num

s = nums.cartesian(nums).collect()
for num in s:
    print num

s = nums.reduce(lambda x, y : x - y)
print s

print nums.collect()
"""

