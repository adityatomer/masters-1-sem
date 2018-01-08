#!/usr/bin/python3
from pyspark import SparkContext
import os
from random import random


os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.6"
os.environ["PYSPARK_DRIVER_PYTHON"]="ipython3.6"

sc = SparkContext()

def getDataForWordCount():
      print("WordCount:");
      data =[(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")];
      return data;

def getDataForSetDifferenceTYPE1():
      print("SetDifference Fruits");
      data = [('R', ['apple', 'orange', 'pear', 'blueberry']),('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
      return data;

def getDataForSetDifferenceTYPE2():
      print("SetDifference Numbers")
      data = [('R', [x for x in range(50) if random() > 0.5]),('S', [x for x in range(50) if random() > 0.75])]
      return data;


def calculateSetDifference(data):
      ans = sc.parallelize(data).\
            flatMap(lambda a:([(i,a[0]) for i in a[1]])).\
            reduceByKey(lambda a,b:a+b).\
            filter( lambda x: x[1]== 'R').\
            map(lambda x: (x[1],x[0])).\
            groupByKey().\
            mapValues(list).\
            collect()

      return ans

def calculateWordCount(data):
      ans = sc.parallelize(data).\
            map(lambda x: x[1]).\
            flatMap(lambda l : l.split(" ")).\
            map(lambda wd:(wd.lower(),1)).\
            reduceByKey(lambda a,b:a+b).\
            collect()
      return ans



if __name__ == '__main__':
      data = getDataForWordCount();            
      print(calculateWordCount(data));

      data = getDataForSetDifferenceTYPE1();            
      print(calculateSetDifference(data));

      data = getDataForSetDifferenceTYPE2();            
      print(calculateSetDifference(data));

