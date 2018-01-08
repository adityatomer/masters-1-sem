#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re
from pyspark import SparkContext
import os

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3.6"
os.environ["PYSPARK_DRIVER_PYTHON"]="ipython3.6"

BLOG_FOLDER = "sample/"; #assuming the blogs folder is in the same directory as this file.

sc = SparkContext()


def getCompleteDataRDD():
	completeRDD =  sc.wholeTextFiles(BLOG_FOLDER)
	return completeRDD;

#Part 1 of 2 Question
def getIndustryNameAndBroadcast(completeRdd): 
	fileNamePathRDD = completeRdd.map(lambda x: x[0]).map(lambda x : x.split(BLOG_FOLDER)[1])
	fileNameRDD = fileNamePathRDD.map(lambda x : x.split(".")[3]);
	industryNames=fileNameRDD.distinct().map(lambda x:(x.lower(),"1")).collectAsMap();
	industryNamesBD = sc.broadcast(set(industryNames))
	return industryNamesBD


def convertIntoDateContentTuples(stg):
	stg = stg.lower().strip();
	# stg=stg.replace('\'',' '); #removing apostrophie
	# stg=stg.replace('!',' '); #removing apostrophie
	# stg=stg.replace('.',' '); #removing apostrophie

	q = re.findall('(?<=<date>).*?(?=</date>)|(?<=<post>).*?(?=</post>)',stg, re.DOTALL)
	datePostTuple=[]
	i=0
	while(i < len(q)-1):	
		date = None;
		try:
			date = q[i]
			if("," in  date):
				dateList = date.split(",")
				date = dateList[0]+"_"+dateList[1]+"_"+dateList[2]
		except:
			pass
		para=q[i+1].strip()
		wds = para.split(" ")
		for wd in wds :
			t = []
			t.append(date)
			wordtuple = []
			wordtuple.append(wd)
			t.append(tuple(wordtuple))
			datePostTuple.append(tuple(t))
		i=i+2
	return datePostTuple


def filterByIndustryValue(stg, bdd):
	wd=stg[0]
	print("filterByIndustryValue wd : "+str(wd))
	if(wd in bdd): 
		return wd
	else: None;


if __name__ == '__main__':
	completeRDD=getCompleteDataRDD();
	industryNamesBD= getIndustryNameAndBroadcast(completeRDD);
	ans =  completeRDD.flatMap(lambda x : convertIntoDateContentTuples(x[1]))\
							.map(lambda x: ((x[0],filterByIndustryValue(x[1],industryNamesBD.value)),1))\
							.filter(lambda x: x[0][1] != None)\
							.reduceByKey(lambda a,b:a+b)\
							.map(lambda a: (a[0][1],(a[0][0],a[1])))\
							.groupByKey()\
							.mapValues(tuple)\
							.collect()
	print(ans)









