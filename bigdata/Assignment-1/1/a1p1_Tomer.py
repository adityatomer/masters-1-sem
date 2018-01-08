########################################
## Template Code for Big Data Analytics
## assignment 1 - part I, at Stony Brook Univeristy
## Fall 2017


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random
from math import ceil
import os
import hashlib

##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:#[TODO]
    __metaclass__ = ABCMeta
    
    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3): #[DONE]
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks

    ###########################################################   
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): #[DONE]
        print("Need to override map")

    
    @abstractmethod
    def reduce(self, k, vs): #[DONE]
        print("Need to override reduce")
        

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r): #[DONE]
        #runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            #run mappers:
            mapped_kvs = self.map(k, v)
            #assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                partitionNumber = self.partitionFunction(k);
                # print "partitionNumber : ",partitionNumber,", (k, v): ",(k, v);
                namenode_m2r.append((partitionNumber, (k, v)))
            # print "mapTask : namenode_m2r : ",namenode_m2r
    def partitionFunction(self,k): #[TODO]
        #given a key returns the reduce task to send it
        if(isinstance(k,int)):
            return k% self.num_reduce_tasks;
        return int(hashlib.sha1(k.encode("utf-8")).hexdigest(), 16) % self.num_reduce_tasks;

    def reduceTask(self, kvs, namenode_fromR): #[TODO]
        #sort all values for each key (can use a list of dictionary)
        # print "Before reduceTask : ",kvs;
        kvs.sort();
        keyToValueList = {};
        for (index,(key,value)) in enumerate(kvs):
            if(key not in keyToValueList):
                keyToValueList[key]=[];
            keyToValueList[key].append(value);
        for keyi in keyToValueList.keys():
            val = self.reduce(keyi, keyToValueList[keyi]);
            if(val != None):
                namenode_fromR.append(val);
        

        
    def runSystem(self): #[TODO]
        #runs the full map-reduce system processes on mrObject

        #the following two lists are shared by all processes
        #in order to simulate the communication
        #[DONE]
        namenode_m2r = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
                                        #in the form [(k, v), ...]
        

        completeData = self.data;
        chunkSizeForAMapper = int(ceil(float(len(completeData))/float(self.num_map_tasks)));
        # print "chunkSizeForAMapper : ",chunkSizeForAMapper,", len(completeData): ",len(completeData),",self.num_map_tasks:",self.num_map_tasks;
        chunkData = [ [] for i in range(self.num_map_tasks)];
        
        k=0;i=0;j=0;
        while(k < len(completeData)):
            chunkData[j].append(completeData[k]);
            j= (j+1)%self.num_map_tasks;
            k=k+1;
        pList = [];
        
        #self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        #self.num_reduce_tasks=num_reduce_tasks; # " " " as reduce tasks

        #divide up the data into chunks accord to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it. 
        #hint: if chunk contains the data going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()  
        #  (it might be useful to keep the processes in a list)
        #[TODO]

        try:
            for i in range(0,self.num_map_tasks):
                p = Process(target=self.mapTask, args=(chunkData[i],namenode_m2r));
                p.start();
                pList.append(p);
            
            for p in pList:
                p.join();
                
            print("namenode_m2r after map tasks complete in main:",namenode_m2r)
            pprint(sorted(list(namenode_m2r)))
        except Exception as e:
            pass
        

        #"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)] 
        for keyIndex,(k,v) in namenode_m2r:
            to_reduce_task[keyIndex].append((k,v));

        reduceProcessList = []
        try:
            for i in range(0,self.num_reduce_tasks):
                p = Process(target=self.reduceTask, args=(to_reduce_task[i],namenode_fromR)); 
                p.start();
                reduceProcessList.append(p);
            for i in reduceProcessList:
                i.join();
        except Exception as e:
            pass;
        
        
        #print output from reducer tasks 
        #[DONE]
        print("namenode_m2r after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        #return all key-value pairs:
        #[DONE]
        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:
            
class WordCountMR(MyMapReduce): #[DONE]
    #the mapper and reducer for word count
    def map(self, k, v): #[DONE]
        counts = dict()
        for w in v.split():
            w = w.lower() #makes this case-insensitive
            try:  #try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs): #[DONE]
        return (k, np.sum(vs))        
    

class SetDifferenceMR(MyMapReduce): #[TODO]
    #contains the map and reduce function for set difference
    #Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, k, v): #[DONE]
        pair = dict()
        for w in v:
            if(isinstance(w,str)):
                w = w.lower(); #makes this case-insensitive
            pair[w] = k;
        return pair.items()

    def reduce(self, k, vs): #[DONE]
        ## Assuming the difference is always set {R}-{S} ##
        if(len(vs)==2 or vs[0] == 'S') : #Ignoring values not having "R" in it.
            return None;
        else: return (vs[0],k);

    


##########################################################################
##########################################################################


if __name__ == "__main__": #[DONE: Uncomment peices to test]
    ###################
    ##run WordCount:
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()
             
    ####################
    ##run SetDifference
    #(TODO: uncomment when ready to test)
    print("\n\n*****************\n Set Difference\n*****************\n")
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    data2 = [('R', [x for x in range(50) if random() > 0.5]),  ('S', [x for x in range(50) if random() > 0.75])]
     
    mrObject = SetDifferenceMR(data1, 2, 2)
    mrObject.runSystem()
    mrObject = SetDifferenceMR(data2, 2, 2)
    mrObject.runSystem()



