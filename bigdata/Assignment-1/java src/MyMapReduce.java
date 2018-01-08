//########################################
//## Template Code for Big Data Analytics
//## assignment 1, at Stony Brook University
//## Fall 2017

import java.util.*;
import java.util.concurrent.*;

//##########################################################################
//##########################################################################
//# PART I. MapReduce

public abstract class MyMapReduce {
	KVPair[] data;
	int num_map_tasks=5;
	int num_reduce_tasks=3;
	final SharedVariables svs = new SharedVariables();
	class SharedVariables{
		public volatile List<ReducerTask> namenode_m2r = Collections.synchronizedList(new ArrayList<ReducerTask>());
		public volatile List<KVPair> namenode_fromR = Collections.synchronizedList(new ArrayList<KVPair>());
	}
	
	public MyMapReduce(KVPair[] data, int num_map_tasks, int num_reduce_tasks){
		this.data=data;
		this.num_map_tasks=num_map_tasks;
		this.num_reduce_tasks=num_reduce_tasks;
	}
	
    //###########################################################   
    //#programmer methods (to be overridden by inheriting class)
	
	public abstract ArrayList<KVPair> map(KVPair kv);
	public abstract KVPair reduce(KVPair kv);
	
    //###########################################################
    //#System Code: What the map reduce backend handles
	public void mapTask(KVPair[] data_chunk, List<ReducerTask> namenode_m2r){
		//#runs the mappers and assigns each k,v to a reduce task
		for (KVPair kv : data_chunk){
			//#run mappers:
			ArrayList<KVPair> mapped_kvs = this.map(kv);
			//#assign each kv pair to a reducer task
			for (KVPair mapped_kv:mapped_kvs){
				namenode_m2r.add(new ReducerTask(this.partitionFunction(mapped_kv.k.toString()),mapped_kv));
			}
		}
	}
	
	public int partitionFunction(String k){
		//#given a key returns the reduce task to send it
		int node_number=0;
		//#implement this method
		return node_number;
	}
	
	
	public void reduceTask(ArrayList<KVPair> kvs, List<KVPair> namenode_fromR){
        //#sort all values for each key into a list 
        //#[TODO]


        //#call reducers on each key paired with a *list* of values
        //#and append the result for each key to namenode_fromR
        //#[TODO]
	}
	
	public List<KVPair> runSystem() throws ExecutionException, InterruptedException{
		/*#runs the full map-reduce system processes on mrObject


        #the following two lists are shared by all processes
        #in order to simulate the communication
        #[DONE]*/
		
		
		/*#divide up the data into chunks accord to num_map_tasks, launch a new process
         *#for each map task, passing the chunk of data to it. 
         *#hint: if a chunk contains the data going to a given maptask then the following
         *#      starts a process (Thread or Future class are possible data types for this variable)
         *#      
         *		if you want to use Future class, you can define variables like this:
         *		manager = Executors.newFixedThreadPool(max_num_of_pools);
		 *		processes = new ArrayList<Future<Runnable>>();
         *          Future process= manager.submit(new Runnable(){
		 *			@Override
		 *			public void run() { //necessary method to be called
		 *			}				
		 *			});
		 *		processes.add(process);
		 *		}
         *#      for loop with p.get()  // this will wait for p to get completed
         *#[TODO]
         *      After using manager, make sure you shutdown : manager.shutdownNow();
         *      else if you want to use Thread
         *      
         *      ArrayList<Thread> processes = new ArrayList<Thread>();
         *      processes.add(new Thread(new Runnable(){
				@Override
				public void run() {					
				}
			}));
			p.start();
			Then, join them with 'p.join();' // wait for all ps to complete tasks
         *      */
		
		System.out.println("namenode_m2r after map tasks complete:");
	
		pprint(svs.namenode_m2r);
		
		/*
		 *#"send" each key-value pair to its assigned reducer by placing each 
         *#into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
		 */
		
		ArrayList<KVPair>[] to_reduce_task= new ArrayList[num_reduce_tasks];
		/*
		 *#[TODO]
         *
         *#launch the reduce tasks as a new process for each. 
         *#[TODO]
         *
         *#join the reduce tasks back
         *#[TODO]
         *
         *#print output from reducer tasks 
         *#[DONE]
		 */
		System.out.println("namenode_m2r after reduce tasks complete:");
		pprint(svs.namenode_fromR);
		
		/*#return all key-value pairs:
         *#[DONE]
		 * 
		 */
		return svs.namenode_fromR;
	}
	
	public void pprint(List list){
		if (list.size()==0){
			System.out.println();
			return;
		}
		if (list.get(0) instanceof ReducerTask){
			ReducerTask[] arrayToPrint=Arrays.copyOf(list.toArray(), list.toArray().length, ReducerTask[].class);
			Arrays.sort(arrayToPrint);
			for(ReducerTask elem : arrayToPrint){
				System.out.println(elem.toString());
			}
		}
		
		if (list.get(0) instanceof KVPair){
			KVPair[] arrayToPrint=Arrays.copyOf(list.toArray(), list.toArray().length, KVPair[].class);
			Arrays.sort(arrayToPrint);
			for(KVPair elem : arrayToPrint){
				System.out.println(elem.toString());
			}
			
		}
		
		
	}
}