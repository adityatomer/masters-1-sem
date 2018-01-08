
public class ReducerTask implements Comparable{

	int task_num;
	KVPair kv;
	public ReducerTask(){};
	public ReducerTask(int task_num, KVPair kv){
		this.task_num=task_num;
		this.kv=kv;
	}
	@Override
	public int compareTo(Object anotherReducerTask) throws ClassCastException {
		if (!(anotherReducerTask instanceof ReducerTask)){
			throw new ClassCastException("Must be sorted with other ReducerTask objects");
		}
		int anotherReducerTaskNum = ((ReducerTask)anotherReducerTask).task_num;
		return this.task_num - anotherReducerTaskNum;
	}
	
	@Override
	public String toString(){
		return "("+this.task_num+", ("+this.kv.k+","+this.kv.v+"))";
	}
	
}
