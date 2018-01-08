import java.util.*;

public class WordCountMR extends MyMapReduce{
	
	public WordCountMR(KVPair[] data, int num_map_tasks, int num_reduce_tasks) {
		super(data, num_map_tasks, num_reduce_tasks);
	}

	public ArrayList<KVPair> map(KVPair kv){
		ArrayList<KVPair> counts = new ArrayList<KVPair>();
		HashMap<String,Integer> countsMap = new HashMap<String,Integer>();
		StringTokenizer tokens= new StringTokenizer((String)kv.v);
		
		while (tokens.hasMoreTokens()){
			String token=tokens.nextToken().toLowerCase();
			if(countsMap.containsKey(token)){
				countsMap.put(token,countsMap.get(token)+1);
			}
			else{
				countsMap.put(token,1);
			}
		}
		for(String token:countsMap.keySet()){
			counts.add(new KVPair(token, countsMap.get(token)));
		}
		
		return counts;
	}
	
	public KVPair reduce(KVPair kv){
		int sum=0;
		for(int val: (ArrayList<Integer>)kv.v){
			sum+=val;
		}
		
		return new KVPair(kv.k,sum);
	}
}
