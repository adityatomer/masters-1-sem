
public class KVPair implements Comparable {
	Object k;
	Object v;
	public KVPair(Object k, Object v){
		this.k=k;
		this.v=v;
	}
	@Override
	public int compareTo(Object anotherKVPair) {
		if ((anotherKVPair instanceof KVPair) && (((KVPair)anotherKVPair).k instanceof Integer) && (this.k instanceof Integer)){
			int anotherKVPairKey = (int)((KVPair)anotherKVPair).k;
			return (int)this.k - anotherKVPairKey;
		}
		if ((anotherKVPair instanceof KVPair) && (((KVPair)anotherKVPair).k instanceof String) && (this.k instanceof String)){
			String anotherKVPairKey = (String)((KVPair)anotherKVPair).k;
			
			return ((String)this.k).compareTo(anotherKVPairKey);
		}
		else{
			throw new ClassCastException();
		}

	}
	
	@Override
	public String toString(){
		return "("+this.k+","+this.v+")";
	}
	
}
