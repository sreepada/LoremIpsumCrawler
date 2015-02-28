import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class SimhashIndex {
	
	private int k = 3;
	private int f = 64;
	
	private ArrayList<Integer> offsets = new ArrayList<Integer>();
	
	private Map<String, Set<String>> bucket = new HashMap<String, Set<String>>();
	
	
	public SimhashIndex(Map<String, Long> objs)
	{
		objs.size();
		for(int i = 0; i < k + 1; i++)
		{
			offsets.add(((f/(k+1))*i));
		}
		Iterator<Entry<String, Long>> it = objs.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry<String, Long> pair = (Map.Entry<String, Long>)it.next();
			add(pair.getKey(), pair.getValue());
		}

	}
	
	public ArrayList<String> get_keys(long hash)
	{
		ArrayList<String> result = new ArrayList<String>();
		for(int i = 0; i < offsets.size(); i++)
		{
			int offset = (int)offsets.get(i);
			int m = 0;
			if (i == offsets.size() - 1)
			{
				m = (int)Math.pow(2, f - offset)-1;
			}
			else
			{
				m = (int)Math.pow(2, offsets.get(i+1) - offset) - 1;
			}
			//System.out.println("m = " + m);
			long c = hash >> offset & m;
			//System.out.println("kooo" + Long.toHexString(c));
			String res = Long.toHexString(c) + "," + Integer.toString(i);
			//System.out.println(res);
			result.add(res);
		}
		return result;
	}
	
	public int bucket_size()
	{
		return bucket.size();
	}
	
	public void add(String id, long value)
	{
		for(String key: get_keys(value))
		{
			String v = Long.toString(value) + "," + id;
			//System.out.println("hex value = " + v);
			
			if(bucket.containsKey(key))
			{
				bucket.get(key).add(v);
			}
			else
			{
				bucket.put(key, new HashSet<String>());
				bucket.get(key).add(v);
			
			}
		}
	}
	
	
	public Set<String> get_near_dups(long hash)
	{
		Set<String> result = new HashSet<String>();
		//System.out.println(bucket);
		for(String key: get_keys(hash))
		{
			Set<String> dups = bucket.get(key);
			//System.out.println("lala");
			//System.out.println(key);
			if(dups == null)
			{
				continue;
			}
			for(String dup:dups)
			{
				//System.out.println(dup);
				String[] parts = dup.split(",");
				//System.out.println(new BigInteger(parts[0], 16));
				int distance = distance(hash,Long.parseLong(parts[0]));
				if(distance <= k)
				{
					result.add(parts[1]);
				}
						
			}
		}
		return result;
	}
	
	public static int distance(long hash1, long hash2) {
		long bits = hash1 ^ hash2;
		int count = 0;
		while (bits != 0) {
			bits &= bits - 1;
			++count;
		}
		return count;
	}

	
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		try{
		
//	      File file1 = new File(args[0]);
//	      File file2 = new File(args[1]);
//
//	      byte data1[] = new byte[(int) file1.length()];
//	      byte data2[] = new byte[(int) file2.length()];
//	      FileInputStream stream1 = new FileInputStream(file1);
//	      FileInputStream stream2 = new FileInputStream(file2);
//	      stream1.read(data1);
//	      stream2.read(data2);
	      //String string1 = new String(data1);
	      //String string2 = new String(data2);
	      String string1 = "abcd";
	      String string2 = "acdb";
	      long simhash1 = SimHash.computeSimHashFromString(Shingle.shingles(string1));
	      long simhash2 = SimHash.computeSimHashFromString(Shingle.shingles(string2));
	      
	      Map<String, Long> objs = new HashMap<String, Long>();
	      objs.put("1", simhash1);
	      objs.put("2", simhash2);
	      objs.put("3", simhash1);
	      objs.put("4", simhash2);	      
	      SimhashIndex simind = new SimhashIndex(objs);
	      
	      System.out.println("Duplicates are");
	      for(String i :simind.get_near_dups(simhash1))
	      {
	    	  System.out.println(i);
	      }
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}

}
