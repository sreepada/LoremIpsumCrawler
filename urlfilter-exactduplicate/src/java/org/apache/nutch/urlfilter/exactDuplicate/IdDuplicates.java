package org.apache.nutch.urlfilter.exactduplicate;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class IdDuplicates {
	
	//Map<simhash, message digest>
	Map<String, List<String>> bucketList = null;
	MessageDigest msgDigest = null;
	Boolean isUsable = false;
	public IdDuplicates()
	{
		//System.out.println("Instatiating Exact exact duplicate detection module.");
		bucketList = new TreeMap<String,List<String>>();
		
		try {
			//System.out.println("Creating MD5 message digest object");
			msgDigest = MessageDigest.getInstance("MD5");
			isUsable = true;
		} catch (NoSuchAlgorithmException e) {
			//System.out.println(e.getMessage());
			//System.out.println("Trying to generate instance of SHA msg digest object.");
			
			try {
				msgDigest = MessageDigest.getInstance("SHA");
				isUsable=true;
			} catch (NoSuchAlgorithmException e1) {
				
				//System.out.println("Failed to instantiate message digest.");
				//e1.printStackTrace();
				isUsable = false;
			}
		}
		
	}
	
	boolean checkForExactDuplicates(String data, String url)
	{
		//extract random text to a variable from data. Hard coded to 5 words after every 10 words.
		String simHashData = extractRandomtextFromData(data,5);
		
		//Generate simhash for randomly extracted data.
		long key = SimHash.computeOptimizedSimHashForString(simHashData);
		
		//Generate checksum value of the textdata.
		BigInteger checksum = new BigInteger(msgDigest.digest(data.getBytes()));
		
		//Get bucket entries with same simhash for randomly extracted text.
		List<String> commonBuckets = bucketList.get(Long.toString(key));
		
		int dupcount = 0;
		for(int i=0;commonBuckets!=null && i<commonBuckets.size();i++)
		{
			String docDigest = commonBuckets.get(i);
			if(docDigest.equals(checksum.toString()))
			{
				//potential duplicate document.
				dupcount++;
				break;
			}
				
		}
		
		if(dupcount > 0)
			//Duplicate found.
			return true;
		else
		{
			//No duplicates found.
			addToBucketList(Long.toString(key), checksum.toString());
			return false;
		}
	}
	
	
	String extractRandomtextFromData(String data, int partSize)
	{
		//Split the text content on spaces.
		String[] dataArray = data.split(" ");
		int size = dataArray.length;
		int i=0;
		
		//select <partsize> number of words after every 10 words starting from word index 0.
		StringBuffer buffer  = new StringBuffer();
		while(i<size)
		{
			for(int j=i;j<i+partSize && j<size;j++)
				buffer.append(dataArray[j]+" ");
			i=i+10;
		}
		
		return buffer.toString();
	}
	
	void addToBucketList(String key, String checksum )
	{
		if(bucketList.containsKey(key))
			bucketList.get(key).add(checksum);
		else
		{
			List<String> newlist = new ArrayList<String>();
			newlist.add(checksum);
			bucketList.put(key, newlist);
		}
	}

/*	public static void main(String args[]) throws Exception
	{
		IdDuplicates tempid = new IdDuplicates();
		
		File file1 = new File("src/com/one/a.txt");
		File file2 = new File("src/com/one/b.txt");
		byte[] data1 = new byte[(int) file1.length()];
		byte[] data2 = new byte[(int) file1.length()];
		FileInputStream reader1 = new FileInputStream(file1);
		FileInputStream reader2 = new FileInputStream(file2);
		
		reader1.read(data1);
		reader2.read(data2);
		
		
		String str1 =new String(data1);
		
		String str2 = new String(data2);
		
		boolean r1 = tempid.checkForExactDuplicates(str1,"temporary");
		
		System.out.println(file1.getName()+" "+r1);
		
		boolean r2 = tempid.checkForExactDuplicates(str2, "temporary");
		
		System.out.println(file2.getName()+" "+r2);
		
	}
*/
	
}

