package org.apache.nutch.urlfilter.nearduplicate;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.io.BufferedReader;
import java.io.FileReader;

public class GetDuplicateUrls
{
    public static void main(String[] args)
    {
       try
       {
           File file_1 = new File(args[0]);
           File file_2 = new File(args[1]);
           Map<String, Long> objs = new HashMap<String, Long>();
           SimhashIndex simind = new SimhashIndex(objs);

           int index = 0;
           BufferedReader br = new BufferedReader(new FileReader(file_1));

           String line;
           while ((line = br.readLine()) != null)
           {
               long simhash = SimHash.computeSimHashFromString(Shingle.shingles(line)); 

               Set<String> duplicates = simind.get_near_dups(simhash);

               if(duplicates.size() > 0)
               {
                    System.out.print("duplicate " + index);
                    System.out.print(" duplicate_of ");
                    for(String i: duplicates)
                    {
                        System.out.print(" " + i);
                    }
               }
               else
               {
                   simind.add(Integer.toString(index), simhash);
               }
               index += 1;

           }

       }
       catch(Exception e)
       {
           e.printStackTrace();
       }
    }
}
