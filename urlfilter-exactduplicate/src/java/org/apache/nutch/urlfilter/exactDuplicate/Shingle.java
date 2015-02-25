package org.apache.nutch.urlfilter.exactduplicate;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

public class Shingle {

  public static final int CHAR_GRAM_LENGTH = 4;

  public static Set<String> shingles(String line) {

    HashSet<String> shingles = new HashSet<String>();

    for (int i = 0; i < line.length() - CHAR_GRAM_LENGTH + 1; i++) {
      // extract an ngram
      String shingle = line.substring(i, i + CHAR_GRAM_LENGTH);
      // get it's index from the dictionary
      shingles.add(shingle);
    }
    return shingles;
  }

  public static float jaccard_similarity_coeff(Set<String> shinglesA,
      Set<String> shinglesB) {
    float neumerator = Sets.intersection(shinglesA, shinglesB).size();
    float denominator = Sets.union(shinglesA, shinglesB).size();
    return neumerator / denominator;
  }
}
