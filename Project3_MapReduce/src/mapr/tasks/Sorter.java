package mapr.tasks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import mapr.io.KVReader;
import mapr.io.KVWriter;

/**
 * Abstract class for Sorter that users can extend based on the MapReduce job.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public abstract class Sorter implements Task {
  /**
   * Reader of intermediate K-V pairs.
   */
  KVReader reader;
  /**
   * Writer of K-V pairs to feed to reducer.
   */
  KVWriter writer;
  /**
   * Other command-line arguments given by user.
   */
  String otherArgs;
  /**
   * <tt>true</tt> if the task has succeeded.
   */
  boolean success;

  public Sorter(String inputFile, String outputFile, String otherArgs) throws FileNotFoundException {
    this.reader = new KVReader(inputFile);
    this.writer = new KVWriter(outputFile);
    this.otherArgs = otherArgs;
    this.success = true;
  }

  public String getOtherArgs() {
    return otherArgs;
  }

  public boolean isSuccess() {
    return success;
  }

  /**
   * Merges a new key-value pair with the current accumulator of final key-value pairs.
   * 
   * @param sorted <tt>TreeMap</tt> as accumulator for the final (key,value) mapping.
   * @param key New key to merge.
   * @param value New value to merge.
   */
  private void add(TreeMap<String, String> sorted, String key, String value) {
    String prevVal = sorted.get(key);
    sorted.put(key, combine(prevVal, value));
  }

  /**
   * Entry point for a sorter task.
   */
  @Override
  public void run() {
    try {
      Comparator<String> comparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          return compareTo(o1, o2);
        }
      };

      /* Add intermediate k-v pairs into final k-v pairs by merging. */
      TreeMap<String, String> sorted = new TreeMap<String, String>(comparator);
      Map.Entry<String, String> entry = null;
      while ((entry = reader.readNextKV()) != null) {
        add(sorted, entry.getKey(), entry.getValue());
      }

      /* Write final k-v mapping to output file. */
      for (Map.Entry<String, String> kv : sorted.entrySet()) {
        writer.writeKV(kv.getKey(), kv.getValue());
      }
    } catch (IOException e) {
      success = false;
      e.printStackTrace();
    } finally {
      reader.close();
      writer.close();
    }
  }

  /**
   * User's rule for combining two values with the same key
   * 
   * @param val1 Previous value
   * @param val2 New value to merge in
   * @return Combined value
   */
  public abstract String combine(String val1, String val2);

  /**
   * Customizable comparator between two Strings.
   * 
   * @param key1 First string
   * @param key2 Second string
   * @return negative, zero, or positive iff first string is smaller than, equal to, or greater than
   *         the second, respectively.
   */
  public abstract int compareTo(String key1, String key2);
}
