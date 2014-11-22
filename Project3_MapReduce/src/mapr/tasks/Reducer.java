package mapr.tasks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mapr.io.KVReader;
import mapr.io.KVWriter;

/**
 * Abstract class for Reducer that users can extend based on the MapReduce job.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public abstract class Reducer implements Task {
  /**
   * List of input file names for reducer to consume.
   */
  List<String> inputFiles;
  /**
   * Writer of final K-V pairs.
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

  public Reducer(List<String> inputFiles, String outputFile, String otherArgs)
      throws FileNotFoundException {
    this.inputFiles = inputFiles;
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
   * Writes a (key,value) pair to the defined output file.
   * 
   * @param key Key to write
   * @param value Value to write
   * @throws IOException File I/O Exception
   */
  public void emit(String key, String value) throws IOException {
    writer.writeKV(key, value);
  }

  /**
   * Returns the index of (key,value) pair with "smallest" key value.
   * 
   * @param list List of (key,value) pairs to consider
   * @return Index to "smallest" key
   */
  private int minIndex(List<Map.Entry<String, String>> list) {
    int minIndex = 0;
    Map.Entry<String, String> minKV = list.get(0);
    for (int x = 1; x < list.size(); x++) {
      if (compareTo(list.get(x).getKey(), minKV.getKey()) < 0) {
        minIndex = x;
        minKV = list.get(x);
      }
    }
    return minIndex;
  }

  /**
   * Entry point for a reducer task.
   */
  @Override
  public void run() {
    List<KVReader> readers = new ArrayList<KVReader>();
    try {
      /* Create a Key-Value Reader for each data source */
      List<Map.Entry<String, String>> kvs = new ArrayList<Map.Entry<String, String>>();
      for (int x = 0; x < inputFiles.size(); x++) {
        KVReader reader = new KVReader(inputFiles.get(x));
        Map.Entry<String, String> kv = reader.readNextKV();
        if (kv != null) {
          readers.add(reader);
          kvs.add(kv);
        }
      }

      /* Keep aggregating (key,value) pairs until we complete. */
      while (readers.size() != 0) {
        int minIndex = minIndex(kvs);
        Map.Entry<String, String> minKV = kvs.get(minIndex);
        /* Replace the Key-Value Reader with minIndex. */
        Map.Entry<String, String> replacement = readers.get(minIndex).readNextKV();
        if (replacement == null) {
          readers.remove(minIndex);
          kvs.remove(minIndex);
        } else {
          kvs.set(minIndex, replacement);
        }

        /* Aggregate same keys */
        ArrayList<String> groupMinKey = new ArrayList<String>();
        groupMinKey.add(minKV.getValue());

        for (int x = 0; x < kvs.size(); x++) {
          while (kvs.get(x) != null && compareTo(kvs.get(x).getKey(), minKV.getKey()) == 0) {
            groupMinKey.add(kvs.get(x).getValue());
            Map.Entry<String, String> replace = readers.get(x).readNextKV();
            kvs.set(x, replace);
            if (replace == null) {
              readers.get(x).close();
            }
          }
        }

        /* Delete completed files */
        Iterator<KVReader> readerIterator = readers.iterator();
        Iterator<Map.Entry<String, String>> kvsIterator = kvs.iterator();
        while (kvsIterator.hasNext()) {
          readerIterator.next();
          Map.Entry<String, String> kv = kvsIterator.next();
          if (kv == null) {
            readerIterator.remove();
            kvsIterator.remove();
          }
        }

        reduce(minKV.getKey(), groupMinKey.iterator());
      }

    } catch (Exception e) {
      success = false;
      e.printStackTrace();
    } finally {
      for (KVReader reader : readers) {
        reader.close();
      }
      writer.close();
    }
  }

  /**
   * User's rule for reducing a list of values associated with a same key.
   * 
   * @param key Shared key value
   * @param values Iterator of values with <tt>key</tt>
   */
  public abstract void reduce(String key, Iterator<String> values) throws Exception;

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
