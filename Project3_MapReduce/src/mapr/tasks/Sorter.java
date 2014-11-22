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
  KVReader reader;
  KVWriter writer;
  String otherArgs;
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

  private void emit(String key, String value) throws IOException {
    writer.writeKV(key, value);
  }

  private void add(TreeMap<String, String> sorted, String key, String value) {
    String prevVal = sorted.get(key);
    sorted.put(key, combine(prevVal, value));
  }

  @Override
  public void run() {
    try {
      Comparator<String> comparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          return compareTo(o1, o2);
        }
      };

      TreeMap<String, String> sorted = new TreeMap<String, String>(comparator);

      Map.Entry<String, String> entry = null;
      while ((entry = reader.readNextKV()) != null) {
        add(sorted, entry.getKey(), entry.getValue());
      }

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

  public abstract String combine(String val1, String val2);

  public abstract int compareTo(String key1, String key2);
}
