package mapr.tasks;

import java.io.IOException;
import java.util.Map;

import mapr.io.KVWriter;
import mapr.io.RecordReader;

/**
 * Abstract class for Mapper that users can extend based on the MapReduce job.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public abstract class Mapper implements Task {
  /**
   * Reader of original input.
   */
  RecordReader reader;
  /**
   * Writer of intermediate K-V pairs.
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

  public Mapper(String inputFile, String outputFile, int start, int end, int recordLength,
      String otherArgs) throws IOException {
    this.reader = new RecordReader(inputFile, start, end, recordLength);
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
   * @param key Key to emit
   * @param value Value to emit
   * @throws IOException File I/O Exception
   */
  public void emit(String key, String value) throws IOException {
    writer.writeKV(key, value);
  }

  /**
   * Entry point for a mapper task.
   */
  @Override
  public void run() {
    try {
      /* For each record, map it. */
      while (reader.hasNextRecord()) {
        Map.Entry<String, String> read = reader.readNextRecord();
        map(read.getKey(), read.getValue());
      }
    } catch (Exception e) {
      success = false;
      e.printStackTrace();
    } finally {
      reader.close();
      writer.close();
    }
  }

  /**
   * User's rule for mapping a (key,value) pair to intermediate (key,value) pairs.
   * 
   * @param key Input <tt>key</tt>
   * @param value Input <tt>value</tt>
   */
  public abstract void map(String key, String value) throws Exception;
}
