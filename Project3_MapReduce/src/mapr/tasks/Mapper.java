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
  RecordReader reader;
  KVWriter writer;
  String otherArgs;
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

  public void emit(String key, String value) throws IOException {
    writer.writeKV(key, value);
  }

  @Override
  public void run() {
    try {
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

  public abstract void map(String key, String value) throws Exception;
}
