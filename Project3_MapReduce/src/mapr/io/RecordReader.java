package mapr.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Map;

/**
 * Used by Mappers to read specific records efficiently by jumping to calculated file offsets.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 */
public class RecordReader {
  RandomAccessFile file;
  int recordLength, startRecord, recordsLeft;

  /**
   * Initialize a RecordReader based on the parameters on the chunk of records.
   * 
   * @param filename Name of the file with input records.
   * @param startRecord Index for the starting record.
   * @param endRecord Index for the ending record.
   * @param recordLength Length of each record.
   * @throws IOException File I/O exception
   */
  public RecordReader(String filename, int startRecord, int endRecord, int recordLength)
      throws IOException {
    this.file = new RandomAccessFile(filename, "r");
    this.recordLength = recordLength;
    this.startRecord = startRecord;
    this.recordsLeft = endRecord - startRecord + 1;
    this.file.seek((recordLength + 1) * startRecord);
  }

  /**
   * Checks if current RecordReader has more records to read.
   * 
   * @return <tt>true</tt> iff current RecordReader has more lines.
   */
  public boolean hasNextRecord() {
    return recordsLeft != 0;
  }

  /**
   * Parses a new line from the input record chunk (if available), and splits it to a <tt>(k,v)</tt>
   * pair (with <tt>v</tt> possibly <tt>null</tt>).
   * 
   * @return <tt>null</tt> if current chunk of records has no more remaining lines; otherwise,
   *         <tt>(k,v)</tt> -mapping corresponding to the current line.
   * 
   * @throws IOException File I/O exception
   */
  public Map.Entry<String, String> readNextRecord() throws IOException {
    if (recordsLeft == 0)
      return null;
    String read = file.readLine();
    String[] parse = read.split(":");
    recordsLeft--;
    if (parse.length < 2)
      return new AbstractMap.SimpleEntry<String, String>(parse[0], null);
    return new AbstractMap.SimpleEntry<String, String>(parse[0], parse[1]);
  }

  /**
   * Clean up the file descriptor in use.
   */
  public void close() {
    try {
      file.close();
    } catch (IOException e) {
      // ignore
    }
  }
}
