package mapr.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Key-Value file writer class.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class KVWriter {
  RandomAccessFile file;

  /**
   * Upon construction, initializes <tt>KVReader</tt> by init'ing a <tt>RandomAccessFile</tt> object
   * for output file.
   * 
   * @param filename Name of the file to write to.
   * @throws FileNotFoundException File not found/accessible.
   */
  public KVWriter(String filename) throws FileNotFoundException {
    this.file = new RandomAccessFile(filename, "rw");
  }

  /**
   * Appends the <tt>(key,value)</tt>-record to the end of current file.
   * 
   * @param key Key of current record
   * @param value Value of current record
   * @throws IOException File I/O exception
   */
  public void writeKV(String key, String value) throws IOException {
    String val = "";
    if (value != null)
      val = ":" + value;
    file.writeBytes(key + val + "\n");
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
