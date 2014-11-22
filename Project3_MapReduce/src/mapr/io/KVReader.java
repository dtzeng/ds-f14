package mapr.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Map;

/**
 * Key-Value file reader class.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class KVReader {
  RandomAccessFile file;

  /**
   * Upon construction, initializes <tt>KVReader</tt> by init'ing a <tt>RandomAccessFile</tt> object
   * for input file.
   * 
   * @param filename Name of the file to read.
   * @throws FileNotFoundException File not found/accessible.
   */
  public KVReader(String filename) throws FileNotFoundException {
    this.file = new RandomAccessFile(filename, "r");
  }

  /**
   * Parses a new line from the input file, and splits it to a <tt>(k,v)</tt> pair (with <tt>v</tt>
   * possibly <tt>null</tt>).
   * 
   * @return <tt>(k,v)</tt>-mapping corresponding to the current line.
   * @throws IOException File I/O exception
   */
  public Map.Entry<String, String> readNextKV() throws IOException {
    String read = file.readLine();
    if (read == null)
      return null;

    String[] parse = read.split(":");
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
