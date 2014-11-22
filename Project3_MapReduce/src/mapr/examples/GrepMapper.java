package mapr.examples;

import java.io.IOException;

import mapr.tasks.Mapper;

/**
 * Mapper class for the Grep Example.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class GrepMapper extends Mapper {
  public GrepMapper(String inputFile, String outputFile, int start, int end, int recordLength,
      String otherArgs) throws IOException {
    super(inputFile, outputFile, start, end, recordLength, otherArgs);
  }

  /**
   * Parses a line (<tt>key</tt>) for the first user-supplied argument. If the current line contains
   * the query token, emit <tt>(line, 1)</tt> pair.
   */
  @Override
  public void map(String key, String value) throws Exception {
    String[] split = super.getOtherArgs().split("\\s+");
    if (split.length < 1) {
      throw new Exception("Not enough arguments provided for grep");
    }
    /* Obtain from user arguments the query string. */
    String match = split[0];

    if (key.contains(match)) {
      super.emit(key, "1");
    }
  }
}
