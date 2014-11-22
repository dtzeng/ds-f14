package mapr.examples;

import java.io.IOException;

import mapr.tasks.Mapper;

/**
 * Mapper class for the WordCount Example.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class WordCountMapper extends Mapper {

  public WordCountMapper(String inputFile, String outputFile, int start, int end, int recordLength,
      String otherArgs) throws IOException {
    super(inputFile, outputFile, start, end, recordLength, otherArgs);
  }

  /**
   * For each token in the file, emit the pair <tt>(token,1)</tt>.
   */
  @Override
  public void map(String key, String value) throws Exception {
    String[] words = key.split("\\s+");
    for (String word : words) {
      super.emit(word, "1");
    }
  }
}
