package mapr.examples;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;

import mapr.tasks.Reducer;

/**
 * Reducer class for the WordCount Example.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class WordCountReducer extends Reducer {
  public WordCountReducer(List<String> inputFiles, String outputFile, String otherArgs)
      throws FileNotFoundException {
    super(inputFiles, outputFile, otherArgs);
  }

  /**
   * For each token (key), simply add up its corresponding values, and emit the
   * <tt>(token, sum)</tt> pair.
   */
  @Override
  public void reduce(String key, Iterator<String> values) throws Exception {
    int sum = 0;
    while (values.hasNext())
      sum += Integer.parseInt(values.next());
    super.emit(key, Integer.toString(sum));
  }

  @Override
  public int compareTo(String key1, String key2) {
    return key1.compareTo(key2);
  }
}
