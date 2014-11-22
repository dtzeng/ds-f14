package mapr.examples;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;

import mapr.tasks.Reducer;

/**
 * Reducer class for the Grep Example.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class GrepReducer extends Reducer {
  public GrepReducer(List<String> inputFiles, String outputFile, String otherArgs)
      throws FileNotFoundException {
    super(inputFiles, outputFile, otherArgs);
  }

  /**
   * For each token (<tt>key</tt>) in the intermediate file, return n times of the key itself, where
   * n is the number of the times it appears in the intermediate files.
   */
  @Override
  public void reduce(String key, Iterator<String> values) throws Exception {
    int sum = 0;
    while (values.hasNext()) {
      sum += Integer.parseInt(values.next());
    }
    for (int x = 0; x < sum; x++) {
      super.emit(key, null);
    }
  }

  @Override
  public int compareTo(String key1, String key2) {
    return key1.compareTo(key2);
  }
}
