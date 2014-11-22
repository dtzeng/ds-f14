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
    public GrepReducer(List<String> inputFiles, String outputFile,
	    String otherArgs) throws FileNotFoundException {
	super(inputFiles, outputFile, otherArgs);
    }

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
