package mapr.examples;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;

import mapr.tasks.Reducer;

/**
 * Created by Derek on 11/17/2014.
 */
public class WordCountReducer extends Reducer {
    public WordCountReducer(List<String> inputFiles, String outputFile,
	    String otherArgs) throws FileNotFoundException {
	super(inputFiles, outputFile, otherArgs);
    }

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
