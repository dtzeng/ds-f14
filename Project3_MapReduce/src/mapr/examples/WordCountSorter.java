package mapr.examples;

import java.io.FileNotFoundException;

import mapr.tasks.Sorter;

/**
 * Created by Derek on 11/17/2014.
 */
public class WordCountSorter extends Sorter {
    public WordCountSorter(String inputFile, String outputFile, String otherArgs)
	    throws FileNotFoundException {
	super(inputFile, outputFile, otherArgs);
    }

    @Override
    public String combine(String val1, String val2) {
	if (val1 == null && val2 == null)
	    return "0";
	else if (val1 == null)
	    return val2;
	else if (val2 == null)
	    return val1;
	else
	    return Integer.toString(Integer.parseInt(val1)
		    + Integer.parseInt(val2));
    }

    @Override
    public int compareTo(String key1, String key2) {
	return key1.compareTo(key2);
    }
}
