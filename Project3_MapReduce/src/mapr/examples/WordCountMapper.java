package mapr.examples;

import mapr.tasks.Mapper;

import java.io.IOException;

/**
 * Created by Derek on 11/17/2014.
 */
public class WordCountMapper extends Mapper {

    public WordCountMapper(String inputFile, String outputFile, int start, int end, int recordLength, String otherArgs) throws IOException {
        super(inputFile, outputFile, start, end, recordLength, otherArgs);
    }

    @Override
    public void map(String key, String value) throws Exception {
        String[] words = key.split("\\s+");
        for(String word: words) {
            super.emit(word, "1");
        }
    }
}
