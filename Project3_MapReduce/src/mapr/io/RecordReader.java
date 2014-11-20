package mapr.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Map;

/**
 * Created by Derek on 11/12/2014.
 */
public class RecordReader {
    RandomAccessFile file;
    int recordLength, startRecord, recordsLeft;

    public RecordReader(String filename, int startRecord, int endRecord, int recordLength) throws IOException {
        this.file = new RandomAccessFile(filename, "r");
        this.recordLength = recordLength;
        this.startRecord = startRecord;
        this.recordsLeft = endRecord - startRecord + 1;
        this.file.seek((recordLength + 1) * startRecord);
    }

    public boolean hasNextRecord() {
        return recordsLeft != 0;
    }

    public Map.Entry<String, String> readNextRecord() throws IOException {
        if(recordsLeft == 0)
            return null;
        String read = file.readLine();
        String[] parse = read.split(":");
        recordsLeft--;
        if(parse.length < 2)
            return new AbstractMap.SimpleEntry<String, String>(parse[0], null);
        return new AbstractMap.SimpleEntry<String, String>(parse[0], parse[1]);
    }

    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
