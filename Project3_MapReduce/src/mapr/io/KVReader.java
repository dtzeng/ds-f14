package mapr.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.Map;

/**
 * Created by Derek on 11/16/2014.
 */
public class KVReader {
    RandomAccessFile file;

    public KVReader(String filename) throws FileNotFoundException {
        this.file = new RandomAccessFile(filename, "r");
    }

    public Map.Entry<String, String> readNextKV() throws IOException {
        String read = file.readLine();
        if(read == null)
            return null;

        String[] parse = read.split(":");
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
