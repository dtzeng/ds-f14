package mapr.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by Derek on 11/17/2014.
 */
public class KVWriter {
    RandomAccessFile file;

    public KVWriter(String filename) throws FileNotFoundException {
	this.file = new RandomAccessFile(filename, "rw");
    }

    public void writeKV(String key, String value) throws IOException {
	String val = "";
	if (value != null)
	    val = ":" + value;
	file.writeBytes(key + val + "\n");
    }

    public void close() {
	try {
	    file.close();
	} catch (IOException e) {
	    // ignore
	}
    }
}
