package migratable.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;

/**
 * Created by Derek on 9/2/2014.
 */
public class TransactionalFileInputStream extends InputStream implements Serializable {
    private String input;
    private long offset;

    public TransactionalFileInputStream(String input) {
        this.input = input;
        this.offset = 0L;
    }

    @Override
    public int read() throws IOException {
        RandomAccessFile file = new RandomAccessFile(input, "r");
        file.seek(offset);
        int next = file.read();
        if(next != -1) {
            offset++;
        }
        file.close();
        return next;
    }
}
