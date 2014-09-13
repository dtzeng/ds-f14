package migratable.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;

/**
 * Created by Derek on 9/2/2014.
 */
public class TransactionalFileOutputStream extends OutputStream implements Serializable {
    private String output;
    private long offset;

    public TransactionalFileOutputStream(String output, boolean append) throws IOException {
        this.output = output;
        if(append) {
            RandomAccessFile file = new RandomAccessFile(output, "rws");
            this.offset = file.length();
            file.close();
        } else {
            this.offset = 0L;
        }
    }

    @Override
    public void write(int b) throws IOException {
        RandomAccessFile file = new RandomAccessFile(output, "rws");
        file.seek(offset);
        file.writeByte(b);
        file.close();
        offset++;
    }
}
