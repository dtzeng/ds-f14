package mapr.user;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;

/**
 * Created by Derek on 11/12/2014.
 */
public class UserCoordinatorServeConnection implements Runnable {
    Socket socket;
    StringBuilder shutdown;
    String dfsRoot;

    public UserCoordinatorServeConnection(Socket socket, StringBuilder shutdown, String dfsRoot) {
        this.socket = socket;
        this.shutdown = shutdown;
        this.dfsRoot = dfsRoot;
    }

    @Override
    public void run() {
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());
            String command = ois.readUTF();
            if(command.equals("shutdown")) {
                shutdown.append(" (DISCONNECTED)");
            }
            else if(command.equals("reduceDone")) {
                int fileLen = ois.readInt();
                String output = ois.readUTF();
                byte[] contents = new byte[fileLen];
                String worker = ois.readUTF();
                String jobNo = ois.readUTF();
                ois.readFully(contents);

                String result = dfsRoot + "/" + output + "_" + jobNo + "_" + worker;

                RandomAccessFile file = new RandomAccessFile(result, "rws");
                file.setLength(0);
                file.write(contents);
                file.close();
            }
            else if(command.equals("ping")) {
                oos.writeUTF("pong");
                oos.flush();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(oos != null) oos.close();
                if(ois != null) ois.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
