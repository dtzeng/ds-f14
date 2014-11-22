package mapr.user;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;

/**
 * Runnable class that handles all connection requests to User node.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class UserCoordinatorServeConnection implements Runnable {
  /**
   * Socket by which user communicates with Master.
   */
  Socket socket;
  /**
   * Helper string buffer to signal if the user is still connected to the cluster.
   */
  StringBuilder shutdown;
  /**
   * DFS working directory on the user node.
   */
  String dfsRoot;

  public UserCoordinatorServeConnection(Socket socket, StringBuilder shutdown, String dfsRoot) {
    this.socket = socket;
    this.shutdown = shutdown;
    this.dfsRoot = dfsRoot;
  }

  /**
   * Entry point for user coordinator server.
   */
  @Override
  public void run() {
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    try {
      oos = new ObjectOutputStream(socket.getOutputStream());
      ois = new ObjectInputStream(socket.getInputStream());
      String command = ois.readUTF();
      /* If Master shuts down, appends `DISCONECTED` to client prompt. */
      if (command.equals("shutdown")) {
        shutdown.append(" (DISCONNECTED)");
      }
      /* If Master completes a reduce task, write result to local DFS directory. */
      else if (command.equals("reduceDone")) {
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
      /* Respond to heartbeat request. */
      else if (command.equals("ping")) {
        oos.writeUTF("pong");
        oos.flush();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (oos != null)
          oos.close();
        if (ois != null)
          ois.close();
        if (!socket.isClosed())
          socket.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
