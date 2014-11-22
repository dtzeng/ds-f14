package migratable.managers;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import migratable.processes.ProcessThread;
import migratable.protocols.ChildToProcess;
import migratable.protocols.ChildToProcessResponse;

/**
 * Created by Derek on 9/6/2014.
 */
public class ChildManager implements Runnable {
  private Map<String, ProcessThread> processes;
  private String masterHost, localHost;
  private int masterPort, localPort;
  private ServerSocket serverSocket;

  public ChildManager(String masterHost, int masterPort) {
    this.masterHost = masterHost;
    this.masterPort = masterPort;
    processes = new ConcurrentHashMap<String, ProcessThread>();

    // Tries to connect to master node. If fails, then exits.
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    try {
      serverSocket = new ServerSocket(0);
      localHost = InetAddress.getLocalHost().getHostName();
      localPort = serverSocket.getLocalPort();
      ChildToProcess connect = new ChildToProcess("newchild", null, localHost, localPort);
      Socket socket = new Socket(masterHost, masterPort);
      oos = new ObjectOutputStream(socket.getOutputStream());
      ois = new ObjectInputStream(socket.getInputStream());
      oos.writeObject(connect);
      oos.flush();
      ChildToProcessResponse response = (ChildToProcessResponse) ois.readObject();
      if (!response.getSuccess())
        throw new Exception("Connection to master node failed.");
      else
        System.out.println("Connection to master node succeeded");
    } catch (Exception e) {
      System.out.println("ChildManager failed to connect to master. Try again.");
      System.exit(-1);
    } finally {
      try {
        if (oos != null)
          oos.close();
        if (ois != null)
          ois.close();
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }
  }

  @Override
  public void run() {
    while (true) {
      Socket clientSocket = null;
      try {
        clientSocket = serverSocket.accept();

        // Launch new thread to serve request
        new Thread(new ChildManagerServeConnection(clientSocket, processes, masterHost, masterPort))
            .start();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Need a host and port as arguments.");
      return;
    }
    String masterHost = args[0];
    int masterPort = Integer.parseInt(args[1]);
    ChildManager cm = new ChildManager(masterHost, masterPort);
    new Thread(cm).start();
  }
}
