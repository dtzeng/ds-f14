package mapr.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MapReduce worker command-line interface.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 */
public class WorkerCoordinator implements Runnable {
  /**
   * Name and Host of worker.
   */
  String name, host;
  /**
   * Port number of worker.
   */
  int port;
  /**
   * Local working directory of DFS.
   */
  String dfsDir;
  /**
   * Host name for Master node.
   */
  String masterHost;
  /**
   * Port number of master node.
   */
  int masterPort;
  /**
   * Maximum number of records (lines) in a partition.
   */
  int recordLength;
  /**
   * Mapping from TaskIDs to TaskThreads.
   */
  ConcurrentHashMap<Integer, TaskThread> tasks;

  public WorkerCoordinator(String name, String host, int port, String masterHost, int masterPort,
      int recordLength) {
    this.name = name;
    this.host = host;
    this.port = port;
    this.masterHost = masterHost;
    this.masterPort = masterPort;
    this.recordLength = recordLength;
    this.tasks = new ConcurrentHashMap<Integer, TaskThread>();
    this.dfsDir = name + "-dfs-root";

    File dir = new File(dfsDir);
    deleteDir(dir);
    dir.mkdir();
  }

  /**
   * Removes a directory recursively.
   * 
   * @param dir Path to the directory
   */
  public void deleteDir(File dir) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory())
          deleteDir(f);
        else
          f.delete();
      }
    }
    dir.delete();
  }

  /**
   * Initial handshake with Master node to show existence.
   * 
   * @return <tt>true</tt> iff handshake succeeded.
   */
  public boolean notifyMaster() {
    Socket socket = null;
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    boolean result = false;
    try {
      socket = new Socket(masterHost, masterPort);
      oos = new ObjectOutputStream(socket.getOutputStream());
      ois = new ObjectInputStream(socket.getInputStream());
      oos.writeUTF("newWorker");
      oos.writeUTF(name);
      oos.writeUTF(host);
      oos.writeInt(port);
      oos.flush();

      result = ois.readBoolean();
    } catch (Exception e) {
      // ignore
    } finally {
      try {
        if (oos != null)
          oos.close();
        if (ois != null)
          ois.close();
        if (socket != null && !socket.isClosed())
          socket.close();
      } catch (Exception e) {
        // ignore
      }
    }
    return result;
  }

  /**
   * Entry point for Worker Coordinator.
   */
  @Override
  public void run() {
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(port);
    } catch (Exception e) {
      System.out.println("Port not valid or already in use. Exiting...");
      System.exit(-1);
    }

    /* Handles requests from Master one per thread */
    while (true) {
      Socket clientSocket = null;
      try {
        clientSocket = serverSocket.accept();
        WorkerCoordinatorServeConnection server =
            new WorkerCoordinatorServeConnection(clientSocket, dfsDir, masterHost, masterPort,
                recordLength, name, tasks);
        new Thread(server).start();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Main entry point for starting worker instane in MapReduce.
   * 
   * @param args [<tt>ConfigFilePath</tt>, <tt>WorkerName</tt>]
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Please provide a config file and worker name.");
      return;
    }

    /* Load config file */
    Properties prop = new Properties();
    try {
      InputStream inputStream = new FileInputStream(args[0]);
      prop.load(inputStream);
    } catch (IOException e) {
      System.out.println("Error occurred when reading config file.");
      return;
    }

    String hostname = null;
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
      return;
    }

    String workerHost = prop.getProperty(args[1] + ".host");
    String workerPort = prop.getProperty(args[1] + ".port");
    String masterHost = prop.getProperty("master.host");
    String masterPort = prop.getProperty("master.port");
    String recordLength = prop.getProperty("record.length");

    /* Verify properties */
    if (workerHost == null) {
      System.out.println("Please specify a '" + args[1] + ".host' in config file.");
      return;
    }
    if (workerPort == null) {
      System.out.println("Please specify a '" + args[1] + ".port' in config file.");
      return;
    }
    if (masterHost == null) {
      System.out.println("Please specify a 'master.host' in config file.");
      return;
    }
    if (masterPort == null) {
      System.out.println("Please specify a 'master.port' in config file.");
      return;
    }
    if (recordLength == null) {
      System.out.println("Please specify a 'record.length' in config file.");
      return;
    }
    if (!workerHost.equals(hostname)) {
      System.out.println("'" + args[1] + ".host' does not match hostname.");
      return;
    }

    // Notify master and start coordinator
    WorkerCoordinator coordinator =
        new WorkerCoordinator(args[1], workerHost, Integer.parseInt(workerPort), masterHost,
            Integer.parseInt(masterPort), Integer.parseInt(recordLength));
    if (coordinator.notifyMaster()) {
      System.out.println("Successful connection to facility master established.");
    } else {
      System.out
          .println("Worker already exists or connection to facility master failed. Exiting...");
      return;
    }

    coordinator.run();
  }
}
