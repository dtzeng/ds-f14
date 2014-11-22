package mapr.user;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import mapr.master.FileInfo;
import mapr.master.JobInfo;

/**
 * Created by Derek on 11/12/2014.
 */
public class UserCoordinator implements Runnable {
  String name, host, dfsRoot;
  int port;
  String masterHost;
  int masterPort;
  int partitionSize;
  StringBuilder disconnect;

  public UserCoordinator(String name, String host, int port, String dfsRoot, String masterHost,
      int masterPort, int partitionSize) {
    this.name = name;
    this.host = host;
    this.port = port;
    this.dfsRoot = dfsRoot;

    File dir = new File(dfsRoot);
    deleteDir(dir);
    dir.mkdir();

    this.masterHost = masterHost;
    this.masterPort = masterPort;
    this.partitionSize = partitionSize;
    this.disconnect = new StringBuilder("");
  }

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
   * Handshakes with Master node upon starting.
   * 
   * @return <tt>true</tt> if handshake succeeds.
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
      oos.writeUTF("newUser");
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

  @Override
  public void run() {
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(port);
    } catch (Exception e) {
      System.out.println("Port not valid or already in use. Exiting...");
      System.exit(-1);
    }

    while (true) {
      Socket clientSocket = null;
      try {
        clientSocket = serverSocket.accept();
        new Thread(new UserCoordinatorServeConnection(clientSocket, disconnect, dfsRoot)).start();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void printHelp() {
    System.out.println("jobs: See the list of running jobs");
    System.out.println("files: List the filenames on the DFS");
    System.out.println("upload <filename>: Uploads the file to the DFS");
    System.out.println("count <input> <start> <end> <output>:");
    System.out.println("\tStarts a word count for the given file from records [start, end].");
    System.out.println("\tResults are put into specified output in user's DFS root.");
    System.out.println("grep <input> <start> <end> <output> <match>:");
    System.out
        .println("\tStarts a grep for the given file from records [start, end] with the given match");
    System.out.println("\tResults are put into specified output in user's DFS root.");
    System.out.println("start <map> <sort> <reduce> <input> <start> <end> <output> <otherArgs[]>");
    System.out.println("\tStarts a user defined map reduce job");
    System.out.println("\tResults are put into specified output in user's DFS root.");
    System.out.println("help: Prints this help message");
    System.out.println("quit: Quits this user");
  }

  public boolean parseCommand(String cmd) {
    if (cmd == null) {
      return false;
    }

    String[] splits = cmd.split("\\s+");

    if (cmd.equals("") || splits.length == 0) {
      return false;
    } else if (splits[0].equals("jobs")) {
      Socket socket = null;
      ObjectOutputStream oos = null;
      ObjectInputStream ois = null;
      try {
        socket = new Socket(masterHost, masterPort);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        oos.writeUTF("jobs");
        oos.flush();
        ConcurrentHashMap<Integer, JobInfo> jobs =
            (ConcurrentHashMap<Integer, JobInfo>) ois.readObject();

        System.out.format("%8s%30s%10s%n", "JobID", "Command", "Status");
        for (Map.Entry<Integer, JobInfo> entry : jobs.entrySet()) {
          String id = Integer.toString(entry.getKey());
          JobInfo info = entry.getValue();
          String command =
              info.getJobType() + " " + info.getRecordStart() + " " + info.getRecordEnd() + " "
                  + info.getOutput() + " " + info.getOtherArgs();
          String status = info.getStatus();
          System.out.format("%8s%30s%10s%n", id, command, status);
        }
        System.out.println();
      } catch (Exception e) {
        System.out.println("Failed to connect to master.");
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
    } else if (splits[0].equals("files")) {
      Socket socket = null;
      ObjectOutputStream oos = null;
      ObjectInputStream ois = null;
      try {
        socket = new Socket(masterHost, masterPort);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        oos.writeUTF("files");
        oos.flush();
        ConcurrentHashMap<String, FileInfo> files =
            (ConcurrentHashMap<String, FileInfo>) ois.readObject();

        System.out.format("%15s%10s%30s%n", "File", "Records", "Replica Locations");
        for (String key : files.keySet()) {
          String records = Integer.toString(files.get(key).getNumRecords());
          String info = files.get(key).toString();
          String[] parts = info.split("\\|");
          for (int x = 0; x < parts.length; x++) {
            System.out.format("%15s%10s%30s%n", (x == 0) ? key : "", (x == 0) ? records : "",
                Integer.toString(x) + ": " + parts[x]);
          }
        }
        System.out.println();
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Failed to connect to master.");
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
    } else if (splits[0].equals("upload")) {
      if (splits.length < 2) {
        System.out.println("Not enough arguments. Usage: upload <filename>");
        return false;
      }

      byte[] contents;
      int fileLen = 0;
      RandomAccessFile file = null;
      try {
        file = new RandomAccessFile(splits[1], "r");
        fileLen = (int) file.length();
        contents = new byte[fileLen];
        file.readFully(contents);
        file.close();
      } catch (FileNotFoundException e) {
        System.out.println("File not found.");
        return false;
      } catch (IOException e) {
        System.out.println("Error reading file.");
        return false;
      } finally {
        if (file != null) {
          try {
            file.close();
          } catch (IOException e) {
            // ignore
          }
        }
      }

      Socket socket = null;
      ObjectOutputStream oos = null;
      ObjectInputStream ois = null;
      try {
        socket = new Socket(masterHost, masterPort);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        oos.writeUTF("upload");
        oos.writeUTF(splits[1]);
        oos.writeInt(fileLen);
        oos.write(contents);
        oos.flush();
        boolean success = ois.readBoolean();
        if (success) {
          System.out.println("File replicated successfully.");
        } else {
          System.out.println("File replication failed or already exists.");
        }

      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Connection to master died.");
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
    }
    /* User attempts to re-start with Master node. */
    else if (splits[0].equals("reconnect")) {
      if (disconnect.length() == 0) {
        System.out.println("Already connected to master.");
      } else {
        if (!notifyMaster()) {
          System.out.println("Failed to reconnect.");
        } else {
          System.out.println("Reconnect successful!");
          disconnect.setLength(0);
        }
      }
    } else if (splits[0].equals("count")) {
      if (splits.length < 5) {
        System.out.println("Not enough arguments. Usage: count <input> <start> <end> <output>");
        return false;
      }
      String filename = splits[1];
      int start = 0;
      int end = 0;
      try {
        start = Integer.parseInt(splits[2]);
        end = Integer.parseInt(splits[3]);
      } catch (NumberFormatException e) {
        System.out
            .println("<start> and <end> must be integers. Usage: count <input> <start> <end> <output>");
        return false;
      }
      String output = splits[4];

      if (!(start >= 0 && end >= 0 && start <= end)) {
        System.out.println("<start> and <end> must be non-negative and start <= end");
        return false;
      }

      Socket socket = null;
      ObjectOutputStream oos = null;
      ObjectInputStream ois = null;
      try {
        socket = new Socket(masterHost, masterPort);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        oos.writeUTF("count");
        oos.writeUTF(name);
        oos.writeUTF(filename);
        oos.writeInt(start);
        oos.writeInt(end);
        oos.writeUTF(output);
        oos.flush();

        int jobID = ois.readInt();
        if (jobID >= 0)
          System.out.println("Job started with ID " + Integer.toString(jobID));
        else
          System.out.println("File does not exist, bad range, or job failed to start.");

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
    } else if (splits[0].equals("grep")) {
      if (splits.length < 6) {
        System.out
            .println("Not enough arguments. Usage: grep <input> <start> <end> <output> <match>");
        return false;
      }
      String filename = splits[1];
      int start = 0;
      int end = 0;
      try {
        start = Integer.parseInt(splits[2]);
        end = Integer.parseInt(splits[3]);
      } catch (NumberFormatException e) {
        System.out
            .println("<start> and <end> must be integers. Usage: grep <input> <start> <end> <output> <match>");
        return false;
      }
      String output = splits[4];
      String otherArgs = splits[5];

      if (!(start >= 0 && end >= 0 && start <= end)) {
        System.out.println("<start> and <end> must be non-negative and start <= end");
        return false;
      }

      Socket socket = null;
      ObjectOutputStream oos = null;
      ObjectInputStream ois = null;
      try {
        socket = new Socket(masterHost, masterPort);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        oos.writeUTF("grep");
        oos.writeUTF(name);
        oos.writeUTF(filename);
        oos.writeInt(start);
        oos.writeInt(end);
        oos.writeUTF(output);
        oos.writeUTF(otherArgs);
        oos.flush();

        int jobID = ois.readInt();
        if (jobID >= 0)
          System.out.println("Job started with ID " + Integer.toString(jobID));
        else
          System.out.println("File does not exist, bad range, or job failed to start.");

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
    } else if (splits[0].equals("help")) {
      printHelp();
    } else if (splits[0].equals("quit")) {
      Socket socket = null;
      ObjectOutputStream oos = null;
      ObjectInputStream ois = null;
      try {
        socket = new Socket(masterHost, masterPort);
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        oos.writeUTF("userQuit");
        oos.writeUTF(name);
        oos.flush();
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

      return true;
    } else {
      System.out.println("'" + splits[0] + "' is not a recognized command");
    }
    return false;
  }

  public static void main(String args[]) {
    if (args.length < 2) {
      System.out.println("Please provide a config file and participant name.");
      return;
    }

    // Read config file
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

    String userHost = prop.getProperty(args[1] + ".host");
    String userPort = prop.getProperty(args[1] + ".port");
    String dfsRoot = prop.getProperty(args[1] + ".dfs.root");
    String masterHost = prop.getProperty("master.host");
    String masterPort = prop.getProperty("master.port");
    String partitionSize = prop.getProperty("partition.size");

    // Verify properties
    if (userHost == null) {
      System.out.println("Please specify a '" + args[1] + ".host' in config file.");
      return;
    }
    if (userPort == null) {
      System.out.println("Please specify a '" + args[1] + ".port' in config file.");
      return;
    }
    if (dfsRoot == null) {
      System.out.println("Please specify a '" + args[1] + ".dfs.root' in config file.");
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
    if (partitionSize == null) {
      System.out.println("Please specify a 'partition.size' in config file.");
      return;
    }
    if (!userHost.equals(hostname)) {
      System.out.println("'" + args[1] + ".host' does not match hostname.");
      return;
    }

    // Notify master and start coordinator
    UserCoordinator coordinator =
        new UserCoordinator(args[1], userHost, Integer.parseInt(userPort), dfsRoot, masterHost,
            Integer.parseInt(masterPort), Integer.parseInt(partitionSize));
    if (coordinator.notifyMaster()) {
      System.out.println("Successful connection to facility master established.");
    } else {
      System.out.println("User already exists or connection to facility master failed. Exiting...");
      return;
    }
    Thread coord = new Thread(coordinator);
    coord.start();
    coord.interrupt();

    // Start command line
    Scanner scan = new Scanner(System.in);
    while (true) {
      System.out.print(args[1] + "@" + userPort + coordinator.disconnect.toString() + " > ");
      String input = scan.nextLine();
      boolean quit = coordinator.parseCommand(input);
      if (quit)
        break;
    }
    scan.close();
    System.exit(0);
  }

}
