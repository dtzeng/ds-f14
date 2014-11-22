package mapr.master;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main class containing all properties that the Master node in a MapReduce class keep track of.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class MasterCoordinator implements Runnable {
  /**
   * Mapping from worker names to (host,port) pairs.
   */
  ConcurrentHashMap<String, HostPort> workers;
  /**
   * Mapping from MapReduce user names to (host,port) pairs.
   */
  ConcurrentHashMap<String, HostPort> users;
  /**
   * Mapping from file names to the replicas on the worker nodes.
   */
  ConcurrentHashMap<String, FileInfo> files;
  /**
   * Mapping from Job ID to Job Information.
   */
  ConcurrentHashMap<Integer, JobInfo> jobs;
  /**
   * Mapping from worker names their running Tasks.
   */
  ConcurrentHashMap<String, RunningTasks> runningTasks;
  /**
   * Mapping from worker names to their queued Tasks.
   */
  ConcurrentHashMap<String, QueuedTasks> queuedTasks;
  /**
   * Keeps track of all the jobs that have been restarted at least once.
   */
  ArrayList<Integer> restartedJobs;
  /**
   * Concurrency-safe counter objects for Jobs and Tasks.
   */
  IDAssigner jobAssigner, taskAssigner;

  int port, startupTimeout, numWorkers, numUsers, partitionSize, recordLength, replicationFactor;
  /**
   * Maximum number of Mapper, Sorting, and Reduce jobs on a worker node.
   */
  int maxMaps, maxSorts, maxReds;
  /**
   * Frequency for checking available slots to run a job.
   */
  int launchFreq;
  /**
   * Frequency for sending heartbeats to worker nodes.
   */
  int checkFailFreq;
  /**
   * Maximum number of timed-out heartbeats before declaring a node as failed.
   */
  int maxPingRetries;
  public volatile Object startupDone;
  Object lock;

  public MasterCoordinator(int port, int startupTimeout, int numWorkers, int numUsers,
      int partitionSize, int recordLength, int replicationFactor, int maxMaps, int maxSorts,
      int maxReds, int launchFreq, int checkFailFreq, int maxPingRetries, Object startupDone) {
    this.port = port;
    this.startupTimeout = startupTimeout;
    this.numWorkers = numWorkers;
    this.numUsers = numUsers;
    this.startupDone = false;
    this.partitionSize = partitionSize;
    this.recordLength = recordLength;
    this.replicationFactor = replicationFactor;
    this.maxMaps = maxMaps;
    this.maxSorts = maxSorts;
    this.maxReds = maxReds;
    this.launchFreq = launchFreq;
    this.checkFailFreq = checkFailFreq;
    this.maxPingRetries = maxPingRetries;
    this.startupDone = startupDone;
    this.lock = new Object();

    this.workers = new ConcurrentHashMap<String, HostPort>(numWorkers);
    this.users = new ConcurrentHashMap<String, HostPort>(numUsers);
    this.files = new ConcurrentHashMap<String, FileInfo>();
    this.jobs = new ConcurrentHashMap<Integer, JobInfo>();
    this.runningTasks = new ConcurrentHashMap<String, RunningTasks>();
    this.queuedTasks = new ConcurrentHashMap<String, QueuedTasks>();
    this.restartedJobs = new ArrayList<Integer>();
    this.jobAssigner = new IDAssigner();
    this.taskAssigner = new IDAssigner();
  }

  /**
   * Prints a human-readable digest of worker lists.
   */
  public void printWorkers() {
    System.out.format("%15s%30s%n", "Worker", "Host:Port");
    synchronized (lock) {
      for (String key : workers.keySet()) {
        System.out.format("%15s%30s%n", key, workers.get(key).toString());
      }
    }
    System.out.println();
  }

  /**
   * Prints a human-readable digest of user lists.
   */
  public void printUsers() {
    System.out.format("%15s%30s%n", "User", "Host:Port");
    for (String key : users.keySet()) {
      System.out.format("%15s%30s%n", key, users.get(key).toString());
    }
    System.out.println();
  }

  /**
   * Prints a human-readable digest of file lists on DFS.
   */
  public void printFiles() {
    System.out.format("%15s%10s%30s%n", "File", "Records", "Replica Locations");
    synchronized (lock) {
      for (String key : files.keySet()) {
        String records = Integer.toString(files.get(key).getNumRecords());
        String info = files.get(key).toString();
        String[] parts = info.split("\\|");
        for (int x = 0; x < parts.length; x++) {
          System.out.format("%15s%10s%30s%n", x == 0 ? key : "", x == 0 ? records : "",
              Integer.toString(x) + ": " + parts[x]);
        }
      }
    }
    System.out.println();
  }

  /**
   * Prints a human-readable digest of job lists running on framework.
   */
  public void printJobs() {
    System.out.format("%8s%30s%10s%n", "JobID", "Command", "Status");
    synchronized (lock) {
      for (Map.Entry<Integer, JobInfo> entry : jobs.entrySet()) {
        String id = Integer.toString(entry.getKey());
        JobInfo info = entry.getValue();
        String cmd =
            info.getJobType() + " " + info.getRecordStart() + " " + info.getRecordEnd() + " "
                + info.getOutput() + " " + info.getOtherArgs();
        String status = info.getStatus();
        System.out.format("%8s%30s%10s%n", id, cmd, status);
      }
    }
    System.out.println();
  }

  /**
   * Prints a human-readable digest of task lists.
   */
  public void printTasks() {
    System.out.println("Running Tasks:");
    System.out.format("%15s%20s%30s%n", "Worker", "Running Tasks", "Queued Tasks");
    synchronized (lock) {
      for (String worker : workers.keySet()) {
        String rt = runningTasks.get(worker).toString();
        String qt = queuedTasks.get(worker).toString();
        String[] rtSplit = rt.split("\\|");
        String[] qtSplit = qt.split("\\|");
        List<String> rtList = new LinkedList<String>(Arrays.asList(rtSplit));
        List<String> qtList = new LinkedList<String>(Arrays.asList(qtSplit));
        rtList.removeAll(Arrays.asList("", null));
        qtList.removeAll(Arrays.asList("", null));

        if (rtList.size() == 0 && qtList.size() == 0) {
          System.out.format("%15s%20s%30s%n", worker, "", "");
        } else {
          for (int x = 0; x < rtList.size() || x < qtList.size(); x++) {
            String thisRT = x >= rtList.size() ? "" : rtList.get(x);
            String thisQT = x >= qtList.size() ? "" : qtList.get(x);
            System.out.format("%15s%20s%30s%n", x == 0 ? worker : "", thisRT, thisQT);
          }
        }
      }
    }
  }

  /**
   * Prints help message for user of Master CLI.
   */
  public void printHelp() {
    System.out.println("jobs: See the list of running jobs");
    System.out.println("tasks: See the list of running tasks");
    System.out.println("files: List the filenames on the DFS");
    System.out.println("workers: List the workers in the facility");
    System.out.println("users: List the current users");
    System.out.println("help: Prints this help message");
    System.out.println("shutdown: Shuts down the facility");
  }

  /**
   * Entry point for Master Coordinator.
   */
  @Override
  public void run() {
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(port);
    } catch (Exception e) {
      System.out.println("Port not valid or already in use.");
      System.exit(-1);
    }

    /* Wait for workers until user-defined timeout. */
    Timer timeout = new Timer(startupTimeout);
    try {
      serverSocket.setSoTimeout(2);
      new Thread(timeout).start();
    } catch (SocketException e) {
      e.printStackTrace();
    }
    boolean flag = true;
    while (flag) {
      Socket clientSocket = null;
      try {
        clientSocket = serverSocket.accept();
        new Thread(new MasterCoordinatorStartup(clientSocket, workers, runningTasks, queuedTasks))
            .start();
      } catch (SocketTimeoutException e) {
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        flag = !timeout.done && (workers.size() != numWorkers);
      }
    }

    /* Check if start-up succeeded */
    if (workers.size() == 0) {
      System.out.println("No workers established within timeout. Exiting...");
      if (!serverSocket.isClosed()) {
        try {
          serverSocket.close();
        } catch (IOException e) {
        }
      }
      System.exit(-1);
    }
    synchronized (startupDone) {
      startupDone.notifyAll();
    }

    /* Initialize and start up cluster failure checker. */
    MasterCoordinatorCheckFailure checkFailure =
        new MasterCoordinatorCheckFailure(workers, users, files, jobs, runningTasks, queuedTasks,
            restartedJobs, jobAssigner, taskAssigner, partitionSize, checkFailFreq, maxPingRetries,
            lock);
    new Thread(checkFailure).start();

    /* Initialize and start up task scheduler (with failure checking). */
    MasterCoordinatorLaunchTasks launcher =
        new MasterCoordinatorLaunchTasks(workers, files, jobs, runningTasks, queuedTasks,
            restartedJobs, jobAssigner, taskAssigner, maxMaps, maxSorts, maxReds, launchFreq,
            partitionSize, lock);
    new Thread(launcher).start();

    /* Main loop that receives and processes user/worker requests. */
    try {
      serverSocket.setSoTimeout(0);
    } catch (SocketException e) {
      e.printStackTrace();
    }
    while (true) {
      Socket clientSocket = null;
      try {
        clientSocket = serverSocket.accept();
        MasterCoordinatorServeConnection serve =
            new MasterCoordinatorServeConnection(clientSocket, workers, users, files, jobs,
                runningTasks, queuedTasks, restartedJobs, jobAssigner, taskAssigner, partitionSize,
                recordLength, replicationFactor, maxMaps, maxSorts, maxReds, lock);
        new Thread(serve).start();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Parses user command from Master CLI.
   * 
   * @param cmd Command-line input for Master node.
   * @return <tt>true</tt> iff the command is <tt>quit</tt>.
   */
  public boolean parseCommand(String cmd) {
    if (cmd == null) {
      return false;
    }

    String[] splits = cmd.split("\\s+");

    if (cmd.equals("") || splits.length == 0) {
      return false;
    } else if (splits[0].equals("workers")) {
      printWorkers();
    } else if (splits[0].equals("users")) {
      printUsers();
    } else if (splits[0].equals("files")) {
      printFiles();
    } else if (splits[0].equals("jobs")) {
      printJobs();
    } else if (splits[0].equals("tasks")) {
      printTasks();
    } else if (splits[0].equals("help")) {
      printHelp();
    } else if (splits[0].equals("shutdown")) {
      for (HostPort worker : workers.values()) {
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
          socket = new Socket(worker.getHost(), worker.getPort());
          oos = new ObjectOutputStream(socket.getOutputStream());
          ois = new ObjectInputStream(socket.getInputStream());
          oos.writeUTF("shutdown");
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
      }

      for (HostPort user : users.values()) {
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
          socket = new Socket(user.getHost(), user.getPort());
          oos = new ObjectOutputStream(socket.getOutputStream());
          ois = new ObjectInputStream(socket.getInputStream());
          oos.writeUTF("shutdown");
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
      }
      return true;
    } else {
      System.out.println("'" + splits[0] + "' is not a recognized command");
    }
    return false;
  }

  /**
   * Entry point for the MapReduce master CLI facility.
   * 
   * @param args User CLI parameters
   */
  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Please provide a config file.");
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

    String masterHost = prop.getProperty("master.host");
    String masterPort = prop.getProperty("master.port");
    String startupTimeout = prop.getProperty("master.startup.timeout");
    String numWorkers = prop.getProperty("num.workers");
    String numUsers = prop.getProperty("num.users");
    String partitionSize = prop.getProperty("partition.size");
    String recordLength = prop.getProperty("record.length");
    String replicationFactor = prop.getProperty("replication.factor");

    String maxMaps = prop.getProperty("max.maps.per.host");
    String maxSorts = prop.getProperty("max.sorts.per.host");
    String maxReds = prop.getProperty("max.reds.per.host");

    String launchFreq = prop.getProperty("task.launch.frequency");
    String checkFailFreq = prop.getProperty("check.failure.frequency");
    String maxPingRetries = prop.getProperty("max.ping.retries");

    /* Verify properties */
    if (masterHost == null) {
      System.out.println("Please specify a 'master.host' in config file.");
      return;
    }
    if (masterPort == null) {
      System.out.println("Please specify a 'master.port' in config file.");
      return;
    }
    if (startupTimeout == null) {
      System.out.println("Please specify a 'master.startup.timeout' in config file.");
      return;
    }
    if (numWorkers == null) {
      System.out.println("Please specify a 'num.workers' in config file.");
      return;
    }
    if (numUsers == null) {
      System.out.println("Please specify a 'num.users' in config file.");
      return;
    }
    if (partitionSize == null) {
      System.out.println("Please specify a 'partition.size' in config file.");
      return;
    }
    if (recordLength == null) {
      System.out.println("Please specify a 'record.length' in config file.");
      return;
    }
    if (replicationFactor == null) {
      System.out.println("Please specify a 'replication.factor' in config file.");
      return;
    }
    if (maxMaps == null) {
      System.out.println("Please specify a 'max.maps.per.host' in config file.");
      return;
    }
    if (maxSorts == null) {
      System.out.println("Please specify a 'max.sorts.per.host' in config file.");
      return;
    }
    if (maxReds == null) {
      System.out.println("Please specify a 'max.reds.per.host' in config file.");
      return;
    }
    if (launchFreq == null) {
      System.out.println("Please specify a 'task.launch.frequency' in config file.");
      return;
    }
    if (checkFailFreq == null) {
      System.out.println("Please specify a 'check.failure.frequency' in config file.");
      return;
    }
    if (maxPingRetries == null) {
      System.out.println("Please specify a 'max.ping.retries' in config file.");
      return;
    }
    if (!masterHost.equals(hostname)) {
      System.out.println("'master.host' does not match hostname.");
      return;
    }

    /* Start up master node coordinator and wait for workers */
    Object lock = new Object();
    MasterCoordinator coordinator =
        new MasterCoordinator(Integer.parseInt(masterPort), Integer.parseInt(startupTimeout),
            Integer.parseInt(numWorkers), Integer.parseInt(numUsers),
            Integer.parseInt(partitionSize), Integer.parseInt(recordLength),
            Integer.parseInt(replicationFactor), Integer.parseInt(maxMaps),
            Integer.parseInt(maxSorts), Integer.parseInt(maxReds), Integer.parseInt(launchFreq),
            Integer.parseInt(checkFailFreq), Integer.parseInt(maxPingRetries), lock);
    Thread t = new Thread(coordinator);
    t.start();
    System.out.println("Waiting for workers to connect (timeout: " + startupTimeout + ")...");
    synchronized (lock) {
      try {
        lock.wait();
      } catch (InterruptedException e) {
        System.out.println("Threading error. Exiting...");
        System.exit(-1);
      }
    }
    String workersConnected = Integer.toString(coordinator.workers.size());
    if (coordinator.workers.size() != Integer.parseInt(numWorkers)) {
      System.out.println("Not all workers connected, starting with " + workersConnected
          + " workers.");
    } else {
      System.out.println("All workers connected, starting facility");
    }

    /* Worker handshakes finished. Start up Master CLI. */
    Scanner scan = new Scanner(System.in);
    while (true) {
      System.out.print("master@" + masterPort + " > ");
      String input = scan.nextLine();
      boolean quit = coordinator.parseCommand(input);
      if (quit)
        break;
    }
    scan.close();
    System.exit(0);

  }
}
