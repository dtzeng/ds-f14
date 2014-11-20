package mapr.master;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Derek on 11/12/2014.
 */
public class MasterCoordinator implements Runnable{
    ConcurrentHashMap<String, HostPort> workers;
    ConcurrentHashMap<String, HostPort> users;
    ConcurrentHashMap<String, FileInfo> files;
    ConcurrentHashMap<Integer, JobInfo> jobs;
    ConcurrentHashMap<String, RunningTasks> runningTasks;
    ConcurrentHashMap<String, QueuedTasks> queuedTasks;
    ArrayList<Integer> restartedJobs;
    IDAssigner jobAssigner, taskAssigner;

    int port, startupTimeout, numWorkers, numUsers, partitionSize, recordLength, replicationFactor;
    int maxMaps, maxSorts, maxReds;
    int launchFreq, checkFailFreq, maxPingRetries;
    public volatile Object startupDone;
    Object lock;

    public MasterCoordinator(int port, int startupTimeout, int numWorkers, int numUsers,
                             int partitionSize, int recordLength, int replicationFactor,
                             int maxMaps, int maxSorts, int maxReds,
                             int launchFreq, int checkFailFreq, int maxPingRetries, Object startupDone) {
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

    public void printWorkers() {
        System.out.format("%15s%30s%n", "Worker", "Host:Port");
        synchronized (lock) {
            for (String key : workers.keySet()) {
                System.out.format("%15s%30s%n", key, workers.get(key).toString());
            }
        }
        System.out.println();
    }

    public void printUsers() {
        System.out.format("%15s%30s%n", "User", "Host:Port");
        for(String key: users.keySet()) {
            System.out.format("%15s%30s%n", key, users.get(key).toString());
        }
        System.out.println();
    }

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

    public void printJobs() {
        System.out.format("%8s%30s%10s%n", "JobID", "Command", "Status");
        synchronized (lock) {
            for(Map.Entry<Integer, JobInfo> entry : jobs.entrySet()) {
                String id = Integer.toString(entry.getKey());
                JobInfo info = entry.getValue();
                String cmd = info.getJobType() + " " +
                             info.getRecordStart() + " " +
                             info.getRecordEnd() + " " +
                             info.getOutput() + " " +
                             info.getOtherArgs();
                String status = info.getStatus();
                System.out.format("%8s%30s%10s%n", id, cmd, status);
            }

            // Remove finished/failed jobs
            /*Iterator<Map.Entry<Integer, JobInfo>> iterator = jobs.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry<Integer, JobInfo> next = iterator.next();
                String status = next.getValue().getStatus();
                if(status.equals("FINISHED") || status.equals("FAILED") || status.contains("RESTARTED")) {
                    iterator.remove();
                }
            }*/
        }
        System.out.println();
    }

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

    public void printHelp() {
        System.out.println("jobs: See the list of running jobs");
        System.out.println("files: List the filenames on the DFS");
        System.out.println("workers: List the workers in the facility");
        System.out.println("users: List the current users");
        System.out.println("help: Prints this help message");
        System.out.println("shutdown: Shuts down the facility");
    }

    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
        } catch (Exception e) {
            System.out.println("Port not valid or already in use.");
            System.exit(-1);
        }

        // Set timeout and wait for workers
        Timer timeout = new Timer(startupTimeout);
        try {
            serverSocket.setSoTimeout(2);
            new Thread(timeout).start();
        } catch (SocketException e) {
            e.printStackTrace();
        }
        boolean flag = true;
        while(flag) {
            Socket clientSocket = null;
            try {
                clientSocket = serverSocket.accept();
                new Thread(new MasterCoordinatorStartup(clientSocket, workers, runningTasks, queuedTasks)).start();
            } catch (SocketTimeoutException e) {
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                flag = !timeout.done && (workers.size() != numWorkers);
            }
        }

        // Check if startup succeeded
        if(workers.size() == 0) {
            System.out.println("No workers established within timeout. Exiting...");
            if(!serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                }
            }
            System.exit(-1);
        }
        synchronized(startupDone) {
            startupDone.notifyAll();
        }

        // Start failure checker
        MasterCoordinatorCheckFailure checkFailure =
                new MasterCoordinatorCheckFailure(workers, users, files, jobs, runningTasks, queuedTasks,
                                                  restartedJobs,
                                                  jobAssigner, taskAssigner, partitionSize,
                                                  checkFailFreq, maxPingRetries, lock);
        new Thread(checkFailure).start();

        // Start task launcher
        MasterCoordinatorLaunchTasks launcher =
                new MasterCoordinatorLaunchTasks(workers, files, jobs, runningTasks, queuedTasks,
                                                 restartedJobs, jobAssigner, taskAssigner,
                                                 maxMaps, maxSorts, maxReds, launchFreq, partitionSize, lock);
        new Thread(launcher).start();

        // Reset timeout and handle connections normally
        try {
            serverSocket.setSoTimeout(0);
        } catch (SocketException e) {
            e.printStackTrace();
        }
        while(true) {
            Socket clientSocket = null;
            try {
                clientSocket = serverSocket.accept();
                MasterCoordinatorServeConnection serve =
                        new MasterCoordinatorServeConnection(clientSocket,
                                                             workers, users, files, jobs, runningTasks, queuedTasks,
                                                             restartedJobs, jobAssigner, taskAssigner,
                                                             partitionSize, recordLength, replicationFactor,
                                                             maxMaps, maxSorts, maxReds, lock);
                new Thread(serve).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean parseCommand(String cmd) {
        if(cmd == null) {
            return false;
        }

        String[] splits = cmd.split("\\s+");

        if(cmd.equals("") || splits.length == 0) {
            return false;
        }
        else if(splits[0].equals("workers")) {
            printWorkers();
        }
        else if(splits[0].equals("users")) {
            printUsers();
        }
        else if(splits[0].equals("files")) {
            printFiles();
        }
        else if(splits[0].equals("jobs")) {
            printJobs();
        }
        else if(splits[0].equals("tasks")) {
            printTasks();
        }
        else if(splits[0].equals("help")) {
            printHelp();
        }
        else if(splits[0].equals("shutdown")) {
            for(HostPort worker: workers.values()) {
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
                        if (oos != null) oos.close();
                        if (ois != null) ois.close();
                        if (socket != null && !socket.isClosed()) socket.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }

            for(HostPort user: users.values()) {
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
                        if (oos != null) oos.close();
                        if (ois != null) ois.close();
                        if (socket != null && !socket.isClosed()) socket.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
            return true;
        }
        else {
            System.out.println("'" + splits[0] + "' is not a recognized command");
        }
        return false;
    }

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Please provide a config file.");
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

        // Verify properties
        if(masterHost == null) {
            System.out.println("Please specify a 'master.host' in config file.");
            return;
        }
        if(masterPort == null) {
            System.out.println("Please specify a 'master.port' in config file.");
            return;
        }
        if(startupTimeout == null) {
            System.out.println("Please specify a 'master.startup.timeout' in config file.");
            return;
        }
        if(numWorkers == null) {
            System.out.println("Please specify a 'num.workers' in config file.");
            return;
        }
        if(numUsers == null) {
            System.out.println("Please specify a 'num.users' in config file.");
            return;
        }
        if(partitionSize == null) {
            System.out.println("Please specify a 'partition.size' in config file.");
            return;
        }
        if(recordLength == null) {
            System.out.println("Please specify a 'record.length' in config file.");
            return;
        }
        if(replicationFactor == null) {
            System.out.println("Please specify a 'replication.factor' in config file.");
            return;
        }
        if(maxMaps == null) {
            System.out.println("Please specify a 'max.maps.per.host' in config file.");
            return;
        }
        if(maxSorts == null) {
            System.out.println("Please specify a 'max.sorts.per.host' in config file.");
            return;
        }
        if(maxReds == null) {
            System.out.println("Please specify a 'max.reds.per.host' in config file.");
            return;
        }
        if(launchFreq == null) {
            System.out.println("Please specify a 'task.launch.frequency' in config file.");
            return;
        }
        if(checkFailFreq == null) {
            System.out.println("Please specify a 'check.failure.frequency' in config file.");
            return;
        }
        if(maxPingRetries == null) {
            System.out.println("Please specify a 'max.ping.retries' in config file.");
            return;
        }
        if(!masterHost.equals(hostname)) {
            System.out.println("'master.host' does not match hostname.");
            return;
        }

        // Start master and wait for workers
        Object lock = new Object();
        MasterCoordinator coordinator = new MasterCoordinator(Integer.parseInt(masterPort),
                                                              Integer.parseInt(startupTimeout),
                                                              Integer.parseInt(numWorkers),
                                                              Integer.parseInt(numUsers),
                                                              Integer.parseInt(partitionSize),
                                                              Integer.parseInt(recordLength),
                                                              Integer.parseInt(replicationFactor),
                                                              Integer.parseInt(maxMaps),
                                                              Integer.parseInt(maxSorts),
                                                              Integer.parseInt(maxReds),
                                                              Integer.parseInt(launchFreq),
                                                              Integer.parseInt(checkFailFreq),
                                                              Integer.parseInt(maxPingRetries),
                                                              lock);
        Thread t = new Thread(coordinator);
        t.start();
        System.out.println("Waiting for workers to connect (timeout: " + startupTimeout + ")...");
        synchronized(lock) {
            try {
                lock.wait();
            } catch (InterruptedException e) {
                System.out.println("Threading error. Exiting...");
                System.exit(-1);
            }
        }
        String workersConnected = Integer.toString(coordinator.workers.size());
        if(coordinator.workers.size() != Integer.parseInt(numWorkers)) {
            System.out.println("Not all workers connected, starting with " + workersConnected + " workers.");
        }
        else {
            System.out.println("All workers connected, starting facility");
        }

        // Start command line
        Scanner scan = new Scanner(System.in);
        while(true) {
            System.out.print("master@" + masterPort + " > ");
            String input = scan.nextLine();
            boolean quit = coordinator.parseCommand(input);
            if(quit) break;
        }
        System.exit(0);

    }
}
