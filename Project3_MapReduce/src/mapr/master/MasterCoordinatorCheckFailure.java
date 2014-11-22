package mapr.master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Background utility that checks for the health of cluster machines.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class MasterCoordinatorCheckFailure implements Runnable {
  /**
   * Mapping from worker names to their (host,port) pairs.
   */
  ConcurrentHashMap<String, HostPort> workers;
  /**
   * Mapping from user names to their (host,port) pairs.
   */
  ConcurrentHashMap<String, HostPort> users;
  /**
   * Mapping from file names on DFS to their metadata.
   */
  ConcurrentHashMap<String, FileInfo> files;
  /**
   * Mapping from job names to their metadata.
   */
  ConcurrentHashMap<Integer, JobInfo> jobs;
  /**
   * Mapping from worker names to their running tasks.
   */
  ConcurrentHashMap<String, RunningTasks> runningTasks;
  /**
   * Mapping from worker names to their queued tasks.
   */
  ConcurrentHashMap<String, QueuedTasks> queuedTasks;
  /**
   * Keeps track of all the jobs that have been restarted at least once.
   */
  ArrayList<Integer> restartedJobs;
  IDAssigner jobAssigner, taskAssigner;
  int partitionSize, checkFailFreq, maxPingRetries;
  Object lock;

  public MasterCoordinatorCheckFailure(ConcurrentHashMap<String, HostPort> workers,
      ConcurrentHashMap<String, HostPort> users, ConcurrentHashMap<String, FileInfo> files,
      ConcurrentHashMap<Integer, JobInfo> jobs,
      ConcurrentHashMap<String, RunningTasks> runningTasks,
      ConcurrentHashMap<String, QueuedTasks> queuedTasks, ArrayList<Integer> restartedJobs,
      IDAssigner jobAssigner, IDAssigner taskAssigner, int partitionSize, int checkFailFreq,
      int maxPingRetries, Object lock) {
    this.workers = workers;
    this.users = users;
    this.files = files;
    this.jobs = jobs;
    this.runningTasks = runningTasks;
    this.queuedTasks = queuedTasks;
    this.restartedJobs = restartedJobs;
    this.jobAssigner = jobAssigner;
    this.taskAssigner = taskAssigner;
    this.partitionSize = partitionSize;
    this.checkFailFreq = checkFailFreq;
    this.maxPingRetries = maxPingRetries;
    this.lock = lock;
  }

  /**
   * Inserts a job to the <tt>QueuedTasks</tt> of a worker node.
   * 
   * @param jobType Type of the job, e.g. `grep` or `wordcount`
   * @param user User node that created the job
   * @param filename Input file name
   * @param start Starting record index
   * @param end Ending record index
   * @param output Output file name
   * @param otherArgs User Additional command-line arguments fed in by the user.
   * @return JobID for new job
   */
  private int startJob(String jobType, String user, String filename, int start, int end,
      String output, String otherArgs) {
    FileInfo info = files.get(filename);
    if (info == null || end >= info.getNumRecords())
      return -1;

    /* Obtain next JobID, construct Job Metadata. */
    int jobID = jobAssigner.getNextJobID();
    JobInfo job =
        new JobInfo(user, jobType, filename, output, otherArgs, "QUEUED", start, end, jobID);

    HashMap<String, TaskInfo> reduces = new HashMap<String, TaskInfo>();

    /* Partition input file by partition size. */
    for (int x = start / partitionSize; x <= end / partitionSize; x++) {
      String worker = files.get(filename).getReplicaLocation(x);

      String replicaFile = filename + "-" + Integer.toString(x);
      int replicaStart = x * partitionSize;
      int replicaEnd = (x + 1) * partitionSize;
      int taskStart = (replicaStart <= start && start < replicaEnd) ? start % partitionSize : 0;
      int taskEnd =
          (replicaStart <= start && start < replicaEnd) ? end % partitionSize : partitionSize - 1;

      /* Pick intermediate and result file names */
      int mapID = taskAssigner.getNextJobID();
      int sortID = taskAssigner.getNextJobID();
      int reduceID = taskAssigner.getNextJobID();
      String mapIn = worker + "-dfs-root/" + replicaFile;
      String mapOut = worker + "-dfs-root/" + replicaFile + "_map" + Integer.toString(mapID);
      String sortOut = worker + "-dfs-root/" + replicaFile + "_sort" + Integer.toString(sortID);
      String reduceOut =
          worker + "-dfs-root/" + replicaFile + "_reduce" + Integer.toString(reduceID);

      /* Create new map task and sort for replica */
      TaskInfo map =
          new TaskInfo("map", mapIn, mapOut, taskStart, taskEnd, mapID, otherArgs, jobID, jobType);
      TaskInfo sort = new TaskInfo("sort", mapOut, sortOut, sortID, mapID, jobID, jobType);

      /* Add map and sort tasks, and update reduce dependencies */
      queuedTasks.get(worker).queueMap(map);
      queuedTasks.get(worker).queueSort(mapID, sort);
      job.addTask(mapID);
      job.addTask(sortID);
      TaskInfo reduce = reduces.get(worker);
      if (reduce == null) {
        TaskInfo newReduce = new TaskInfo("reduce", reduceOut, reduceID, jobID, jobType);
        newReduce.addDependency(sortID);
        newReduce.addFilename(sortOut);
        reduces.put(worker, newReduce);
      } else {
        reduce.addDependency(sortID);
        reduce.addFilename(sortOut);
      }
    }

    /* Add all reduce tasks */
    Iterator<Map.Entry<String, TaskInfo>> iter = reduces.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, TaskInfo> next = iter.next();
      String worker = next.getKey();
      TaskInfo reduce = next.getValue();
      queuedTasks.get(worker).queueReduce(reduce);
      job.addTask(reduce.getTaskID());
    }
    jobs.put(jobID, job);
    return jobID;
  }

  /**
   * Sends a heartbeat to a machine to check health.
   * 
   * @param machine Worker name for the node.
   * @return <tt>true</tt> iff the worker node is healthy.
   */
  private boolean pingMachine(HostPort machine) {
    for (int x = 0; x < maxPingRetries; x++) {
      Socket socket = null;
      ObjectOutputStream oos = null;
      ObjectInputStream ois = null;
      boolean success = false;
      try {
        socket = new Socket(machine.getHost(), machine.getPort());
        oos = new ObjectOutputStream(socket.getOutputStream());
        ois = new ObjectInputStream(socket.getInputStream());
        oos.writeUTF("ping");
        oos.flush();
        success = ois.readUTF().equals("pong");
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
      if (success)
        return true;
    }
    return false;
  }

  /**
   * Re-assigns a job to <i>avoid</i> a certain worker node.
   * 
   * @param worker Name for the worker node.
   */
  private void reassignTasks(String worker) {
    RunningTasks rt = runningTasks.get(worker);
    if (rt == null)
      return;

    ConcurrentHashMap<Integer, TaskInfo> maps = rt.getMaps();
    ConcurrentHashMap<Integer, TaskInfo> sorts = rt.getSorts();
    ConcurrentHashMap<Integer, TaskInfo> reduces = rt.getReduces();

    Iterator<TaskInfo> iter = maps.values().iterator();
    while (iter.hasNext()) {
      TaskInfo taskInfo = iter.next();
      int oldJobID = taskInfo.getSourceJobID();
      if (!restartedJobs.contains(oldJobID)) {
        JobInfo oldJob = jobs.get(oldJobID);
        if (oldJob != null) {
          int newJobID =
              startJob(oldJob.getJobType(), oldJob.getUser(), oldJob.getInput(),
                  oldJob.getRecordStart(), oldJob.getRecordEnd(), oldJob.getOutput(),
                  oldJob.getOtherArgs());
          oldJob.setStatus("RESTARTED AS JOB " + Integer.toString(newJobID));
          restartedJobs.add(oldJobID);
        }
      }
    }

    iter = sorts.values().iterator();
    while (iter.hasNext()) {
      TaskInfo taskInfo = iter.next();
      int oldJobID = taskInfo.getSourceJobID();
      if (!restartedJobs.contains(oldJobID)) {
        JobInfo oldJob = jobs.get(oldJobID);
        if (oldJob != null) {
          int newJobID =
              startJob(oldJob.getJobType(), oldJob.getUser(), oldJob.getInput(),
                  oldJob.getRecordStart(), oldJob.getRecordEnd(), oldJob.getOutput(),
                  oldJob.getOtherArgs());
          oldJob.setStatus("RESTARTED AS JOB " + Integer.toString(newJobID));
          restartedJobs.add(oldJobID);
        }
      }
    }

    iter = reduces.values().iterator();
    while (iter.hasNext()) {
      TaskInfo taskInfo = iter.next();
      int oldJobID = taskInfo.getSourceJobID();
      if (!restartedJobs.contains(oldJobID)) {
        JobInfo oldJob = jobs.get(oldJobID);
        if (oldJob != null) {
          int newJobID =
              startJob(oldJob.getJobType(), oldJob.getUser(), oldJob.getInput(),
                  oldJob.getRecordStart(), oldJob.getRecordEnd(), oldJob.getOutput(),
                  oldJob.getOtherArgs());
          oldJob.setStatus("RESTARTED AS JOB " + Integer.toString(newJobID));
          restartedJobs.add(oldJobID);
        }
      }
    }
  }

  /**
   * Entry point for Master Failure Checker.
   */
  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(checkFailFreq * 1000);
      } catch (InterruptedException e) {
        // ignore
      }
      
      /* Periodically pings all machines */
      synchronized (lock) {
        Iterator<Map.Entry<String, HostPort>> iter = workers.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<String, HostPort> entry = iter.next();
          /* If machine unreachable, remove tasks from it */
          if (!pingMachine(entry.getValue())) {
            for (FileInfo fileInfo : files.values()) {
              fileInfo.removeWorker(entry.getKey());
            }
            queuedTasks.remove(entry.getKey());
            reassignTasks(entry.getKey());
            runningTasks.remove(entry.getKey());
            iter.remove();
          }
        }

        iter = users.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<String, HostPort> entry = iter.next();
          if (!pingMachine(entry.getValue())) {
            iter.remove();
          }
        }
      }
    }
  }
}
