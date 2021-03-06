package mapr.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runnable class that handles all connection requests to Master node.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class MasterCoordinatorServeConnection implements Runnable {
  /**
   * Socket by which Master communicates with workers and users.
   */
  Socket socket;
  /**
   * Mapping from worker names to (host,port) pairs.
   */
  ConcurrentHashMap<String, HostPort> workers;
  /**
   * Mapping from user names to (host,port) pairs.
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
  IDAssigner jobAssigner, taskAssigner;
  int partitionSize, recordLength, replicationFactor;
  int maxMaps, maxSorts, maxReds;

  Object lock;

  public MasterCoordinatorServeConnection(Socket socket,
      ConcurrentHashMap<String, HostPort> workers, ConcurrentHashMap<String, HostPort> users,
      ConcurrentHashMap<String, FileInfo> files, ConcurrentHashMap<Integer, JobInfo> jobs,
      ConcurrentHashMap<String, RunningTasks> runningTasks,
      ConcurrentHashMap<String, QueuedTasks> queuedTasks, ArrayList<Integer> restartedJobs,
      IDAssigner jobAssigner, IDAssigner taskAssigner, int partitionSize, int recordLength,
      int replicationFactor, int maxMaps, int maxSorts, int maxReds, Object lock) {
    this.socket = socket;
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
    this.recordLength = recordLength;
    this.replicationFactor = replicationFactor;
    this.maxMaps = maxMaps;
    this.maxSorts = maxSorts;
    this.maxReds = maxReds;
    this.lock = lock;
  }

  /**
   * Obtains a random sub-list of worker nodes.
   * 
   * @return Random sublist of workers.
   */
  public List<String> getRandomWorkers() {
    Set<String> keys = workers.keySet();
    List<String> list = new LinkedList<String>(keys);
    Collections.shuffle(list);
    return list.subList(0, Math.min(replicationFactor, list.size()));
  }

  /**
   * Replicates a given file onto DFS.
   * 
   * @param filename Name of the file to be replicated.
   * @param contents File data.
   * @param fileLen Length of file.
   * @return <tt>true</tt> iff replication succeeds.
   */
  public boolean replicateFile(String filename, byte[] contents, int fileLen) {
    if (files.containsKey(filename)) {
      return false;
    }

    int adjRL = recordLength + 1; // adjust for extra newline character
    int partitionSizeBytes = partitionSize * adjRL;

    int numPartitions = (fileLen + partitionSizeBytes - 1) / partitionSizeBytes;
    FileInfo info = new FileInfo(numPartitions, fileLen / adjRL);

    for (int x = 0; x < numPartitions; x++) {
      byte[] part;
      if (x == (numPartitions - 1)) {
        part = Arrays.copyOfRange(contents, x * partitionSizeBytes, fileLen);
      } else {
        part = Arrays.copyOfRange(contents, x * partitionSizeBytes, (x + 1) * partitionSizeBytes);
      }

      for (String worker : getRandomWorkers()) {
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        HostPort hp = workers.get(worker);
        try {
          socket = new Socket(hp.getHost(), hp.getPort());
          oos = new ObjectOutputStream(socket.getOutputStream());
          ois = new ObjectInputStream(socket.getInputStream());
          oos.writeUTF("newReplica");
          oos.writeUTF(filename + "-" + Integer.toString(x));
          oos.writeInt(part.length);
          oos.write(part);
          oos.flush();

          if (!ois.readBoolean()) {
            return false;
          } else {
            info.addReplicaLocation(x, worker);
          }
        } catch (Exception e) {
          e.printStackTrace();
          return false;
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
    }
    files.put(filename, info);
    return true;
  }

  /**
   * Entry point for master coordinator handler.
   */
  @Override
  public void run() {
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    try {
      oos = new ObjectOutputStream(socket.getOutputStream());
      ois = new ObjectInputStream(socket.getInputStream());
      String command = ois.readUTF();
      /* New worker identifies itself */
      if (command.equals("newWorker")) {
        String name = ois.readUTF();
        String host = ois.readUTF();
        int port = ois.readInt();
        HostPort hp = new HostPort(host, port);
        /* If no name conflict, add worker to pool */
        synchronized (lock) {
          if (workers.containsKey(name) || workers.containsValue(hp)) {
            oos.writeBoolean(false);
          } else {
            workers.put(name, new HostPort(host, port));
            runningTasks.put(name, new RunningTasks());
            queuedTasks.put(name, new QueuedTasks());
            oos.writeBoolean(true);
          }
        }
        oos.flush();
      }
      /* New user identifies itself */
      else if (command.equals("newUser")) {
        String name = ois.readUTF();
        String host = ois.readUTF();
        int port = ois.readInt();
        HostPort hp = new HostPort(host, port);
        /* If user is in the list, add to user pool */
        if (users.containsKey(name) || users.containsValue(hp)) {
          oos.writeBoolean(false);
        } else {
          users.put(name, new HostPort(host, port));
          oos.writeBoolean(true);
        }
        oos.flush();
      }
      /* User quits */
      else if (command.equals("userQuit")) {
        String name = ois.readUTF();
        users.remove(name);
      }
      /* User asks for job list */
      else if (command.equals("jobs")) {
        synchronized (lock) {
          oos.writeObject(jobs);
          oos.flush();
        }
      }
      /* User asks for file list */
      else if (command.equals("files")) {
        oos.writeObject(files);
        oos.flush();
      }
      /* User uploads file to DFS */
      else if (command.equals("upload")) {
        String filename = ois.readUTF();
        int fileLen = ois.readInt();
        byte[] contents = new byte[fileLen];
        ois.readFully(contents);

        boolean success = true;
        synchronized (lock) {
          success = replicateFile(filename, contents, fileLen);
        }
        oos.writeBoolean(success);
        oos.flush();
      }
      /* User requests a `count` MapReduce Job */
      else if (command.equals("count")) {
        String user = ois.readUTF();
        String filename = ois.readUTF();
        int start = ois.readInt();
        int end = ois.readInt();
        String output = ois.readUTF();
        synchronized (lock) {
          int jobID = startJob("count", user, filename, start, end, output, "");
          oos.writeInt(jobID);
        }
        oos.flush();
      }
      /* User requests a `grep` MapReduce Job */
      else if (command.equals("grep")) {
        String user = ois.readUTF();
        String filename = ois.readUTF();
        int start = ois.readInt();
        int end = ois.readInt();
        String output = ois.readUTF();
        String otherArgs = ois.readUTF();
        synchronized (lock) {
          int jobID = startJob("grep", user, filename, start, end, output, otherArgs);
          oos.writeInt(jobID);
        }
        oos.flush();
      }
      /* Work sends in mapDone notification */
      else if (command.equals("mapDone")) {
        String workerName = ois.readUTF();
        int taskID = ois.readInt();
        int jobID = ois.readInt();

        synchronized (lock) {
          if (runningTasks.get(workerName) == null || queuedTasks.get(workerName) == null) {
            return;
          }

          runningTasks.get(workerName).finishedMap(taskID);
          queuedTasks.get(workerName).finishedMap(taskID);
          if (jobs.get(jobID) != null) {
            jobs.get(jobID).removeTask(taskID);
          }

          /* Allocate new mapper jobs on free slots. */
          if (runningTasks.get(workerName).numMaps() < maxMaps) {
            while (true) {
              TaskInfo map = queuedTasks.get(workerName).nextMap();
              if (map == null)
                break;

              int nextJobID = map.getSourceJobID();

              if (jobs.get(nextJobID) == null || jobs.get(nextJobID).getStatus().equals("FAILED")
                  || jobs.get(nextJobID).getStatus().contains("RESTARTED")) {
                continue;
              } else if (!sendTaskToWorker(workerName, map)) {
                if (!jobs.get(nextJobID).getStatus().contains("RESTARTED"))
                  jobs.get(nextJobID).setStatus("FAILED");
              } else {
                runningTasks.get(workerName).addMap(map.getTaskID(), map);
                jobs.get(nextJobID).setStatus("RUNNING");
                break;
              }
            }
          }
        }
      }
      /* Work sends in mapFailed notification */
      else if (command.equals("mapFailed")) {
        String workerName = ois.readUTF();
        int taskID = ois.readInt();
        int jobID = ois.readInt();

        synchronized (lock) {
          if (runningTasks.get(workerName) == null || queuedTasks.get(workerName) == null) {
            return;
          }

          runningTasks.get(workerName).finishedMap(taskID);
          queuedTasks.get(workerName).finishedMap(taskID);
          if (jobs.get(jobID) != null) {
            jobs.get(jobID).removeTask(taskID);
            jobs.get(jobID).setStatus("FAILED");
          }
          /* Allocate new mapper jobs on free slots. */
          if (runningTasks.get(workerName).numMaps() < maxMaps) {
            while (true) {
              TaskInfo map = queuedTasks.get(workerName).nextMap();
              if (map == null)
                break;

              int nextJobID = map.getSourceJobID();

              if (jobs.get(nextJobID) == null || jobs.get(nextJobID).getStatus().equals("FAILED")
                  || jobs.get(nextJobID).getStatus().contains("RESTARTED")) {
                continue;
              } else if (!sendTaskToWorker(workerName, map)) {
                if (!jobs.get(nextJobID).getStatus().contains("RESTARTED"))
                  jobs.get(nextJobID).setStatus("FAILED");
              } else {
                runningTasks.get(workerName).addMap(map.getTaskID(), map);
                jobs.get(nextJobID).setStatus("RUNNING");
                break;
              }
            }
          }
        }
      }
      /* Work sends in sortDone notification */
      else if (command.equals("sortDone")) {
        String workerName = ois.readUTF();
        int taskID = ois.readInt();
        int jobID = ois.readInt();

        synchronized (lock) {
          if (runningTasks.get(workerName) == null || queuedTasks.get(workerName) == null) {
            return;
          }

          runningTasks.get(workerName).finishedSort(taskID);
          queuedTasks.get(workerName).finishedSort(taskID);
          if (jobs.get(jobID) != null) {
            jobs.get(jobID).removeTask(taskID);
          }
          /* Allocate new sorter jobs on free slots. */
          if (runningTasks.get(workerName).numSorts() < maxSorts) {
            while (true) {
              TaskInfo sort = queuedTasks.get(workerName).nextSort();
              if (sort == null)
                break;

              int nextJobID = sort.getSourceJobID();

              if (jobs.get(nextJobID) == null || jobs.get(nextJobID).getStatus().equals("FAILED")
                  || jobs.get(nextJobID).getStatus().contains("RESTARTED")) {
                continue;
              } else if (!sendTaskToWorker(workerName, sort)) {
                if (!jobs.get(nextJobID).getStatus().contains("RESTARTED"))
                  jobs.get(nextJobID).setStatus("FAILED");
              } else {
                runningTasks.get(workerName).addSort(sort.getTaskID(), sort);
                jobs.get(nextJobID).setStatus("RUNNING");
                break;
              }
            }
          }
        }
      }
      /* Work sends in sortFailed notification */
      else if (command.equals("sortFailed")) {
        String workerName = ois.readUTF();
        int taskID = ois.readInt();
        int jobID = ois.readInt();

        synchronized (lock) {
          if (runningTasks.get(workerName) == null || queuedTasks.get(workerName) == null) {
            return;
          }

          runningTasks.get(workerName).finishedSort(taskID);
          queuedTasks.get(workerName).finishedSort(taskID);
          if (jobs.get(jobID) != null) {
            jobs.get(jobID).removeTask(taskID);
            jobs.get(jobID).setStatus("FAILED");
          }
          /* Allocate new sorter jobs on free slots. */
          if (runningTasks.get(workerName).numSorts() < maxSorts) {
            while (true) {
              TaskInfo sort = queuedTasks.get(workerName).nextSort();
              if (sort == null)
                break;

              int nextJobID = sort.getSourceJobID();

              if (jobs.get(nextJobID) == null || jobs.get(nextJobID).getStatus().equals("FAILED")
                  || jobs.get(nextJobID).getStatus().contains("RESTARTED")) {
                continue;
              } else if (!sendTaskToWorker(workerName, sort)) {
                if (!jobs.get(nextJobID).getStatus().contains("RESTARTED"))
                  jobs.get(nextJobID).setStatus("FAILED");
              } else {
                runningTasks.get(workerName).addSort(sort.getTaskID(), sort);
                jobs.get(nextJobID).setStatus("RUNNING");
                break;
              }
            }
          }
        }

      }
      /* Work sends in reduceDone notification */
      else if (command.equals("reduceDone")) {
        String workerName = ois.readUTF();
        int taskID = ois.readInt();
        int jobID = ois.readInt();

        synchronized (lock) {
          if (runningTasks.get(workerName) == null) {
            return;
          }

          runningTasks.get(workerName).finishedReduce(taskID);
          if (jobs.get(jobID) != null) {
            jobs.get(jobID).removeTask(taskID);
          } else {
            return;
          }
        }

        int fileLen = ois.readInt();
        byte[] contents = new byte[fileLen];
        ois.readFully(contents);

        HostPort hpUser = null;
        String jobOut = null;
        String jobNo = null;
        synchronized (lock) {
          if (jobs.get(jobID) == null || jobs.get(jobID).getStatus().equals("FAILED")) {
            return;
          } else {
            String user = jobs.get(jobID).getUser();
            hpUser = users.get(user);
            jobOut = jobs.get(jobID).getOutput();
            jobNo = "job" + Integer.toString(jobs.get(jobID).getJobID());
          }
        }
        /* Write reduce output back to user */
        if (hpUser != null) {
          Socket socketUser = null;
          ObjectOutputStream oosUser = null;
          ObjectInputStream oisUser = null;
          try {
            socketUser = new Socket(hpUser.getHost(), hpUser.getPort());
            oosUser = new ObjectOutputStream(socketUser.getOutputStream());
            oisUser = new ObjectInputStream(socketUser.getInputStream());
            oosUser.writeUTF("reduceDone");
            oosUser.writeInt(fileLen);
            oosUser.writeUTF(jobOut);
            oosUser.writeUTF(workerName);
            oosUser.writeUTF(jobNo);
            oosUser.write(contents);
            oosUser.flush();
          } catch (Exception e) {
            synchronized (lock) {
              if (jobs.get(jobID) != null)
                jobs.get(jobID).setStatus("FAILED");
            }
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
        synchronized (lock) {
          if (jobs.get(jobID) != null && !jobs.get(jobID).equals("FAILED")
              && jobs.get(jobID).numTasks() == 0)
            jobs.get(jobID).setStatus("FINISHED");
        }
        /* Allocate new reduce jobs on free slots. */
        synchronized (lock) {
          if (runningTasks.get(workerName).numReduces() < maxReds) {
            while (true) {
              TaskInfo reduce = queuedTasks.get(workerName).nextReduce();
              if (reduce == null)
                break;

              int nextJobID = reduce.getSourceJobID();

              if (jobs.get(nextJobID) == null || jobs.get(nextJobID).getStatus().equals("FAILED")
                  || jobs.get(nextJobID).getStatus().contains("RESTARTED")) {
                continue;
              } else if (!sendTaskToWorker(workerName, reduce)) {
                if (!jobs.get(nextJobID).getStatus().contains("RESTARTED"))
                  jobs.get(nextJobID).setStatus("FAILED");
              } else {
                runningTasks.get(workerName).addReduce(reduce.getTaskID(), reduce);
                jobs.get(nextJobID).setStatus("RUNNING");
                break;
              }
            }
          }
        }
      }
      /* Work sends in reduceFailed notification */
      else if (command.equals("reduceFailed")) {
        String workerName = ois.readUTF();
        int taskID = ois.readInt();
        int jobID = ois.readInt();

        synchronized (lock) {
          if (runningTasks.get(workerName) == null) {
            return;
          }
          runningTasks.get(workerName).finishedReduce(taskID);
          if (jobs.get(jobID) != null) {
            jobs.get(jobID).removeTask(taskID);
            jobs.get(jobID).setStatus("FAILED");
          }

          /* Allocate new reduce jobs on free slots. */
          if (runningTasks.get(workerName).numReduces() < maxReds) {
            while (true) {
              TaskInfo reduce = queuedTasks.get(workerName).nextReduce();
              if (reduce == null)
                break;

              int nextJobID = reduce.getSourceJobID();

              if (jobs.get(nextJobID) == null || jobs.get(nextJobID).getStatus().equals("FAILED")
                  || jobs.get(nextJobID).getStatus().contains("RESTARTED")) {
                continue;
              } else if (!sendTaskToWorker(workerName, reduce)) {
                if (!jobs.get(nextJobID).getStatus().contains("RESTARTED"))
                  jobs.get(nextJobID).setStatus("FAILED");
              } else {
                runningTasks.get(workerName).addReduce(reduce.getTaskID(), reduce);
                jobs.get(nextJobID).setStatus("RUNNING");
                break;
              }
            }
          }
        }
      }

      oos.flush();
    } catch (IOException e) {
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

  /**
   * Delivers a task onto some worker node.
   * 
   * @param worker Name of worker node
   * @param task Metadata for task
   * @return <tt>true</tt> iff the task is sent successfully
   */
  public boolean sendTaskToWorker(String worker, TaskInfo task) {
    Socket socket = null;
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    HostPort hp = workers.get(worker);

    // Check if worker failed before
    if (hp == null) {
      int oldJobID = task.getSourceJobID();
      if (!restartedJobs.contains(oldJobID)) {
        JobInfo oldJob = jobs.get(oldJobID);
        if (oldJob != null && !oldJob.getStatus().equals("FAILED")) {
          int newJobID =
              startJob(oldJob.getJobType(), oldJob.getUser(), oldJob.getInput(),
                  oldJob.getRecordStart(), oldJob.getRecordEnd(), oldJob.getOutput(),
                  oldJob.getOtherArgs());
          oldJob.setStatus("RESTARTED AS JOB " + Integer.toString(newJobID));
          restartedJobs.add(oldJobID);
        }
      }
      return false;
    }

    boolean result = true;
    try {
      socket = new Socket(hp.getHost(), hp.getPort());
      oos = new ObjectOutputStream(socket.getOutputStream());
      ois = new ObjectInputStream(socket.getInputStream());
      oos.writeUTF("newTask");
      oos.writeObject(task);
      oos.flush();
      if (!ois.readBoolean())
        result = false;
    } catch (Exception e) {
      result = false;
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

    /* Reassign tasks for worker if worker failed during connection */
    if (!result) {
      for (FileInfo fileInfo : files.values()) {
        fileInfo.removeWorker(worker);
      }
      queuedTasks.remove(worker);
      runningTasks.get(worker).addTask(task);
      reassignTasks(worker);
      runningTasks.remove(worker);
    }

    return result;
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
  public int startJob(String jobType, String user, String filename, int start, int end,
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
}
