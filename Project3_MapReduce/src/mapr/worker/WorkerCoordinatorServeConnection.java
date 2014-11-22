package mapr.worker;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mapr.examples.GrepMapper;
import mapr.examples.GrepReducer;
import mapr.examples.GrepSorter;
import mapr.examples.WordCountMapper;
import mapr.examples.WordCountReducer;
import mapr.examples.WordCountSorter;
import mapr.master.TaskInfo;
import mapr.tasks.Task;

/**
 * Runnable class that handles all connection requests to Worker node.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class WorkerCoordinatorServeConnection implements Runnable {
  /**
   * Socket by which Worker communicates with Master.
   */
  Socket socket;
  /**
   * DFS working directory on the user node.
   */
  String dfsDir;
  /**
   * Hostname of Master node.
   */
  String masterHost;
  /**
   * Port number of Master node.
   */
  int masterPort;
  /**
   * Maximum number of records (lines) in a partition.
   */
  int recordLength;
  /**
   * Name of current worker node.
   */
  String workerName;
  /**
   * Mapping from TaskIDs to TaskThreads.
   */
  ConcurrentHashMap<Integer, TaskThread> tasks;

  public WorkerCoordinatorServeConnection(Socket socket, String dfsDir, String masterHost,
      int masterPort, int recordLength, String workerName,
      ConcurrentHashMap<Integer, TaskThread> tasks) {
    this.socket = socket;
    this.dfsDir = dfsDir;
    this.masterHost = masterHost;
    this.masterPort = masterPort;
    this.recordLength = recordLength;
    this.workerName = workerName;
    this.tasks = tasks;
  }

  /**
   * Creates a Mapper/Reducer/Sorter runnable task from its metadata.
   * 
   * @param task Metadata for a task.
   * @return Runnable class for the task.
   */
  public Task createTask(TaskInfo task) {
    Task runner = null;
    try {
      if (task.getJobType().equals("count")) {
        if (task.getTaskType().equals("map")) {
          runner =
              new WordCountMapper(task.getInput(), task.getOutput(), task.getRecordStart(),
                  task.getRecordEnd(), recordLength, task.getOtherArgs());
        } else if (task.getTaskType().equals("sort")) {
          runner = new WordCountSorter(task.getInput(), task.getOutput(), task.getOtherArgs());
        } else if (task.getTaskType().equals("reduce")) {
          runner = new WordCountReducer(task.getFilenames(), task.getOutput(), task.getOtherArgs());
        }

      } else if (task.getJobType().equals("grep")) {
        if (task.getTaskType().equals("map")) {
          runner =
              new GrepMapper(task.getInput(), task.getOutput(), task.getRecordStart(),
                  task.getRecordEnd(), recordLength, task.getOtherArgs());
        } else if (task.getTaskType().equals("sort")) {
          runner = new GrepSorter(task.getInput(), task.getOutput(), task.getOtherArgs());
        } else if (task.getTaskType().equals("reduce")) {
          runner = new GrepReducer(task.getFilenames(), task.getOutput(), task.getOtherArgs());
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return runner;
  }

  /**
   * Creates a task from its metadata and adds it to running task list.
   * 
   * @param taskInfo Metadata for the task to run.
   * @return <tt>true</tt> iff execution succeeds.
   */
  public boolean executeTask(TaskInfo taskInfo) {
    Task task = createTask(taskInfo);
    if (task == null)
      return false;
    TaskThread taskThread = new TaskThread(taskInfo, new Thread(task));
    TaskThreadRunner runner =
        new TaskThreadRunner(taskThread, task, masterHost, masterPort, workerName, tasks);
    new Thread(runner).start();
    return true;
  }

  /**
   * Helper method for deleting an entire directory recursively.
   * 
   * @param dir Path name to directory.
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
   * Entry point for worker coordinator server.
   */
  @Override
  public void run() {
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    try {
      oos = new ObjectOutputStream(socket.getOutputStream());
      ois = new ObjectInputStream(socket.getInputStream());
      String command = ois.readUTF();
      /* Worker requested to store a partition of file for DFS */
      if (command.equals("newReplica")) {
        String filename = ois.readUTF();
        int fileLen = ois.readInt();
        byte[] contents = new byte[fileLen];
        ois.readFully(contents);
        RandomAccessFile file = new RandomAccessFile(dfsDir + "/" + filename, "rws");
        file.write(contents);
        file.close();

        oos.writeBoolean(true);
        oos.flush();
      }
      /* Worker requested to create a new task. */
      else if (command.equals("newTask")) {
        TaskInfo task = (TaskInfo) ois.readObject();
        boolean success = executeTask(task);
        oos.writeBoolean(success);
        oos.flush();
      }
      /* Worker responds to handshake request. */
      else if (command.equals("ping")) {
        oos.writeUTF("pong");
        oos.flush();
      }
      /* Worker requested to shut down. */
      else if (command.equals("shutdown")) {
        System.out.println("Received shutdown. Exiting...");
        deleteDir(new File(dfsDir));
        System.exit(-1);
      }

    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
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
