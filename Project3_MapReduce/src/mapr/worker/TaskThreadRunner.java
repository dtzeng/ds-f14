package mapr.worker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import mapr.master.TaskInfo;
import mapr.tasks.Task;

/**
 * Worker thread wrapper for running a MapReduce task and report status to Master.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 */
public class TaskThreadRunner implements Runnable {
  /**
   * Task thread and metadata.
   */
  TaskThread taskThread;
  /**
   * Task metadata.
   */
  TaskInfo info;
  /**
   * Task thread.
   */
  Thread thread;
  /**
   * Task configuration and properties.
   */
  Task task;
  /**
   * Hostname for master node.
   */
  String masterHost;
  /**
   * Port number for master node.
   */
  int masterPort;
  /**
   * Name of current worker node.
   */
  String workerName;
  /**
   * Mapping from Task IDs to Task Threads.
   */
  ConcurrentHashMap<Integer, TaskThread> tasks;


  public TaskThreadRunner(TaskThread taskThread, Task task, String masterHost, int masterPort,
      String workerName, ConcurrentHashMap<Integer, TaskThread> tasks) {
    this.taskThread = taskThread;
    this.info = taskThread.getInfo();
    this.thread = taskThread.getThread();
    this.task = task;
    this.masterHost = masterHost;
    this.masterPort = masterPort;
    this.workerName = workerName;
    this.tasks = tasks;
  }

  /**
   * Notify Master of the job's completion status.
   * 
   * @param success <tt>true</tt> iff the task completed successfully.
   */
  public void notifyMaster(String success) {
    Socket socket = null;
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    try {
      socket = new Socket(masterHost, masterPort);
      oos = new ObjectOutputStream(socket.getOutputStream());
      ois = new ObjectInputStream(socket.getInputStream());
      oos.writeUTF(info.getTaskType() + success);
      oos.writeUTF(workerName);
      oos.writeInt(info.getTaskID());
      oos.writeInt(info.getSourceJobID());

      /* If a Reducer completes, write result to master node. */
      if (info.getTaskType().equals("reduce") && success.equals("Done")) {
        byte[] contents;
        int fileLen = 0;
        try {
          RandomAccessFile file = new RandomAccessFile(info.getOutput(), "r");
          fileLen = (int) file.length();
          contents = new byte[fileLen];
          file.readFully(contents);
          file.close();
        } catch (IOException e) {
          e.printStackTrace();
          return;
        }

        oos.writeInt(fileLen);
        oos.write(contents);
      }

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

  /**
   * Entry point for a task thread <i>wrapper</i>.
   */
  @Override
  public void run() {
    int taskID = info.getTaskID();
    tasks.put(taskID, taskThread);
    thread.start();
    try {
      taskThread.getThread().join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      notifyMaster("Failed");
      return;
    }
    if (task.isSuccess())
      notifyMaster("Done");
    else
      notifyMaster("Failed");
  }
}
