package mapr.worker;

import mapr.master.TaskInfo;

/**
 * Encapsulates a Task thread along with its metadata.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 */
public class TaskThread {
  /**
   * Metadata of a task.
   */
  TaskInfo info;
  /**
   * Actual thread of the task.
   */
  Thread thread;

  public TaskThread(TaskInfo info, Thread thread) {
    this.info = info;
    this.thread = thread;
  }

  public TaskInfo getInfo() {
    return info;
  }

  public Thread getThread() {
    return thread;
  }

  public void setInfo(TaskInfo info) {
    this.info = info;
  }

  public void setThread(Thread thread) {
    this.thread = thread;
  }
}
