package mapr.master;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of all running Mapper, Sorter, and Reducers of a worker node.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class RunningTasks implements Serializable {

  private static final long serialVersionUID = 2639601857635361135L;
  ConcurrentHashMap<Integer, TaskInfo> maps;
  ConcurrentHashMap<Integer, TaskInfo> sorts;
  ConcurrentHashMap<Integer, TaskInfo> reduces;

  public RunningTasks() {
    maps = new ConcurrentHashMap<Integer, TaskInfo>();
    sorts = new ConcurrentHashMap<Integer, TaskInfo>();
    reduces = new ConcurrentHashMap<Integer, TaskInfo>();
  }

  public void addMap(int taskID, TaskInfo task) {
    maps.put(taskID, task);
  }

  public void addSort(int taskID, TaskInfo task) {
    sorts.put(taskID, task);
  }

  public void addReduce(int taskID, TaskInfo task) {
    reduces.put(taskID, task);
  }

  /**
   * Adds a task as <i>Running</i>.
   * 
   * @param task Metadata for the task to be added.
   */
  public void addTask(TaskInfo task) {
    if (task.getTaskType().equals("map"))
      addMap(task.getTaskID(), task);
    else if (task.getTaskType().equals("sort"))
      addSort(task.getTaskID(), task);
    else
      addReduce(task.getTaskID(), task);
  }

  public void finishedMap(int taskID) {
    maps.remove(taskID);
  }

  public void finishedSort(int taskID) {
    sorts.remove(taskID);
  }

  public void finishedReduce(int taskID) {
    reduces.remove(taskID);
  }

  public int numMaps() {
    return maps.size();
  }

  public int numSorts() {
    return sorts.size();
  }

  public int numReduces() {
    return reduces.size();
  }

  public ConcurrentHashMap<Integer, TaskInfo> getMaps() {
    return maps;
  }

  public ConcurrentHashMap<Integer, TaskInfo> getSorts() {
    return sorts;
  }

  public ConcurrentHashMap<Integer, TaskInfo> getReduces() {
    return reduces;
  }

  /**
   * Displays a human-readable summary of all running MapReduce tasks.
   */
  public String toString() {
    String result = "";
    Iterator<TaskInfo> mapIter = maps.values().iterator();
    while (mapIter.hasNext()) {
      TaskInfo task = mapIter.next();
      result += task.getTaskType() + task.getTaskID() + " (Job " + task.getSourceJobID() + ")";
      if (mapIter.hasNext())
        result += "|";
    }

    Iterator<TaskInfo> sortIter = sorts.values().iterator();
    while (sortIter.hasNext()) {
      TaskInfo task = sortIter.next();
      result += "|";
      result += task.getTaskType() + task.getTaskID() + " (Job " + task.getSourceJobID() + ")";
    }

    Iterator<TaskInfo> reduceIter = reduces.values().iterator();
    while (reduceIter.hasNext()) {
      TaskInfo task = reduceIter.next();
      result += "|";
      result += task.getTaskType() + task.getTaskID() + " (Job " + task.getSourceJobID() + ")";
    }

    return result;
  }
}
