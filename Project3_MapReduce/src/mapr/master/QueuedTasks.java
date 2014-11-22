package mapr.master;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Encapsulates the pending and queued tasks of a worker node.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 * 
 */
public class QueuedTasks {
  /**
   * Mapper tasks that can be started at any time.
   */
  ConcurrentLinkedQueue<TaskInfo> pendingMaps;
  /**
   * Sorter tasks that can be started at any time.
   */
  ConcurrentLinkedQueue<TaskInfo> pendingSorts;
  /**
   * Reducer tasks that can be started at any time.
   */
  ConcurrentLinkedQueue<TaskInfo> pendingReduces;
  /**
   * Sorting tasks that are pending on certain dependencies.
   */
  ConcurrentHashMap<Integer, TaskInfo> dependentSorts;
  /**
   * Reducer tasks that are pending on certain dependencies.
   */
  ConcurrentHashMap<Integer, TaskInfo> dependentReduces;

  public QueuedTasks() {
    this.pendingMaps = new ConcurrentLinkedQueue<TaskInfo>();
    this.pendingSorts = new ConcurrentLinkedQueue<TaskInfo>();
    this.pendingReduces = new ConcurrentLinkedQueue<TaskInfo>();

    this.dependentSorts = new ConcurrentHashMap<Integer, TaskInfo>();
    this.dependentReduces = new ConcurrentHashMap<Integer, TaskInfo>();
  }

  /**
   * Adds a Mapper task in the pending list.
   * 
   * @param task Task to be added into pending list.
   */
  public void queueMap(TaskInfo task) {
    pendingMaps.add(task);
  }

  /**
   * Adds a Sorter task in the pending list.
   * 
   * @param task Task to be added into pending list.
   */
  public void queueSort(Integer dependency, TaskInfo task) {
    dependentSorts.put(dependency, task);
  }

  /**
   * Adds a Reducer task in the pending list.
   * 
   * @param task Task to be added into pending list.
   */
  public void queueReduce(TaskInfo task) {
    synchronized (dependentReduces) {
      List<Integer> dependencies = task.getTaskDependencies();
      for (Integer dependency : dependencies) {
        dependentReduces.put(dependency, task);
      }
    }
  }

  /**
   * Upon finishing a Mapper, remove it from the dependency list of all Sorters.
   * 
   * @param id TaskID for the finished Mapper
   */
  public void finishedMap(Integer id) {
    synchronized (dependentSorts) {
      TaskInfo sort = dependentSorts.remove(id);
      if (sort.removeDependency(id) == 0) {
        pendingSorts.add(sort);
      }
    }
  }

  /**
   * Upon finishing a Sorter, remove it from the dependency list of all Reducers.
   * 
   * @param id TaskID for the finished Sorter
   */
  public void finishedSort(Integer id) {
    synchronized (dependentReduces) {
      TaskInfo reduce = dependentReduces.remove(id);
      if (reduce.removeDependency(id) == 0) {
        pendingReduces.add(reduce);
      }
    }
  }

  /**
   * Return the next Mapper tasks in the waiting list.
   * 
   * @return Next pending Mapper task.
   */
  public TaskInfo nextMap() {
    TaskInfo result = null;
    try {
      result = pendingMaps.remove();
    } catch (NoSuchElementException e) {
    }
    return result;
  }

  /**
   * Return the next Sorter tasks in the waiting list.
   * 
   * @return Next pending Sorter task.
   */
  public TaskInfo nextSort() {
    TaskInfo result = null;
    try {
      result = pendingSorts.remove();
    } catch (NoSuchElementException e) {
    }
    return result;
  }

  /**
   * Return the next Reducer tasks in the waiting list.
   * 
   * @return Next pending Reducer task.
   */
  public TaskInfo nextReduce() {
    TaskInfo result = null;
    try {
      result = pendingReduces.remove();
    } catch (NoSuchElementException e) {
    }
    return result;
  }

  /**
   * Returns a human-readable digest of task statuses.
   */
  public String toString() {
    String result = "";

    Iterator<TaskInfo> iterator = pendingMaps.iterator();
    while (iterator.hasNext()) {
      TaskInfo task = iterator.next();
      result +=
          task.getTaskType() + task.getTaskID() + " (Job " + task.getSourceJobID() + "), pending";
      if (iterator.hasNext())
        result += "|";
    }

    iterator = pendingSorts.iterator();
    while (iterator.hasNext()) {
      TaskInfo task = iterator.next();
      result += "|";
      result +=
          task.getTaskType() + task.getTaskID() + " (Job " + task.getSourceJobID() + "), pending";
    }

    iterator = pendingReduces.iterator();
    while (iterator.hasNext()) {
      TaskInfo task = iterator.next();
      result += "|";
      result +=
          task.getTaskType() + task.getTaskID() + " (Job " + task.getSourceJobID() + "), pending";
    }

    iterator = dependentSorts.values().iterator();
    while (iterator.hasNext()) {
      TaskInfo task = iterator.next();
      result += "|";
      result +=
          task.getTaskType() + task.getTaskID() + " (Job " + task.getSourceJobID() + "), waiting";
    }

    ArrayList<String> seenReduces = new ArrayList<String>();
    iterator = dependentReduces.values().iterator();
    while (iterator.hasNext()) {
      TaskInfo task = iterator.next();
      result += "|";
      if (!seenReduces.contains(task.getTaskType() + task.getTaskID())) {
        result +=
            task.getTaskType() + task.getTaskID() + " (Job " + task.getSourceJobID() + "), waiting";
        seenReduces.add(task.getTaskType() + task.getTaskID());
      }
    }

    return result;
  }
}
