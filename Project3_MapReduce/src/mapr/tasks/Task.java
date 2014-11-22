package mapr.tasks;

/**
 * Dummy runnable interface for all tasks in the MapReduce framework. Supports an <tt>isSuccess</tt>
 * method to know if the task has succeeded.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public interface Task extends Runnable {

  public boolean isSuccess();
}
