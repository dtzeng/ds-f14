package mapr.master;

/**
 * Global JobID / TaskID manager, where a unique, thread-safe ID can be obtained for each new Job or
 * new Task.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 */
public class IDAssigner {
  private int counter;

  public IDAssigner() {
    counter = 0;
  }

  /**
   * Obtains the next counter value.
   * 
   * @return Next counter value.
   */
  public synchronized int getNextJobID() {
    return counter++;
  }
}
