package mapr.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Encapsulates all metadata (excluding name) of a file on DFS, including its replica locations,
 * partitioning rules, etc.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class FileInfo implements Serializable {

  private static final long serialVersionUID = -6341305799979787052L;
  /**
   * List of worker names that contain some replica of the file.
   */
  List<ConcurrentLinkedQueue<String>> partitions;
  /**
   * Number of partitions for the file.
   */
  int numPartitions;
  /**
   * Number of records (lines) of the file.
   */
  int numRecords;

  /**
   * Initializes a <tt>FileInfo</tt> with certain number of partitions (chunks) and records (lines).
   * 
   * @param numPartitions Numbre of paritions of the file.
   * @param numRecords Number of line of the file.
   */
  public FileInfo(int numPartitions, int numRecords) {
    partitions =
        Collections.synchronizedList(new ArrayList<ConcurrentLinkedQueue<String>>(numPartitions));
    for (int x = 0; x < numPartitions; x++) {
      partitions.add(new ConcurrentLinkedQueue<String>());
    }
    this.numPartitions = numPartitions;
    this.numRecords = numRecords;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getNumRecords() {
    return numRecords;
  }

  /**
   * Looks up a specific partition of the file on DFS.
   * 
   * @param partition Parition ID to request.
   * @return Name of some worker that contains the requested partition.
   */
  public String getReplicaLocation(int partition) {
    ConcurrentLinkedQueue<String> replicaLocations = partitions.get(partition);
    String location = replicaLocations.remove();
    replicaLocations.add(location);
    return location;
  }

  /**
   * Adds to the record that some specific worker contains a certain parition of file.
   * 
   * @param partition Parition ID that the Worker contains.
   * @param worker Name of the Worker node in concern.
   */
  public void addReplicaLocation(int partition, String worker) {
    partitions.get(partition).add(worker);
  }

  /**
   * Remove from the record that some specific worker contains a certain parition of file.
   * 
   * @param partition Parition ID that the Worker <i>no longer</i> contains.
   * @param workerName of the Worker node in concern.
   */
  public void removeReplicaLocation(int partition, String worker) {
    partitions.get(partition).remove(worker);
  }

  /**
   * Removes and cleans up a worker node.
   * 
   * @param worker Name for the worker node to remove.
   */
  public void removeWorker(String worker) {
    for (int x = 0; x < numPartitions; x++) {
      removeReplicaLocation(x, worker);
    }
  }

  /**
   * Convenient method to represent the replica information of a file in a String format.
   */
  public String toString() {
    String result = "";
    Iterator<ConcurrentLinkedQueue<String>> iter = partitions.iterator();
    while (iter.hasNext()) {
      Iterator<String> locs = iter.next().iterator();
      while (locs.hasNext()) {
        result += locs.next();
        if (locs.hasNext())
          result += ", ";
      }
      if (iter.hasNext())
        result += "|";
    }
    return result;
  }
}
