package mapr.master;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Encapsulates all metadata of a Job from MapReduce user, including input-/output-file,
 * command-line argument, job status, JobID, etc.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class JobInfo implements Serializable {

  private static final long serialVersionUID = -1099484088749732015L;
  /**
   * Name of user node that started the job.
   */
  private String user;
  /**
   * Type of the job, e.g. `grep` or `wordcount`.
   */
  private String jobType;
  /**
   * Path to the input-/output-files.
   */
  private String input, output;
  /**
   * Additional command-line arguments fed in by the user.
   */
  private String otherArgs;
  /**
   * Human-readable status message for the job.
   */
  private String status;
  /**
   * The range for the current job to operate on from the input file.
   */
  private int recordStart, recordEnd;
  /**
   * Unique JobID associated to the current job.
   */
  private int jobID;
  /**
   * List of Task IDs for pending tasks associated to the job.
   */
  private ArrayList<Integer> tasks;

  public JobInfo(String user, String jobType, String input, String output, String otherArgs,
      String status, int recordStart, int recordEnd, int jobID) {
    this.user = user;
    this.jobType = jobType;
    this.input = input;
    this.output = output;
    this.otherArgs = otherArgs;
    this.status = status;
    this.recordStart = recordStart;
    this.recordEnd = recordEnd;
    this.jobID = jobID;
    this.tasks = new ArrayList<Integer>();
  }

  public String getUser() {
    return user;
  }

  public String getJobType() {
    return jobType;
  }

  public String getInput() {
    return input;
  }

  public String getOutput() {
    return output;
  }

  public String getOtherArgs() {
    return otherArgs;
  }

  public String getStatus() {
    return status;
  }

  public int getRecordStart() {
    return recordStart;
  }

  public int getRecordEnd() {
    return recordEnd;
  }

  public int getJobID() {
    return jobID;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public void addTask(Integer taskID) {
    tasks.add(taskID);
  }

  public void removeTask(Integer taskID) {
    tasks.remove(taskID);
  }

  public int numTasks() {
    return tasks.size();
  }

}
