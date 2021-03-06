package mapr.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates all metadata for a task, including Task ID, Task Type, I/O File Name, etc.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class TaskInfo implements Serializable {

  private static final long serialVersionUID = 92699575894330642L;
  String taskType, input, output, otherArgs, jobType;
  int recordStart, recordEnd, taskID, sourceJobID;
  List<Integer> taskDependencies;
  /*
   * (Only used for `Reduce`) List of file names to merge from.
   */
  List<String> filenames;

  /**
   * Constructor used for new Mapper tasks.
   * 
   * @param taskType `mapper`
   * @param input Input file name
   * @param output Output file name
   * @param recordStart Starting point of input file to map
   * @param recordEnd End point of input file to map
   * @param taskID Unique TaskID
   * @param otherArgs Extra user arguments
   * @param sourceJobID JobID for Mapper task
   * @param jobType Class name of MapReduce job
   */
  public TaskInfo(String taskType, String input, String output, int recordStart, int recordEnd,
      int taskID, String otherArgs, int sourceJobID, String jobType) {
    this.taskType = taskType;
    this.input = input;
    this.output = output;
    this.recordStart = recordStart;
    this.recordEnd = recordEnd;
    this.taskID = taskID;
    this.otherArgs = otherArgs;
    this.sourceJobID = sourceJobID;
    this.jobType = jobType;
    this.taskDependencies = Collections.synchronizedList(new ArrayList<Integer>());
  }

  /**
   * Constructor used for new Sorter tasks.
   * 
   * @param taskType `sorter`
   * @param input Input file name
   * @param output Output file name
   * @param taskID Unique TaskID
   * @param dependency TaskID that sorter depends on before starting
   * @param sourceJobID JobID for Mapper task
   * @param jobType Class name of MapReduce job
   */
  public TaskInfo(String taskType, String input, String output, int taskID, int dependency,
      int sourceJobID, String jobType) {
    this.taskType = taskType;
    this.input = input;
    this.output = output;
    this.taskID = taskID;
    this.sourceJobID = sourceJobID;
    this.jobType = jobType;
    this.taskDependencies = Collections.synchronizedList(new ArrayList<Integer>());
    this.taskDependencies.add(dependency);
  }

  /**
   * Constructor used for new Reduce tasks.
   * 
   * @param taskType `reduce`
   * @param output Output file name
   * @param taskID Unique TaskID
   * @param sourceJobID JobID for Mapper task
   * @param jobType Class name of MapReduce job
   */
  public TaskInfo(String taskType, String output, int taskID, int sourceJobID, String jobType) {
    this.taskType = taskType;
    this.output = output;
    this.taskID = taskID;
    this.sourceJobID = sourceJobID;
    this.jobType = jobType;
    this.taskDependencies = Collections.synchronizedList(new ArrayList<Integer>());
    this.filenames = Collections.synchronizedList(new ArrayList<String>());
  }

  public String getTaskType() {
    return taskType;
  }

  public String getInput() {
    return input;
  }

  public String getOutput() {
    return output;
  }

  public int getRecordStart() {
    return recordStart;
  }

  public int getRecordEnd() {
    return recordEnd;
  }

  public int getTaskID() {
    return taskID;
  }

  public String getOtherArgs() {
    return otherArgs;
  }

  public int getSourceJobID() {
    return sourceJobID;
  }

  public String getJobType() {
    return jobType;
  }

  public List<Integer> getTaskDependencies() {
    return taskDependencies;
  }

  public synchronized int removeDependency(Integer dependency) {
    taskDependencies.remove(dependency);
    return taskDependencies.size();
  }

  public synchronized void addDependency(Integer dependency) {
    taskDependencies.add(dependency);
  }

  public List<String> getFilenames() {
    return filenames;
  }

  public synchronized void addFilename(String file) {
    filenames.add(file);
  }
}
