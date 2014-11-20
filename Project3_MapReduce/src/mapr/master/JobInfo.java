package mapr.master;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Derek on 11/12/2014.
 */
public class JobInfo implements Serializable {
    private String user, jobType, input, output, otherArgs, status;
    private int recordStart, recordEnd, jobID;
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
