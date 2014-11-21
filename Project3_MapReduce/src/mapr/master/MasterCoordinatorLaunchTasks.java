package mapr.master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Derek on 11/15/2014.
 */
public class MasterCoordinatorLaunchTasks implements Runnable {
    ConcurrentHashMap<String, HostPort> workers;
    ConcurrentHashMap<String, FileInfo> files;
    ConcurrentHashMap<Integer, JobInfo> jobs;
    ConcurrentHashMap<String, RunningTasks> runningTasks;
    ConcurrentHashMap<String, QueuedTasks> queuedTasks;
    ArrayList<Integer> restartedJobs;
    IDAssigner jobAssigner, taskAssigner;
    int maxMaps, maxSorts, maxReds, launchFreq, partitionSize;

    Object lock;

    public MasterCoordinatorLaunchTasks(ConcurrentHashMap<String, HostPort> workers,
                                        ConcurrentHashMap<String, FileInfo> files,
                                        ConcurrentHashMap<Integer, JobInfo> jobs,
                                        ConcurrentHashMap<String, RunningTasks> runningTasks,
                                        ConcurrentHashMap<String, QueuedTasks> queuedTasks,
                                        ArrayList<Integer> restartedJobs,
                                        IDAssigner jobAssigner, IDAssigner taskAssigner,
                                        int maxMaps, int maxSorts, int maxReds, int launchFreq, int partitionSize,
                                        Object lock) {
        this.workers = workers;
        this.files = files;
        this.jobs = jobs;
        this.runningTasks = runningTasks;
        this.queuedTasks = queuedTasks;
        this.restartedJobs = restartedJobs;
        this.jobAssigner = jobAssigner;
        this.taskAssigner = taskAssigner;
        this.maxMaps = maxMaps;
        this.maxSorts = maxSorts;
        this.maxReds = maxReds;
        this.launchFreq = launchFreq;
        this.partitionSize = partitionSize;
        this.lock = lock;
    }

    private int startJob(String jobType, String user, String filename, int start, int end, String output, String otherArgs) {
        FileInfo info = files.get(filename);
        if (info == null || end >= info.getNumRecords()) return -1;

        int jobID = jobAssigner.getNextJobID();
        JobInfo job = new JobInfo(user, jobType, filename, output, otherArgs, "QUEUED", start, end, jobID);

        HashMap<String, TaskInfo> reduces = new HashMap<String, TaskInfo>();

        for (int x = start / partitionSize; x <= end / partitionSize; x++) {
            String worker = files.get(filename).getReplicaLocation(x);

            String replicaFile = filename + "-" + Integer.toString(x);
            int replicaStart = x * partitionSize;
            int replicaEnd = (x + 1) * partitionSize;
            int taskStart = (replicaStart <= start && start < replicaEnd) ? start % partitionSize : 0;
            int taskEnd = (replicaStart <= start && start < replicaEnd) ? end % partitionSize : partitionSize - 1;

            int mapID = taskAssigner.getNextJobID();
            int sortID = taskAssigner.getNextJobID();
            int reduceID = taskAssigner.getNextJobID();
            String mapIn = worker + "-dfs-root/" + replicaFile;
            String mapOut = worker + "-dfs-root/" + replicaFile + "_map" + Integer.toString(mapID);
            String sortOut = worker + "-dfs-root/" + replicaFile + "_sort" + Integer.toString(sortID);
            String reduceOut = worker + "-dfs-root/" + replicaFile + "_reduce" + Integer.toString(reduceID);

            // Create new map and sort for replica
            TaskInfo map = new TaskInfo("map", mapIn, mapOut, taskStart, taskEnd, mapID, otherArgs, jobID, jobType);
            TaskInfo sort = new TaskInfo("sort", mapOut, sortOut, sortID, mapID, jobID, jobType);

            // Add map and sort tasks, and update reduce dependencies
            queuedTasks.get(worker).queueMap(map);
            queuedTasks.get(worker).queueSort(mapID, sort);
            job.addTask(mapID);
            job.addTask(sortID);
            TaskInfo reduce = reduces.get(worker);
            if (reduce == null) {
                TaskInfo newReduce = new TaskInfo("reduce", reduceOut, reduceID, jobID, jobType);
                newReduce.addDependency(sortID);
                newReduce.addFilename(sortOut);
                reduces.put(worker, newReduce);
            } else {
                reduce.addDependency(sortID);
                reduce.addFilename(sortOut);
            }
        }

        // Add all reduces
        Iterator<Map.Entry<String, TaskInfo>> iter = reduces.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, TaskInfo> next = iter.next();
            String worker = next.getKey();
            TaskInfo reduce = next.getValue();
            queuedTasks.get(worker).queueReduce(reduce);
            job.addTask(reduce.getTaskID());
        }

        jobs.put(jobID, job);

        return jobID;
    }

    public boolean sendTaskToWorker(String worker, TaskInfo task) {
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        HostPort hp = workers.get(worker);

        // Check if worker failed before
        if(hp == null) {
            int oldJobID = task.getSourceJobID();
            if(!restartedJobs.contains(oldJobID)) {
                JobInfo oldJob = jobs.get(oldJobID);
                if(oldJob != null && !oldJob.getStatus().equals("FAILED")) {
                    int newJobID = startJob(oldJob.getJobType(), oldJob.getUser(), oldJob.getInput(),
                                            oldJob.getRecordStart(), oldJob.getRecordEnd(),
                                            oldJob.getOutput(), oldJob.getOtherArgs());
                    oldJob.setStatus("RESTARTED AS JOB " + Integer.toString(newJobID));
                    restartedJobs.add(oldJobID);
                }
            }
            return false;
        }

        boolean result = true;
        try {
            socket = new Socket(hp.getHost(), hp.getPort());
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());
            oos.writeUTF("newTask");
            oos.writeObject(task);
            oos.flush();
            if(!ois.readBoolean())
                result = false;
        } catch (Exception e) {
            result = false;
        } finally {
            try {
                if (oos != null) oos.close();
                if (ois != null) ois.close();
                if (socket != null && !socket.isClosed()) socket.close();
            } catch (Exception e) {
                // ignore
            }
        }

        // Reassign tasks for worker if worker failed during connection
        if(!result) {
            for(FileInfo fileInfo: files.values()) {
                fileInfo.removeWorker(worker);
            }
            queuedTasks.remove(worker);
            runningTasks.get(worker).addTask(task);
            reassignTasks(worker);
            runningTasks.remove(worker);
        }

        return result;
    }

    private void reassignTasks(String worker) {
        RunningTasks rt = runningTasks.get(worker);
        if(rt == null) return;
        ConcurrentHashMap<Integer, TaskInfo> maps = rt.getMaps();
        ConcurrentHashMap<Integer, TaskInfo> sorts = rt.getSorts();
        ConcurrentHashMap<Integer, TaskInfo> reduces = rt.getReduces();

        Iterator<TaskInfo> iter = maps.values().iterator();
        while(iter.hasNext()) {
            TaskInfo taskInfo = iter.next();
            int oldJobID = taskInfo.getSourceJobID();
            if(!restartedJobs.contains(oldJobID)) {
                JobInfo oldJob = jobs.get(oldJobID);
                if(oldJob != null) {
                    int newJobID = startJob(oldJob.getJobType(), oldJob.getUser(), oldJob.getInput(),
                            oldJob.getRecordStart(), oldJob.getRecordEnd(),
                            oldJob.getOutput(), oldJob.getOtherArgs());
                    oldJob.setStatus("RESTARTED AS JOB " + Integer.toString(newJobID));
                    restartedJobs.add(oldJobID);
                }
            }
        }

        iter = sorts.values().iterator();
        while(iter.hasNext()) {
            TaskInfo taskInfo = iter.next();
            int oldJobID = taskInfo.getSourceJobID();
            if(!restartedJobs.contains(oldJobID)) {
                JobInfo oldJob = jobs.get(oldJobID);
                if(oldJob != null) {
                    int newJobID = startJob(oldJob.getJobType(), oldJob.getUser(), oldJob.getInput(),
                            oldJob.getRecordStart(), oldJob.getRecordEnd(),
                            oldJob.getOutput(), oldJob.getOtherArgs());
                    oldJob.setStatus("RESTARTED AS JOB " + Integer.toString(newJobID));
                    restartedJobs.add(oldJobID);
                }
            }
        }

        iter = reduces.values().iterator();
        while(iter.hasNext()) {
            TaskInfo taskInfo = iter.next();
            int oldJobID = taskInfo.getSourceJobID();
            if(!restartedJobs.contains(oldJobID)) {
                JobInfo oldJob = jobs.get(oldJobID);
                if(oldJob != null) {
                    int newJobID = startJob(oldJob.getJobType(), oldJob.getUser(), oldJob.getInput(),
                            oldJob.getRecordStart(), oldJob.getRecordEnd(),
                            oldJob.getOutput(), oldJob.getOtherArgs());
                    oldJob.setStatus("RESTARTED AS JOB " + Integer.toString(newJobID));
                    restartedJobs.add(oldJobID);
                }
            }
        }
    }

    @Override
    public void run() {
        while(true) {
            try {
                Thread.sleep(launchFreq * 1000);
            } catch (InterruptedException e) {
                //ignore
            }

            synchronized (lock) {
                Iterator<Map.Entry<String, RunningTasks>> iter = runningTasks.entrySet().iterator();
                while(iter.hasNext()) {
                    Map.Entry<String, RunningTasks> next = iter.next();
                    String worker = next.getKey();
                    RunningTasks tasks = next.getValue();

                    // Launch up to max number of maps
                    for(int x = 0; x < maxMaps - tasks.numMaps(); x++) {
                        if(queuedTasks.get(worker) == null) {
                            break;
                        }

                        TaskInfo nextMap = queuedTasks.get(worker).nextMap();
                        int jobID = 0;
                        if(nextMap == null) {
                            break;
                        }
                        else {
                            jobID = nextMap.getSourceJobID();
                        }

                        if(jobs.get(jobID) == null) {
                            x--;
                            continue;
                        }

                        if(jobs.get(jobID).getStatus().equals("FAILED") || jobs.get(jobID).getStatus().contains("RESTARTED")) {
                            jobs.get(jobID).removeTask(nextMap.getTaskID());
                            x--;
                            continue;
                        }
                        else {
                            if(!sendTaskToWorker(worker, nextMap)) {
                                if(!jobs.get(jobID).getStatus().contains("RESTARTED")) {
                                    jobs.get(jobID).setStatus("FAILED");
                                    x--;
                                }
                            }
                            else {
                                runningTasks.get(worker).addMap(nextMap.getTaskID(), nextMap);
                                jobs.get(jobID).setStatus("RUNNING");
                            }
                        }
                    }

                    // Launch up to max number of sorts
                    for(int x = 0; x < maxSorts - tasks.numSorts(); x++) {
                        if(queuedTasks.get(worker) == null) {
                            break;
                        }

                        TaskInfo nextSort = queuedTasks.get(worker).nextSort();
                        int jobID = 0;
                        if(nextSort == null) {
                            break;
                        }
                        else {
                            jobID = nextSort.getSourceJobID();
                        }

                        if(jobs.get(jobID) == null) {
                            x--;
                            continue;
                        }

                        if(jobs.get(jobID).getStatus().equals("FAILED") || jobs.get(jobID).getStatus().contains("RESTARTED")) {
                            jobs.get(jobID).removeTask(nextSort.getTaskID());
                            x--;
                            continue;
                        }
                        else {
                            if(!sendTaskToWorker(worker, nextSort)) {
                                if(!jobs.get(jobID).getStatus().contains("RESTARTED")) {
                                    jobs.get(jobID).setStatus("FAILED");
                                    x--;
                                }
                            }
                            else {
                                runningTasks.get(worker).addSort(nextSort.getTaskID(), nextSort);
                            }
                        }
                    }

                    // Launch up to max number of reduces
                    for(int x = 0; x < maxReds - tasks.numReduces(); x++) {
                        if(queuedTasks.get(worker) == null) {
                            break;
                        }

                        TaskInfo nextReduce = queuedTasks.get(worker).nextReduce();
                        int jobID = 0;
                        if(nextReduce == null) {
                            break;
                        }
                        else {
                            jobID = nextReduce.getSourceJobID();
                        }

                        if(jobs.get(jobID) == null) {
                            x--;
                            continue;
                        }

                        if(jobs.get(jobID).getStatus().equals("FAILED") || jobs.get(jobID).getStatus().contains("RESTARTED")) {
                            jobs.get(jobID).removeTask(nextReduce.getTaskID());
                            x--;
                            continue;
                        }
                        else {
                            if(!sendTaskToWorker(worker, nextReduce)) {
                                if(!jobs.get(jobID).getStatus().contains("RESTARTED")) {
                                    jobs.get(jobID).setStatus("FAILED");
                                    x--;
                                }
                            }
                            else {
                                runningTasks.get(worker).addReduce(nextReduce.getTaskID(), nextReduce);
                            }
                        }
                    }
                }
            }
        }
    }
}
