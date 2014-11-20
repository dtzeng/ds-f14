package mapr.worker;

import mapr.master.TaskInfo;

/**
 * Created by Derek on 11/16/2014.
 */
public class TaskThread {
    TaskInfo info;
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
