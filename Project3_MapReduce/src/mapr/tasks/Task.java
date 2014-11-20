package mapr.tasks;

/**
 * Created by Derek on 11/17/2014.
 */
public interface Task extends Runnable {

    public boolean isSuccess();
}
