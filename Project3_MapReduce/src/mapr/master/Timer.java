package mapr.master;

/**
 * Timeout object for MapReduce framework.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 */
public class Timer implements Runnable {
    volatile boolean done;
    int seconds;

    public Timer(int seconds) {
	this.done = false;
	this.seconds = seconds;
    }

    @Override
    public void run() {
	try {
	    Thread.sleep(seconds * 1000);
	} catch (InterruptedException e) {
	} finally {
	    done = true;
	}
    }
}
