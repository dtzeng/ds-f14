package mapr.master;

/**
 * Created by Derek on 11/13/2014.
 */
public class IDAssigner {
    private int counter;

    public IDAssigner() {
        counter = 0;
    }

    public synchronized int getNextJobID() {
        return counter++;
    }
}
