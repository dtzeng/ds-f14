package migratable.network;

/**
 * Created by Derek on 9/7/2014.
 *
 */
public class ChildNamer {
    private int counter;

    public ChildNamer() {
	counter = 0;
    }

    public synchronized String getNextName() {
	return "child" + Integer.toString(counter++);
    }
}
