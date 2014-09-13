package migratable.processes;

import java.io.Serializable;

/**
 * Created by Derek on 9/7/2014.
 */
public interface MigratableProcess extends Runnable, Serializable {

    public void run();

    public void suspend();

    public String getName();

    public String getCommand();

    public boolean getDone();

    public void setDone(boolean b);

}
