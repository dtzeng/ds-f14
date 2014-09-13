package migratable.processes;

/**
 * Created by Derek on 9/7/2014.
 */
public class ProcessState {
    private String name, command, suspended, child;

    public ProcessState(String name, String command, String suspended, String child) {
        this.name = name;
        this.command = command;
        this.suspended = suspended;
        this.child = child;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public String getSuspended() {
        return suspended;
    }

    public void setSuspended(String suspended) {
        this.suspended = suspended;
    }

    public String getChild() {
        return child;
    }

    public void setChild(String child) {
        this.child = child;
    }
}
