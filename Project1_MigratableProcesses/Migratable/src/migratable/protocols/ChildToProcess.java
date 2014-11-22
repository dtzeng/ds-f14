package migratable.protocols;

import java.io.Serializable;

/**
 * Created by Derek on 9/6/2014.
 *
 * The structure of a ChildManager to ProcessManager request.
 *
 */
public class ChildToProcess implements Serializable {
    private String command, name, host;
    private int port;

    public ChildToProcess(String command, String name, String host, int port) {
	this.command = command;
	this.name = name;
	this.host = host;
	this.port = port;
    }

    public String getCommand() {
	return command;
    }

    public String getName() {
	return name;
    }

    public String getHost() {
	return host;
    }

    public int getPort() {
	return port;
    }
}
