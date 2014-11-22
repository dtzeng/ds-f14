package migratable.managers;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

import migratable.network.ChildNamer;
import migratable.network.HostPort;
import migratable.processes.ProcessState;
import migratable.protocols.ChildToProcess;
import migratable.protocols.ChildToProcessResponse;

/**
 * Created by Derek on 9/7/2014.
 */
public class ProcessManagerServeConnection implements Runnable {
    Socket socket;
    private Map<String, ProcessState> processes;
    private Map<String, HostPort> children;
    private ChildNamer namer;

    public ProcessManagerServeConnection(Socket socket,
	    Map<String, ProcessState> processes,
	    Map<String, HostPort> children, ChildNamer namer) {
	this.socket = socket;
	this.processes = processes;
	this.children = children;
	this.namer = namer;
    }

    @Override
    public void run() {
	ObjectOutputStream oos = null;
	ObjectInputStream ois = null;
	try {
	    oos = new ObjectOutputStream(socket.getOutputStream());
	    ois = new ObjectInputStream(socket.getInputStream());
	    ChildToProcess ctp = (ChildToProcess) ois.readObject();
	    ChildToProcessResponse response = null;

	    if (ctp.getCommand().equals("finished")) {
		if (processes.remove(ctp.getName()) != null) {
		    response = new ChildToProcessResponse(true);
		} else {
		    response = new ChildToProcessResponse(false);
		}
	    } else if (ctp.getCommand().equals("newchild")) {
		HostPort hp = new HostPort(ctp.getHost(), ctp.getPort());
		try {
		    children.put(namer.getNextName(), hp);
		    response = new ChildToProcessResponse(true);
		} catch (Exception e) {
		    response = new ChildToProcessResponse(false);
		}
	    }
	    oos.writeObject(response);
	    oos.flush();
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    try {
		if (oos != null)
		    oos.close();
		if (ois != null)
		    ois.close();
		if (!socket.isClosed())
		    socket.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }
}
