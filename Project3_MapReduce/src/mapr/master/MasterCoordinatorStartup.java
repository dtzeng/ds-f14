package mapr.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runnable class with the entry point for the Master node in MapReduce cluster.
 * Waits for workers to become available,
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class MasterCoordinatorStartup implements Runnable {
    Socket socket;
    ConcurrentHashMap<String, HostPort> workers;
    ConcurrentHashMap<String, RunningTasks> runningTasks;
    ConcurrentHashMap<String, QueuedTasks> queuedTasks;

    public MasterCoordinatorStartup(Socket socket,
	    ConcurrentHashMap<String, HostPort> workers,
	    ConcurrentHashMap<String, RunningTasks> runningTasks,
	    ConcurrentHashMap<String, QueuedTasks> queuedTasks) {
	this.socket = socket;
	this.workers = workers;
	this.runningTasks = runningTasks;
	this.queuedTasks = queuedTasks;
    }

    @Override
    public void run() {
	ObjectOutputStream oos = null;
	ObjectInputStream ois = null;
	try {
	    oos = new ObjectOutputStream(socket.getOutputStream());
	    ois = new ObjectInputStream(socket.getInputStream());
	    if (ois.readUTF().equals("newWorker")) {
		String name = ois.readUTF();
		String host = ois.readUTF();
		int port = ois.readInt();
		HostPort hp = new HostPort(host, port);
		if (workers.containsKey(name) || workers.containsValue(hp)) {
		    oos.writeBoolean(false);
		} else {
		    workers.put(name, new HostPort(host, port));
		    runningTasks.put(name, new RunningTasks());
		    queuedTasks.put(name, new QueuedTasks());
		    oos.writeBoolean(true);
		}
		oos.flush();
	    }
	} catch (IOException e) {
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
