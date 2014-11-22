package migratable.managers;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

import migratable.processes.ProcessThread;
import migratable.protocols.ChildToProcess;
import migratable.protocols.ChildToProcessResponse;

/**
 * Created by Derek on 9/6/2014.
 */
public class ProcessRunner implements Runnable {
  private ProcessThread processThread;
  private Map<String, ProcessThread> processes;
  private String masterHost;
  private int masterPort;

  public ProcessRunner(ProcessThread processThread, Map<String, ProcessThread> processes,
      String masterHost, int masterPort) {
    this.processThread = processThread;
    this.processes = processes;
    this.masterHost = masterHost;
    this.masterPort = masterPort;
  }

  @Override
  public void run() {
    String name = processThread.getProcess().getName();
    processes.put(name, processThread);
    processThread.getThread().start();
    try {
      while (!processThread.getProcess().getDone());
      processThread.getThread().join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    processes.remove(name);
    ChildToProcess ctp = new ChildToProcess("finished", name, null, 0);
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    try {
      Socket socket = new Socket(masterHost, masterPort);
      oos = new ObjectOutputStream(socket.getOutputStream());
      ois = new ObjectInputStream(socket.getInputStream());
      oos.writeObject(ctp);
      oos.flush();
      ChildToProcessResponse response = (ChildToProcessResponse) ois.readObject();
      if (!response.getSuccess())
        throw new Exception("Failed to delete object");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (oos != null)
          oos.close();
        if (ois != null)
          ois.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
