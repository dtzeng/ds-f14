package migratable.managers;

import migratable.processes.MigratableProcess;
import migratable.processes.ProcessThread;
import migratable.protocols.ProcessToChild;
import migratable.protocols.ProcessToChildResponse;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

/**
 * Created by Derek on 9/7/2014.
 */
public class ChildManagerServeConnection implements Runnable{
    private Socket socket;
    private Map<String, ProcessThread> processes;
    private String masterHost;
    private int masterPort;

    public ChildManagerServeConnection(Socket socket, Map<String, ProcessThread> processes, String masterHost, int masterPort) {
        this.socket = socket;
        this.processes = processes;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    private void killAllProcesses() {
        for(ProcessThread p: processes.values()) {
            p.getProcess().setDone(true);
            if(!p.getThread().isAlive()) {
                p.getThread().start();
            }
        }
    }

    @Override
    public void run() {
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());
            ProcessToChild ptc = (ProcessToChild) ois.readObject();
            ProcessToChildResponse response = null;

            if(ptc.getCommand().equals("shutdown")) {
                killAllProcesses();
                response = new ProcessToChildResponse(true, null);
                oos.writeObject(response);
                oos.flush();
                System.out.println("Received shutdown. Exiting...");
                System.exit(0);
            }
            else if(ptc.getCommand().equals("run")) {
                MigratableProcess p = ptc.getProcess();
                Thread t = new Thread(p);
                ProcessThread processThread = new ProcessThread(p, t);
                Thread wrap = new Thread(new ProcessRunner(processThread, processes, masterHost, masterPort));
                wrap.start();
                System.out.println("Started process '" + p.getName() + "'");
                response = new ProcessToChildResponse(true, null);
            }
            else if(ptc.getCommand().equals("suspend")) {
                ProcessThread processThread = processes.get(ptc.getName());
                if(processThread == null) response = new ProcessToChildResponse(false, null);
                else {
                    if(processThread.getThread().isAlive()) {
                        processThread.getProcess().suspend();
                        Thread t = new Thread(processThread.getProcess());
                        processThread.setThread(t);
                        System.out.println("Suspended process '" + processThread.getProcess().getName() + "'");
                    }
                    else {
                        System.out.println("Process '" + processThread.getProcess().getName() + "' already suspended");
                    }
                    response = new ProcessToChildResponse(true, null);
                }
            }
            else if(ptc.getCommand().equals("resume")) {
                ProcessThread processThread = processes.get(ptc.getName());
                if(processThread == null) response = new ProcessToChildResponse(false, null);
                else {
                    if(!processThread.getThread().isAlive()) {
                        processThread.getThread().start();
                        System.out.println("Resumed process '" + processThread.getProcess().getName() + "'");
                    }
                    else {
                        System.out.println("Process '" + processThread.getProcess().getName() + "' already running");
                    }
                    response = new ProcessToChildResponse(true, null);
                }
            }
            else if(ptc.getCommand().equals("kill")) {
                ProcessThread processThread = processes.get(ptc.getName());
                if(processThread == null) response = new ProcessToChildResponse(false, null);
                else {
                    processThread.getProcess().setDone(true);
                    if(!processThread.getThread().isAlive()) {
                        processThread.getThread().start();
                    }
                    System.out.println("Killed process '" + processThread.getProcess().getName() + "'");
                    response = new ProcessToChildResponse(true, null);
                }
            }
            else if(ptc.getCommand().equals("migrate")) {
                ProcessThread processThread = processes.get(ptc.getName());
                if(processThread == null) response = new ProcessToChildResponse(false, null);
                else {
                    if(processThread.getThread().isAlive()) {
                        processThread.getProcess().suspend();
                    }
                    processes.remove(processThread.getProcess().getName());
                    System.out.println("Stopped process '" + processThread.getProcess().getName() + "' and returning to master");
                    response = new ProcessToChildResponse(true, processThread.getProcess());
                }
            }
            oos.writeObject(response);
            oos.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(oos != null) oos.close();
                if(ois != null) ois.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
