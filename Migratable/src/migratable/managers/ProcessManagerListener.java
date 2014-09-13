package migratable.managers;

import migratable.network.ChildNamer;
import migratable.network.HostPort;
import migratable.processes.ProcessState;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

/**
 * Created by Derek on 9/6/2014.
 */
public class ProcessManagerListener implements Runnable, Serializable {
    private Map<String, ProcessState> processes;
    private Map<String, HostPort> children;
    private ChildNamer namer;
    private int port;


    public ProcessManagerListener(Map<String, ProcessState> processes, Map<String, HostPort> children, ChildNamer namer) {
        this.processes = processes;
        this.children = children;
        this.namer = namer;
    }

    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(0);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        port = serverSocket.getLocalPort();
        while(true) {
            Socket clientSocket = null;
            try {
                clientSocket = serverSocket.accept();

                // Launch new thread to serve request
                new Thread(new ProcessManagerServeConnection(clientSocket, processes, children, namer)).start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public int getPort() {
        return port;
    }
}