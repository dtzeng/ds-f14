package rmi.server;

import rmi.communication.RemoteObjectRef;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Derek on 10/1/2014.
 */
public class RegistryServer implements Runnable {
    private volatile int port;
    private Map<String, RemoteObjectRef> refs;

    public RegistryServer() {
        refs = new ConcurrentHashMap<String, RemoteObjectRef>();
    }

    public int getPort() {
        return port;
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
                new Thread(new RegistryServeRequest(clientSocket, refs)).start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
