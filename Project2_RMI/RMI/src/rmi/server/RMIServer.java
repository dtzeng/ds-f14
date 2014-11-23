package rmi.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created by Derek on 10/1/2014.
 */
public class RMIServer implements Runnable {
    private String host;
    private volatile int port;
    private int registryPort;
    private RORTable table;

    public RMIServer() throws UnknownHostException {
        this.host = InetAddress.getLocalHost().getHostName();
        this.table = new RORTable();
    }

    public int getPort() {
        return port;
    }

    public int getRegistryPort() {
        return registryPort;
    }

    public void setRegistryPort(int registryPort) {
        this.registryPort = registryPort;
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
                new Thread(new RMIServeRequest(clientSocket, host, registryPort, table)).start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws UnknownHostException {
        // Start up the registry and server
        RegistryServer registry = new RegistryServer();
        RMIServer server = new RMIServer();
        new Thread(registry).start();
        while(registry.getPort() == 0);
        server.setRegistryPort(registry.getPort());
        new Thread(server).start();
        while(server.getPort() == 0);

        System.out.println("Started RMI registry on port " + Integer.toString(server.getRegistryPort()));
        System.out.println("Started RMI server on port " + Integer.toString(server.getPort()));
    }


}
