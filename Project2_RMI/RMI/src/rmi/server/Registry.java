package rmi.server;

import rmi.communication.RemoteObjectRef;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by Derek on 10/2/2014.
 *
 * This class provides the interface for the user to interact with the registry.
 *
 */
public class Registry {
    private String host;
    private int port;


    public Registry(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    /**
     * Binds the specified RemoteObjectRef to the given service name.
     * @param serviceName
     * @param ref
     * @return If the service name already exists or if an error occurred, returns false.  Otherwise, returns true.
     */
    public boolean bind(String serviceName, RemoteObjectRef ref) {
        Socket socket = null;
        BufferedReader in = null;
        PrintWriter out = null;
        boolean success = false;

        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);

            out.println("bind");
            out.println(serviceName);
            out.println(ref.getHost());
            out.println(ref.getPort());
            out.println(ref.getKey());
            out.println(ref.getInterfaceName());

            success = Boolean.parseBoolean(in.readLine());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null) in.close();
                if(out != null) out.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return success;
    }


    /**
     * Looks up the specified service
     * @param serviceName
     * @return null if not found, otherwise returns the RemoteObjectRef
     */
    public RemoteObjectRef lookup(String serviceName) {
        Socket socket = null;
        BufferedReader in = null;
        PrintWriter out = null;
        RemoteObjectRef ror = null;

        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);

            String host;
            int port, key;
            String interfaceName;

            out.println("lookup");
            out.println(serviceName);

            boolean success = Boolean.parseBoolean(in.readLine());
            if(success) {
                host = in.readLine();
                port = Integer.parseInt(in.readLine());
                key = Integer.parseInt(in.readLine());
                interfaceName = in.readLine();
                ror = new RemoteObjectRef(host, port, key, interfaceName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null) in.close();
                if(out != null) out.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ror;
    }

    /**
     * Rebinds the specified RemoteObjectRef to the service name.
     * @param serviceName
     * @param ref
     * @return false if the service name doesn't exist or an error occurred, otherwise true
     */
    public boolean rebind(String serviceName, RemoteObjectRef ref) {
        Socket socket = null;
        BufferedReader in = null;
        PrintWriter out = null;
        boolean success = false;

        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);

            out.println("rebind");
            out.println(serviceName);
            out.println(ref.getHost());
            out.println(ref.getPort());
            out.println(ref.getKey());
            out.println(ref.getInterfaceName());

            success = Boolean.parseBoolean(in.readLine());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null) in.close();
                if(out != null) out.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return success;
    }

    /**
     * Unbinds the specified service name
     * @param serviceName
     * @return false is the service name doesn't exist or an error occurred, otherwise true
     */
    public boolean unbind(String serviceName) {
        Socket socket = null;
        BufferedReader in = null;
        PrintWriter out = null;
        boolean success = false;

        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);

            out.println("unbind");
            out.println(serviceName);

            success = Boolean.parseBoolean(in.readLine());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null) in.close();
                if(out != null) out.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return success;
    }


}
