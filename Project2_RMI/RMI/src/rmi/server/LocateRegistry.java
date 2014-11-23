package rmi.server;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by Derek on 10/2/2014.
 */
public class LocateRegistry {
    /**
     * @param host: host of the RMI server
     * @param port: port of the RMI server
     * @return the registry on the given RMI server
     */
    public static Registry getRegistry(String host, int port) {
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        Registry registry;

        try {
            socket = new Socket(host, port);
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());

            String registryHost;
            int registryPort;

            oos.writeObject("registryLocation");
            registryHost = (String) ois.readObject();
            registryPort = ois.readInt();
            registry = new Registry(registryHost, registryPort);
        } catch (Exception e) {
            e.printStackTrace();
            registry = null;
        } finally {
            try {
                if(oos != null) oos.close();
                if(ois != null) ois.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return registry;
    }
}
