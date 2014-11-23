package rmi.server;

import rmi.communication.RMIMessage;
import rmi.communication.RemoteObjectRef;
import rmi.examples.MyStub;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.Socket;

/**
 * Created by Derek on 10/2/2014.
 */
public class RMIServeRequest implements Runnable {
    Socket socket;
    String host;
    int registryPort;
    RORTable table;

    public RMIServeRequest(Socket socket, String host, int registryPort, RORTable table) {
        this.socket = socket;
        this.host = host;
        this.registryPort = registryPort;
        this.table = table;
    }

    @Override
    public void run() {
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());

            String request = (String) ois.readObject();
            if(request.equals("registryLocation")) {
                oos.writeObject(host);
                oos.writeInt(registryPort);
            }
            else if(request.equals("invoke")) {
                RMIMessage msg = (RMIMessage) ois.readObject();
                if(!table.containsObj(msg.getRef())) {
                    table.addObj(msg.getRef());
                }
                Object obj = table.findObj(msg.getRef());
                Method[] methods = obj.getClass().getMethods();
                Method method = null;
                for(int x = 0; x < methods.length; x++) {
                    if(methods[x].getName().equals(msg.getMethodName())) {
                        method = methods[x];
                        break;
                    }
                }
                Object[] unmarshParams = msg.getParams();
                for(int x = 0; x < unmarshParams.length; x++) {
                    if(unmarshParams[x] instanceof RemoteObjectRef) {
                        unmarshParams[x] = ((RemoteObjectRef) unmarshParams[x]).localize();
                    }
                }
                RMIMessage response;
                try {
                    System.out.println("Invoking '" + msg.getMethodName() + "()'...");
                    Object result = method.invoke(obj, unmarshParams);
                    if(result instanceof MyStub) {
                        result = ((MyStub) result).getRef();
                    }
                    response = new RMIMessage(null, null, null, result, null);
                } catch (Exception e) {
                    e.printStackTrace();
                    response = new RMIMessage(null, null, null, null, e);
                }
                oos.writeObject(response);
            }
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
