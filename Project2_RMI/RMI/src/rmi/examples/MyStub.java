package rmi.examples;

import rmi.communication.RMIMessage;
import rmi.communication.RemoteObjectRef;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by Derek on 10/4/2014.
 */
public abstract class MyStub {
    public abstract RemoteObjectRef getRef();

    /**
     * Sends the given request to the dispatcher and unmarshals the return if necessary.
     * @param request
     * @return The return object from the dispatcher
     */
    public Object sendToDispatcher(RMIMessage request) {
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        Object result = null;

        try {
            socket = new Socket(getRef().getHost(), getRef().getPort());
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());

            oos.writeObject("invoke");
            oos.writeObject(request);
            RMIMessage response = (RMIMessage) ois.readObject();
            if(response.getReturnException() != null) throw response.getReturnException();
            result = response.getReturnObject();

            // If the return object is a remote object reference, then convert it back into its stub
            if(result instanceof RemoteObjectRef) {
                result = ((RemoteObjectRef) result).localize();
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

        return result;
    }
}
