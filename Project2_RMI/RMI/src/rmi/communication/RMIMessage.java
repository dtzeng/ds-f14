package rmi.communication;

import java.io.Serializable;

/**
 * Created by Derek on 10/3/2014.
 *
 * This class encapsulates the information needed for the communication between
 * the client side stubs and server side dispatcher.
 *
 */
public class RMIMessage implements Serializable {
    private RemoteObjectRef ref;
    private String methodName;
    private Object[] params;

    private Object returnObject;
    private Exception returnException;

    public RMIMessage(RemoteObjectRef ref, String methodName, Object[] params, Object returnObject, Exception returnException) {
        this.ref = ref;
        this.methodName = methodName;
        this.params = params;
        this.returnObject = returnObject;
        this.returnException = returnException;
    }

    public RemoteObjectRef getRef() {
        return ref;
    }

    public String getMethodName() {
        return methodName;
    }

    public Object[] getParams() {
        return params;
    }

    public Object getReturnObject() {
        return returnObject;
    }

    public Exception getReturnException() {
        return returnException;
    }
}
