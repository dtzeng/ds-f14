package rmi.examples;

import rmi.communication.RMIMessage;
import rmi.communication.RemoteObjectRef;

/**
 * Created by Derek on 10/4/2014.
 */
public class ContainerStub extends MyStub implements Container {
    private RemoteObjectRef ref;

    public ContainerStub(RemoteObjectRef ref) {
        this.ref = ref;
    }

    public RemoteObjectRef getRef() {
        return ref;
    }

    @Override
    public Object getObject() {
        RMIMessage request = new RMIMessage(ref, "getObject", new Object[]{}, null, null);
        return sendToDispatcher(request);
    }

    @Override
    public void setObject(Object o) {
        RMIMessage request = new RMIMessage(ref, "setObject", new Object[]{o}, null, null);
        sendToDispatcher(request);
    }
}
