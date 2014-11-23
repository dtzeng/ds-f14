package rmi.examples;

import rmi.communication.RMIMessage;
import rmi.communication.RemoteObjectRef;

/**
 * Created by Derek on 10/4/2014.
 */
public class ConcatRefsStub extends MyStub implements ConcatRefs {
    private RemoteObjectRef ref;

    public ConcatRefsStub(RemoteObjectRef ref) {
        this.ref = ref;
    }

    public RemoteObjectRef getRef() {
        return ref;
    }

    @Override
    public String concatRemotes(Container a, Container b) {
        ContainerStub aStub = (ContainerStub) a;
        ContainerStub bStub = (ContainerStub) b;

        // Marshals remote objects into their references
        RMIMessage request = new RMIMessage(ref, "concatRemotes", new Object[]{aStub.getRef(), bStub.getRef()}, null, null);
        return (String) sendToDispatcher(request);
    }

    @Override
    public Container concatReturnRemote(String a, String b, String host, int port, int key) {
        RMIMessage request = new RMIMessage(ref, "concatReturnRemote", new Object[]{a, b, host, port, key}, null, null);
        return (Container) sendToDispatcher(request);
    }
}
