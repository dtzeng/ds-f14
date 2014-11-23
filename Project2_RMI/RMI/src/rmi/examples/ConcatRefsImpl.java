package rmi.examples;

import rmi.communication.RemoteObjectRef;

/**
 * Created by Derek on 10/4/2014.
 */
public class ConcatRefsImpl implements ConcatRefs {
    @Override
    public String concatRemotes(Container a, Container b) {
        return ((String) a.getObject()) + ((String) b.getObject());
    }

    @Override
    public Container concatReturnRemote(String a, String b, String host, int port, int key) {
        RemoteObjectRef ref = new RemoteObjectRef(host, port, key, "Container");
        Container stub = null;
        try {
            stub = (Container) ref.localize();
            stub.setObject(a + b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stub;
    }
}
