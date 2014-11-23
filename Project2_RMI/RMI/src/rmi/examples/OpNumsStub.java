package rmi.examples;

import rmi.communication.RMIMessage;
import rmi.communication.RemoteObjectRef;

/**
 * Created by Derek on 10/3/2014.
 */
public class OpNumsStub extends MyStub implements OpNums {
    private RemoteObjectRef ref;

    public OpNumsStub(RemoteObjectRef ref) {
        this.ref = ref;
    }

    public RemoteObjectRef getRef() {
        return ref;
    }

    @Override
    public int add(int x, int y) {
        RMIMessage request = new RMIMessage(ref, "add", new Object[]{x, y}, null, null);
        return (Integer) sendToDispatcher(request);
    }

    @Override
    public int sub(int x, int y) {
        RMIMessage request = new RMIMessage(ref, "sub", new Object[]{x, y}, null, null);
        return (Integer) sendToDispatcher(request);
    }

    @Override
    public int mult(int x, int y) {
        RMIMessage request = new RMIMessage(ref, "mult", new Object[]{x, y}, null, null);
        return (Integer) sendToDispatcher(request);
    }

    @Override
    public int div(int x, int y) {
        RMIMessage request = new RMIMessage(ref, "div", new Object[]{x, y}, null, null);
        return (Integer) sendToDispatcher(request);
    }
}
