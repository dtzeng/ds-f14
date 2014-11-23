package rmi.server;

import rmi.communication.RemoteObjectRef;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Derek on 10/3/2014.
 */
public class RORTable {
    private Map<RemoteObjectRef, Object> refs;

    public RORTable() {
        refs = new ConcurrentHashMap<RemoteObjectRef, Object>();
    }

    public void addObj(RemoteObjectRef ref) throws Exception {
        Class klass = Class.forName("rmi.examples." + ref.getInterfaceName() + "Impl");
        refs.put(ref, klass.newInstance());
    }

    public boolean containsObj(RemoteObjectRef ref) {
        return refs.containsKey(ref);
    }

    public Object findObj(RemoteObjectRef ref) {
        return refs.get(ref);
    }
}
