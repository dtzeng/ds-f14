package rmi.communication;

import java.io.Serializable;
import java.lang.reflect.Constructor;

/**
 * Created by Derek on 10/1/2014.
 */
public class RemoteObjectRef implements Serializable {
    String host;
    int port, key;
    String interfaceName;

    public RemoteObjectRef(String host, int port, int key, String interfaceName) {
        this.host = host;
        this.port = port;
        this.key = key;
        this.interfaceName = interfaceName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getKey() {
        return key;
    }

    public String getInterfaceName() {
        return interfaceName;
    }

    @Override
    public boolean equals(Object o) {
        RemoteObjectRef ref = (RemoteObjectRef) o;
        return this.host.equals(ref.getHost()) &&
               this.port == ref.getPort() &&
               this.key == ref.getKey() &&
               this.interfaceName.equals(ref.getInterfaceName());
    }

    @Override
    public int hashCode() {
        return host.hashCode() + port + key + interfaceName.hashCode();
    }

    /**
     * Creates a stub
     * @return A stub to this remote object reference
     * @throws Exception
     */
    public Object localize() throws Exception {
        Class c = Class.forName("rmi.examples." + interfaceName + "Stub");
        Constructor ctor = c.getDeclaredConstructor(RemoteObjectRef.class);
        return ctor.newInstance(this);
    }
}
