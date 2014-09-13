package migratable.network;

import java.io.Serializable;

/**
 * Created by Derek on 9/6/2014.
 */
public class HostPort implements Serializable {
    private String host;
    private int port;

    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return host + ":" + Integer.toString(port);
    }
}
