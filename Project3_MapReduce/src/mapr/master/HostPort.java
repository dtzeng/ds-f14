package mapr.master;

import java.io.Serializable;

/**
 * Created by Derek on 11/12/2014.
 */
public class HostPort implements Serializable {
    String host;
    int port;

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

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof HostPort)) return false;
        HostPort cast = (HostPort) o;
        return host.equals(cast.getHost()) && (port == cast.getPort());
    }

    @Override
    public int hashCode() {
        return host.hashCode() + port;
    }
}
