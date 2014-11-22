package mapr.master;

import java.io.Serializable;

/**
 * Encapsulates a <tt>(hostname, portnum)</tt> pair for any component of MapReduce framework.
 * 
 * @author Derek Tzeng <dtzeng@andrew.cmu.edu>
 *
 */
public class HostPort implements Serializable {

  private static final long serialVersionUID = -6082476923519403699L;
  /**
   * Hostname of the node.
   */
  String host;
  /**
   * Port number for contacting the node.
   */
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
    if (!(o instanceof HostPort))
      return false;
    HostPort cast = (HostPort) o;
    return host.equals(cast.getHost()) && (port == cast.getPort());
  }

  @Override
  public int hashCode() {
    return host.hashCode() + port;
  }
}
