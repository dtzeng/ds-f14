package rmi.examples;

/**
 * Created by Derek on 10/4/2014.
 */
public interface ConcatRefs extends MyRemote {
    public String concatRemotes(Container a, Container b);
    public Container concatReturnRemote(String a, String b, String host, int port, int key);
}
