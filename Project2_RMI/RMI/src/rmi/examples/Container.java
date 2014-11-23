package rmi.examples;

/**
 * Created by Derek on 10/4/2014.
 */
public interface Container extends MyRemote {
    public Object getObject();
    public void setObject(Object o);
}
