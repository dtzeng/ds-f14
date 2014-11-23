package rmi.examples;

/**
 * Created by Derek on 10/4/2014.
 */
public class ContainerImpl implements Container {
    public Object obj;

    public ContainerImpl() {}

    public ContainerImpl(Object obj) {
        this.obj = obj;
    }

    @Override
    public synchronized Object getObject() {
        return this.obj;
    }

    @Override
    public synchronized void setObject(Object o) {
        this.obj = o;
    }
}
