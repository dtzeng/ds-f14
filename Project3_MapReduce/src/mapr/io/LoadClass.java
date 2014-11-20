package mapr.io;

/**
 * Created by Derek on 11/18/2014.
 */
public class LoadClass extends ClassLoader {

    public LoadClass() {
        super();
    }

    public Class<?> defineClass(String name, byte[] contents) {
        Class <?> cls = super.defineClass(name, contents, 0, contents.length);
        super.resolveClass(cls);
        return cls;
    }

    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return super.loadClass(name, resolve);
    }
}
