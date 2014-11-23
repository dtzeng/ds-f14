package rmi.examples;

/**
 * Created by Derek on 10/3/2014.
 */
public interface OpNums extends MyRemote {
    public int add(int x, int y);
    public int sub(int x, int y);
    public int mult(int x, int y);
    public int div(int x, int y);
}
