package rmi.examples;

/**
 * Created by Derek on 10/3/2014.
 */
public class OpNumsImpl implements OpNums {

    public OpNumsImpl() {
    }

    @Override
    public synchronized int add(int x, int y) {
        return x + y;
    }

    @Override
    public synchronized int sub(int x, int y) {
        return x - y;
    }

    @Override
    public synchronized int mult(int x, int y) {
        return x * y;
    }

    @Override
    public synchronized int div(int x, int y) {
        return x / y;
    }
}
