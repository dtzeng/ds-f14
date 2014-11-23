package rmi.tests;

import rmi.communication.RemoteObjectRef;
import rmi.examples.OpNums;
import rmi.server.LocateRegistry;
import rmi.server.Registry;

/**
 * Created by Derek on 10/4/2014.
 */
public class OpNumsTest {
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String serviceName = "Opper";

        Registry registry = LocateRegistry.getRegistry(host, port);
        System.out.println("Located registry at " + registry.getHost() + ":" + Integer.toString(registry.getPort()));

        registry.bind(serviceName, new RemoteObjectRef(host, port, 1, "OpNums"));
        System.out.println("Registered new OpNums reference");

        RemoteObjectRef ref = registry.lookup(serviceName);
        System.out.println("Successfully retrieved reference");

        OpNums opStub = (OpNums) ref.localize();

        System.out.println("Testing add: 1 + 2. Remote invocation returns " + opStub.add(1, 2));
        System.out.println("Testing sub: 10 - 2. Remote invocation returns " + opStub.sub(10, 2));
        System.out.println("Testing mult: 4 * 0. Remote invocation returns " + opStub.mult(4, 0));
        System.out.println("Testing div: 9 / 3. Remote invocation returns " + opStub.div(9, 3));
    }
}
