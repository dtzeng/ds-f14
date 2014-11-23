package rmi.tests;

import rmi.communication.RemoteObjectRef;
import rmi.examples.ConcatRefs;
import rmi.examples.Container;
import rmi.server.LocateRegistry;
import rmi.server.Registry;

/**
 * Created by Derek on 10/4/2014.
 */
public class ConcatRefsTest {
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String serviceName1 = "Container 1";
        String serviceName2 = "Container 2";
        String serviceName3 = "Concatter";


        Registry registry = LocateRegistry.getRegistry(host, port);
        System.out.println("Located registry at " + registry.getHost() + ":" + Integer.toString(registry.getPort()));

        registry.bind(serviceName1, new RemoteObjectRef(host, port, 10, "Container"));
        System.out.println("Registered new Container reference for service: " + serviceName1);

        registry.bind(serviceName2, new RemoteObjectRef(host, port, 11, "Container"));
        System.out.println("Registered new Container reference for service: " + serviceName2);

        registry.bind(serviceName3, new RemoteObjectRef(host, port, 12, "ConcatRefs"));
        System.out.println("Registered new ConcatRefs reference for service: " + serviceName3);


        RemoteObjectRef ref1 = registry.lookup(serviceName1);
        System.out.println("Successfully retrieved reference to: " + serviceName1);

        RemoteObjectRef ref2 = registry.lookup(serviceName2);
        System.out.println("Successfully retrieved reference to: " + serviceName2);

        RemoteObjectRef ref3 = registry.lookup(serviceName3);
        System.out.println("Successfully retrieved reference to: " + serviceName3);

        Container stub1 = (Container) ref1.localize();
        Container stub2 = (Container) ref2.localize();
        ConcatRefs stub3 = (ConcatRefs) ref3.localize();

        stub1.setObject("hello ");
        stub2.setObject("world");

        System.out.println("Testing marshalling of remote object arguments...");
        System.out.println((stub3.concatRemotes(stub1, stub2)).equals("hello world") ? "Passed." : "Failed.");

        System.out.println("Testing return of remote object...");
        Container result = stub3.concatReturnRemote("remote ", "testing", host, port, 15);
        System.out.println(result.getObject().equals("remote testing") ? "Passed." : "Failed.");
    }
}
