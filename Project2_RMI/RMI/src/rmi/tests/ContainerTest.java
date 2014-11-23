package rmi.tests;

import rmi.communication.RemoteObjectRef;
import rmi.examples.Container;
import rmi.server.LocateRegistry;
import rmi.server.Registry;

/**
 * Created by Derek on 10/4/2014.
 */
public class ContainerTest {
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String serviceName = "Container";

        Registry registry = LocateRegistry.getRegistry(host, port);
        System.out.println("Located registry at " + registry.getHost() + ":" + Integer.toString(registry.getPort()));

        registry.bind(serviceName, new RemoteObjectRef(host, port, 2, "Container"));
        System.out.println("Registered new Container reference");

        RemoteObjectRef ref = registry.lookup(serviceName);
        System.out.println("Successfully retrieved reference");

        Container containerStub = (Container) ref.localize();

        System.out.println("Remote invocation of getObject should return null: " + containerStub.getObject());
        System.out.println("Remote invocation of setObject to String 'hello'");
        containerStub.setObject("hello");
        System.out.println("Remote invocation of getObject should return 'hello': " + containerStub.getObject());
    }
}
