package rmi.server;

import rmi.communication.RemoteObjectRef;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;

/**
 * Created by Derek on 10/1/2014.
 */
public class RegistryServeRequest implements Runnable {
    Socket socket;
    private Map<String, RemoteObjectRef> refs;

    public RegistryServeRequest(Socket socket, Map refs) {
        this.socket = socket;
        this.refs = refs;
    }

    @Override
    public void run() {
        BufferedReader in = null;
        PrintWriter out = null;
        try {
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);

            String serviceName, host;
            int port, key;
            String interfaceName;

            String request = in.readLine();
            if(request.equals("bind")) {
                serviceName = in.readLine();
                host = in.readLine();
                port = Integer.parseInt(in.readLine());
                key = Integer.parseInt(in.readLine());
                interfaceName = in.readLine();
                if(refs.containsKey(serviceName)) {
                    out.println(false);
                }
                else {
                    refs.put(serviceName, new RemoteObjectRef(host, port, key, interfaceName));
                    out.println(true);
                }
            }
            else if(request.equals("lookup")) {
                serviceName = in.readLine();
                RemoteObjectRef ref = refs.get(serviceName);
                if(ref == null) {
                    out.println(false);
                }
                else {
                    out.println(true);
                    out.println(ref.getHost());
                    out.println(ref.getPort());
                    out.println(ref.getKey());
                    out.println(ref.getInterfaceName());
                }
            }
            else if(request.equals("rebind")) {
                serviceName = in.readLine();
                host = in.readLine();
                port = Integer.parseInt(in.readLine());
                key = Integer.parseInt(in.readLine());
                interfaceName = in.readLine();
                if(refs.containsKey(serviceName)) {
                    refs.put(serviceName, new RemoteObjectRef(host, port, key, interfaceName));
                    out.println(true);
                }
                else {
                    out.println(false);
                }
            }
            else if(request.equals("unbind")) {
                serviceName = in.readLine();
                if(refs.containsKey(serviceName)) {
                    refs.remove(serviceName);
                    out.println(true);
                }
                else {
                    out.println(false);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(in != null) in.close();
                if(out != null) out.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
