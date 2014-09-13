package migratable.managers;

import migratable.network.ChildNamer;
import migratable.network.HostPort;
import migratable.processes.MigratableProcess;
import migratable.processes.ProcessState;
import migratable.protocols.ProcessToChild;
import migratable.protocols.ProcessToChildResponse;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Derek on 9/4/2014.
 */
public class ProcessManager {
    private Map<String, ProcessState> processes;
    private Map<String, HostPort> children;
    private ProcessManagerListener server;
    private ChildNamer namer;

    public ProcessManager() {
        processes = new ConcurrentHashMap<String, ProcessState>();
        children = new ConcurrentHashMap<String, HostPort>();
        namer = new ChildNamer();
        server = new ProcessManagerListener(processes, children, namer);
    }

    public void printStates() {
        Collection<ProcessState> allProcesses = processes.values();
        if(allProcesses.size() == 0) {
            System.out.println("No processes currently running.");
            return;
        }
        for(ProcessState process: allProcesses) {
            System.out.println("Process: " + process.getName() + ", " +
                    "Command: " + process.getCommand() + ", " +
                    "Child: " + process.getChild() + ", " +
                    "Status: " + process.getSuspended());
        }
    }

    public void printChildren() {
        if(children.size() == 0) {
            System.out.println("No child nodes have been started.");
            return;
        }
        Iterator<Map.Entry<String, HostPort>> iterator = children.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<String, HostPort> kv = iterator.next();
            System.out.println("Child: " + kv.getKey() + ", Server: " + kv.getValue().toString());
        }
    }

    public void printHelp() {
        System.out.println("To run: 'run <childname> <ClassName> <args[]>'");
        System.out.println("To suspend: 'suspend <processname>'");
        System.out.println("To resume: 'resume <processname>'");
        System.out.println("To kill: 'kill <processname>'");
        System.out.println("To migratable: 'migratable <processname> <destchild>'");
        System.out.println("To see currently connection children: 'list children'");
        System.out.println("To see currently running processes: 'list processes'");
        System.out.println("To get port of server: 'getport'");
        System.out.println("To quit and close children/processes: 'quit'");
    }

    public void startServer() {
        Thread t = new Thread(server);
        t.start();
    }

    public int getPort() {
        return server.getPort();
    }

    private ProcessToChildResponse sendRequestToChild(ProcessToChild request, HostPort child) {
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        ProcessToChildResponse response = null;
        try {
            socket = new Socket(child.getHost(), child.getPort());
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());
            oos.writeObject(request);
            oos.flush();
            response = (ProcessToChildResponse) ois.readObject();
        } catch (Exception e) {
            // ignore
        } finally {
            try {
                if (oos != null) oos.close();
                if (ois != null) ois.close();
                if (socket != null && !socket.isClosed()) socket.close();
            } catch (Exception e) {
                // ignore
            }
        }
        return response;
    }

    public boolean parseCommand(String cmd) {
        if(cmd == null) {
            return false;
        }

        String[] splits = cmd.split("\\s+");

        if(cmd.equals("") || splits.length == 0) {
            return false;
        }
        else if(splits[0].equals("run")) {
            if(splits.length < 4) {
                System.out.println("Not enough arguments.");
                return false;
            }
            String[] params = Arrays.copyOfRange(splits, 3, splits.length);

            // Creates the specified object using reflection.
            MigratableProcess process = null;
            try {
                Class c = Class.forName("migratable.processes." + splits[2]);
                Constructor ctor = c.getDeclaredConstructors()[0];
                ctor.setAccessible(true);
                process = (MigratableProcess) ctor.newInstance((Object) params);
            } catch (ClassNotFoundException e) {
                System.out.println(splits[2] + " is not a valid class.");
                return false;
            } catch (Exception e) {
                System.out.println("Invalid arguments");
                e.printStackTrace();
                return false;
            }

            // Checks if the given process name already exists
            String name = process.getName();
            String command = process.getCommand();
            if(processes.containsKey(name)) {
                System.out.println("A process with the name '" + name + "' already exists. Try a different name.");
                return false;
            }

            // Checks if the given child name is valid
            HostPort child = children.get(splits[1]);
            if(child == null) {
                System.out.println("No child with the name '" + splits[1] + "' exists. Try again.");
                return false;
            }

            // Constructs request and send to appropriate child
            ProcessToChild request = new ProcessToChild("run", name, process);
            ProcessToChildResponse response = sendRequestToChild(request, child);
            if(response == null || !response.getSuccess()) {
                System.out.println("New process failed.");
            }
            else {
                processes.put(name, new ProcessState(name, command, "running", splits[1]));
                System.out.println("Process '" + name + "' successfully started on " + splits[1]);
            }
            return false;
        }
        else if(splits[0].equals("suspend")) {
            if(splits.length < 2) {
                System.out.println("Not enough arguments.");
                return false;
            }
            ProcessState processState = processes.get(splits[1]);
            if(processState == null) {
                System.out.println("No process with the name '" + splits[1] + "' exists. Try again.");
                return false;
            }
            HostPort child = children.get(processState.getChild());
            ProcessToChild request = new ProcessToChild("suspend", splits[1], null);
            ProcessToChildResponse response = sendRequestToChild(request, child);
            if(response == null || !response.getSuccess()) {
                System.out.println("Suspend process failed. Process already finished running.");
            }
            else {
                processState.setSuspended("suspended");
                System.out.println("Process '" + splits[1] + "' successfully suspended");
            }
            return false;
        }
        else if(splits[0].equals("resume")) {
            if(splits.length < 2) {
                System.out.println("Not enough arguments.");
                return false;
            }
            ProcessState processState = processes.get(splits[1]);
            if(processState == null) {
                System.out.println("No process with the name '" + splits[1] + "' exists. Try again.");
                return false;
            }
            HostPort child = children.get(processState.getChild());
            ProcessToChild request = new ProcessToChild("resume", splits[1], null);
            ProcessToChildResponse response = sendRequestToChild(request, child);
            if(response == null || !response.getSuccess()) {
                System.out.println("Resume process failed. Process already finished running.");
            }
            else {
                processState.setSuspended("running");
                System.out.println("Process '" + splits[1] + "' successfully resumed");
            }
            return false;
        }
        else if(splits[0].equals("kill")) {
            if(splits.length < 2) {
                System.out.println("Not enough arguments.");
                return false;
            }
            ProcessState processState = processes.get(splits[1]);
            if(processState == null) {
                System.out.println("No process with the name '" + splits[1] + "' exists. Try again.");
                return false;
            }
            HostPort child = children.get(processState.getChild());
            ProcessToChild request = new ProcessToChild("kill", splits[1], null);
            ProcessToChildResponse response = sendRequestToChild(request, child);
            if(response == null || !response.getSuccess()) {
                System.out.println("Kill process failed. Process already finished running.");
            }
            else {
                System.out.println("Process '" + splits[1] + "' successfully killed");
            }
            return false;
        }
        else if(splits[0].equals("migrate")) {
            if(splits.length < 3) {
                System.out.println("Not enough arguments.");
                return false;
            }
            ProcessState processState = processes.get(splits[1]);
            if(processState == null) {
                System.out.println("No process with the name '" + splits[1] + "' exists. Try again.");
                return false;
            }
            HostPort childSource = children.get(processState.getChild());
            HostPort childDest = children.get(splits[2]);
            if(childDest == null) {
                System.out.println("No child with the name '" + splits[2] + "' exists. Try again.");
                return false;
            }

            ProcessToChild request = new ProcessToChild("migrate", splits[1], null);
            ProcessToChildResponse response = sendRequestToChild(request, childSource);
            if(response == null || !response.getSuccess()) {
                System.out.println("Migrate process failed. Process already finished running.");
                return false;
            }
            MigratableProcess process = response.getProcess();
            ProcessToChild migrate = new ProcessToChild("run", splits[1], process);
            response = sendRequestToChild(migrate, childDest);
            if(response == null || !response.getSuccess()) {
                System.out.println("Failed to start process on " + splits[2]);
            }
            else {
                processes.get(splits[1]).setSuspended("running");
                processes.get(splits[1]).setChild(splits[2]);
                System.out.println("Process '" + splits[1] + "' successfully migrated to " + splits[2]);
            }
        }
        else if(splits[0].equals("list")) {
            if(splits.length < 2) {
                System.out.println("Not enough arguments.");
                return false;
            }
            if(splits[1].equals("processes")) {
                printStates();
            }
            else if(splits[1].equals("children")) {
                printChildren();
            }
            else {
                System.out.println("Bad argument");
            }
            return false;
        }
        else if(splits[0].equals("help")) {
            printHelp();
            return false;
        }
        else if(splits[0].equals("quit")) {
            for(HostPort child: children.values()) {
                ProcessToChild request = new ProcessToChild("shutdown", null, null);
                sendRequestToChild(request, child);
            }
            return true;
        }
        else {
            System.out.println("'" + splits[0] + "' is not a recognized command");
        }
        return false;
    }

    public static void main(String[] args) {
        ProcessManager processManager = new ProcessManager();
        processManager.startServer();

        // Wait until ProcessManager starts up the server
        while(processManager.getPort() == 0);
        int port = processManager.getPort();
        Scanner scan = new Scanner(System.in);
        while(true) {
            System.out.print("master@" + Integer.toString(port) + " > ");
            String input = scan.nextLine();
            boolean quit = processManager.parseCommand(input);
            if(quit) break;
        }
        System.exit(0);
    }
}
