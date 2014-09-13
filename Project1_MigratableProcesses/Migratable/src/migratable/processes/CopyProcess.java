package migratable.processes;

import migratable.io.TransactionalFileInputStream;
import migratable.io.TransactionalFileOutputStream;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Created by Derek on 9/10/2014.
 *
 * Copies a file line by line.
 *
 */
public class CopyProcess implements MigratableProcess {

    private TransactionalFileInputStream inFile;
    private TransactionalFileOutputStream outFile;
    private String name, command;

    private volatile boolean suspending;
    private volatile boolean done;

    public CopyProcess(String args[]) throws Exception
    {
        if (args.length < 3) {
            System.out.println("usage: CopyProcess <name> <inputFile> <outputFile>");
            throw new Exception("Invalid Arguments");
        }

        name = args[0];
        command = "CopyProcess " + name + " " + args[1] + " " + args[2];
        inFile = new TransactionalFileInputStream(args[1]);
        outFile = new TransactionalFileOutputStream(args[2], true);
    }

    public void run()
    {
        PrintStream out = new PrintStream(outFile);
        DataInputStream in = new DataInputStream(inFile);

        try {
            while (!suspending) {
                String line = in.readLine();

                if (line == null || done) {
                    done = true;
                    break;
                }

                out.println(line);

                // Make grep take longer so that we don't require extremely large files for interesting results
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    // ignore it
                }
            }
        } catch (EOFException e) {
            done = true;
        } catch (IOException e) {
            done = true;
            System.out.println ("CopyProcess: Error: " + e);
            e.printStackTrace();
        }

        suspending = false;
    }

    public void suspend()
    {
        suspending = true;
        while (suspending);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getCommand() {
        return command;
    }

    @Override
    public boolean getDone() {
        return done;
    }

    @Override
    public void setDone(boolean b) {
        done = b;
    }
}
