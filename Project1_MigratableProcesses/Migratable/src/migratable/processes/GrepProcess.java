package migratable.processes;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;

import migratable.io.TransactionalFileInputStream;
import migratable.io.TransactionalFileOutputStream;

/**
 * Created by Derek on 9/8/2014.
 *
 * Searches for a query line by line, and prints any matching lines to the
 * output file.
 *
 */
public class GrepProcess implements MigratableProcess {
    private TransactionalFileInputStream inFile;
    private TransactionalFileOutputStream outFile;
    private String query;
    private String name, command;

    private volatile boolean suspending;
    private volatile boolean done;

    public GrepProcess(String args[]) throws Exception {
	if (args.length < 4) {
	    System.out
		    .println("usage: GrepProcess <name> <queryString> <inputFile> <outputFile>");
	    throw new Exception("Invalid Arguments");
	}

	name = args[0];
	query = args[1];
	command = "GrepProcess " + name + " " + query + " " + args[2] + " "
		+ args[3];
	inFile = new TransactionalFileInputStream(args[2]);
	outFile = new TransactionalFileOutputStream(args[3], true);
    }

    public void run() {
	PrintStream out = new PrintStream(outFile);
	DataInputStream in = new DataInputStream(inFile);

	try {
	    while (!suspending) {
		String line = in.readLine();

		if (line == null || done) {
		    done = true;
		    break;
		}

		if (line.contains(query)) {
		    out.println(line);
		}

		// Make grep take longer so that we don't require extremely
		// large files for interesting results
		try {
		    Thread.sleep(500);
		} catch (InterruptedException e) {
		    // ignore it
		}
	    }
	} catch (EOFException e) {
	    done = true;
	} catch (IOException e) {
	    done = true;
	    System.out.println("GrepProcess: Error: " + e);
	    e.printStackTrace();
	}

	suspending = false;
    }

    public void suspend() {
	suspending = true;
	while (suspending)
	    ;
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
