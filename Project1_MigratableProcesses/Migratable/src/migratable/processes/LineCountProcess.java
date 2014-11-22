package migratable.processes;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;

import migratable.io.TransactionalFileInputStream;
import migratable.io.TransactionalFileOutputStream;

/**
 * Created by Derek on 9/10/2014.
 *
 * Prints the line numbers of the input file to the output file.
 *
 */
public class LineCountProcess implements MigratableProcess {

  private TransactionalFileInputStream inFile;
  private TransactionalFileOutputStream outFile;
  private String name, command;
  private int counter;

  private volatile boolean suspending;
  private volatile boolean done;

  public LineCountProcess(String args[]) throws Exception {
    if (args.length < 3) {
      System.out.println("usage: LineCountProcess <name> <inputFile> <outputFile>");
      throw new Exception("Invalid Arguments");
    }

    counter = 0;
    name = args[0];
    command = "LineCountProcess " + name + " " + args[1] + " " + args[2];
    inFile = new TransactionalFileInputStream(args[1]);
    outFile = new TransactionalFileOutputStream(args[2], true);
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

        out.println(Integer.toString(counter));
        counter++;

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
      System.out.println("LineCountProcess: Error: " + e);
      e.printStackTrace();
    }

    suspending = false;
  }

  public void suspend() {
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
