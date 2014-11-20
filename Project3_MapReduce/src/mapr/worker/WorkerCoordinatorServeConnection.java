package mapr.worker;

import mapr.examples.*;
import mapr.master.TaskInfo;
import mapr.tasks.Task;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Derek on 11/12/2014.
 */
public class WorkerCoordinatorServeConnection implements Runnable {
    Socket socket;
    String dfsDir;
    String masterHost;
    int masterPort;
    int recordLength;
    String workerName;
    ConcurrentHashMap<Integer, TaskThread> tasks;

    public WorkerCoordinatorServeConnection(Socket socket, String dfsDir, String masterHost, int masterPort,
                                            int recordLength, String workerName,
                                            ConcurrentHashMap<Integer, TaskThread> tasks) {
        this.socket = socket;
        this.dfsDir = dfsDir;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.recordLength = recordLength;
        this.workerName = workerName;
        this.tasks = tasks;
    }

    public Task createTask(TaskInfo task) {
        Task runner = null;
        try {
            if (task.getJobType().equals("count")) {
                if (task.getTaskType().equals("map")) {
                    runner = new WordCountMapper(task.getInput(), task.getOutput(),
                                                 task.getRecordStart(), task.getRecordEnd(),
                                                 recordLength, task.getOtherArgs());
                }
                else if (task.getTaskType().equals("sort")) {
                    runner = new WordCountSorter(task.getInput(), task.getOutput(), task.getOtherArgs());
                }
                else if (task.getTaskType().equals("reduce")) {
                    runner = new WordCountReducer(task.getFilenames(), task.getOutput(), task.getOtherArgs());
                }

            } else if (task.getJobType().equals("grep")) {
                if (task.getTaskType().equals("map")) {
                    runner = new GrepMapper(task.getInput(), task.getOutput(),
                                            task.getRecordStart(), task.getRecordEnd(),
                                            recordLength, task.getOtherArgs());
                }
                else if (task.getTaskType().equals("sort")) {
                    runner = new GrepSorter(task.getInput(), task.getOutput(), task.getOtherArgs());
                }
                else if (task.getTaskType().equals("reduce")) {
                    runner = new GrepReducer(task.getFilenames(), task.getOutput(), task.getOtherArgs());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return runner;
    }

    public boolean executeTask(TaskInfo taskInfo) {
        Task task = createTask(taskInfo);
        if(task == null)
            return false;
        TaskThread taskThread = new TaskThread(taskInfo, new Thread(task));
        TaskThreadRunner runner = new TaskThreadRunner(taskThread, task, masterHost, masterPort, workerName, tasks);
        new Thread(runner).start();
        return true;
    }

    public void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if(files != null) {
            for(File f: files) {
                if(f.isDirectory())
                    deleteDir(f);
                else
                    f.delete();
            }
        }
        dir.delete();
    }

    @Override
    public void run() {
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
            oos = new ObjectOutputStream(socket.getOutputStream());
            ois = new ObjectInputStream(socket.getInputStream());
            String command = ois.readUTF();
            if(command.equals("newReplica")) {
                String filename = ois.readUTF();
                int fileLen = ois.readInt();
                byte[] contents = new byte[fileLen];
                ois.readFully(contents);
                RandomAccessFile file = new RandomAccessFile(dfsDir + "/" + filename, "rws");
                file.write(contents);
                file.close();

                oos.writeBoolean(true);
                oos.flush();
            }
            else if(command.equals("newTask")) {
                TaskInfo task = (TaskInfo) ois.readObject();
                boolean success = executeTask(task);
                oos.writeBoolean(success);
                oos.flush();
            }
            else if(command.equals("ping")) {
                oos.writeUTF("pong");
                oos.flush();
            }
            else if(command.equals("shutdown")) {
                System.out.println("Received shutdown. Exiting...");
                deleteDir(new File(dfsDir));
                System.exit(-1);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if(oos != null) oos.close();
                if(ois != null) ois.close();
                if(!socket.isClosed()) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
