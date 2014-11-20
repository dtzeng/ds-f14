package mapr.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Derek on 11/12/2014.
 */
public class FileInfo implements Serializable {
    List<ConcurrentLinkedQueue<String>> partitions;
    int numPartitions, numRecords;

    public FileInfo(int numPartitions, int numRecords) {
        partitions = Collections.synchronizedList(new ArrayList<ConcurrentLinkedQueue<String>>(numPartitions));
        for(int x = 0; x < numPartitions; x++) {
            partitions.add(new ConcurrentLinkedQueue<String>());
        }
        this.numPartitions = numPartitions;
        this.numRecords = numRecords;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getNumRecords() {
        return numRecords;
    }

    public String getReplicaLocation(int partition) {
        ConcurrentLinkedQueue<String> replicaLocations = partitions.get(partition);
        String location = replicaLocations.remove();
        replicaLocations.add(location);
        return location;
    }

    public void addReplicaLocation(int partition, String worker) {
        partitions.get(partition).add(worker);
    }

    public void removeReplicaLocation(int partition, String worker) {
        partitions.get(partition).remove(worker);
    }

    public void removeWorker(String worker) {
        for(int x = 0; x < numPartitions; x++) {
            partitions.get(x).remove(worker);
        }
    }

    public String toString() {
        String result = "";
        Iterator<ConcurrentLinkedQueue<String>> iter = partitions.iterator();
        while(iter.hasNext()) {
            Iterator<String> locs = iter.next().iterator();
            while(locs.hasNext()) {
                result += locs.next();
                if(locs.hasNext()) result += ", ";
            }
            if(iter.hasNext()) result += "|";
        }
        return result;
    }
}
