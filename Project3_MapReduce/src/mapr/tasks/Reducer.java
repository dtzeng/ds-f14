package mapr.tasks;

import mapr.io.KVReader;
import mapr.io.KVWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Derek on 11/16/2014.
 */
public abstract class Reducer implements Task {
    List<String> inputFiles;
    KVWriter writer;
    String otherArgs;
    boolean success;

    public Reducer(List<String> inputFiles, String outputFile, String otherArgs) throws FileNotFoundException {
        this.inputFiles = inputFiles;
        this.writer = new KVWriter(outputFile);
        this.otherArgs = otherArgs;
        this.success = true;
    }

    public String getOtherArgs() {
        return otherArgs;
    }

    public boolean isSuccess() {
        return success;
    }

    public void emit(String key, String value) throws IOException {
        writer.writeKV(key, value);
    }

    private int minIndex(List<Map.Entry<String, String>> list) {
        int minIndex = 0;
        Map.Entry<String, String> minKV = list.get(0);
        for(int x = 1; x < list.size(); x++) {
            if(compareTo(list.get(x).getKey(), minKV.getKey()) < 0) {
                minIndex = x;
                minKV = list.get(x);
            }
        }
        return minIndex;
    }

    @Override
    public void run() {
        List<KVReader> readers = new ArrayList<KVReader>();
        try {
            List<Map.Entry<String, String>> kvs = new ArrayList<Map.Entry<String, String>>();
            for(int x = 0; x < inputFiles.size(); x++) {
                KVReader reader = new KVReader(inputFiles.get(x));
                Map.Entry<String, String> kv = reader.readNextKV();
                if(kv != null) {
                    readers.add(reader);
                    kvs.add(kv);
                }
            }

            while(readers.size() != 0) {
                int minIndex = minIndex(kvs);
                Map.Entry<String, String> minKV = kvs.get(minIndex);

                // Replace minKV
                Map.Entry<String, String> replacement = readers.get(minIndex).readNextKV();
                if(replacement == null) {
                    readers.remove(minIndex);
                    kvs.remove(minIndex);
                }
                else {
                    kvs.set(minIndex, replacement);
                }

                // Aggregate same keys
                ArrayList<String> groupMinKey = new ArrayList<String>();
                groupMinKey.add(minKV.getValue());

                for(int x = 0; x < kvs.size(); x++) {
                    while(kvs.get(x) != null && compareTo(kvs.get(x).getKey(), minKV.getKey()) == 0) {
                        groupMinKey.add(kvs.get(x).getValue());
                        Map.Entry<String, String> replace = readers.get(x).readNextKV();
                        kvs.set(x, replace);
                        if(replace == null) {
                            readers.get(x).close();
                        }
                    }
                }

                // Delete completed files
                Iterator<KVReader> readerIterator = readers.iterator();
                Iterator<Map.Entry<String, String>> kvsIterator = kvs.iterator();
                while(kvsIterator.hasNext()) {
                    readerIterator.next();
                    Map.Entry<String, String> kv = kvsIterator.next();
                    if(kv == null) {
                        readerIterator.remove();
                        kvsIterator.remove();
                    }
                }

                reduce(minKV.getKey(), groupMinKey.iterator());
            }

        } catch (Exception e) {
            success = false;
            e.printStackTrace();
        } finally {
            for(KVReader reader: readers) {
                reader.close();
            }
            writer.close();
        }
    }

    public abstract void reduce(String key, Iterator<String> values) throws Exception;
    public abstract int compareTo(String key1, String key2);
}
