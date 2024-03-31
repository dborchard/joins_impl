package com.arjunsk.hash.symetric_hash_join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SymmetricHashJoin {
    static class Record {
        String key;
        String value;

        public Record(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    // Symmetric HashTable's
    private final Map<String, List<Record>> hashTableA = new HashMap<>();
    private final Map<String, List<Record>> hashTableB = new HashMap<>();


    private final List<Record> joinedRecords = new ArrayList<>();

    // Processes records from stream A
    public void processStreamA(Record record) {
        if (hashTableB.containsKey(record.key)) {
            // Join with existing records in B
            for (Record recordB : hashTableB.get(record.key)) {
                joinedRecords.add(new Record(record.key, record.value + "|" + recordB.value));
            }
        } else {
            // Add to hashTableA if no join was performed
            hashTableA.computeIfAbsent(record.key, k -> new ArrayList<>()).add(record);
        }
    }

    // Processes records from stream B
    public void processStreamB(Record record) {
        if (hashTableA.containsKey(record.key)) {
            // Join with existing records in A
            for (Record recordA : hashTableA.get(record.key)) {
                joinedRecords.add(new Record(record.key, recordA.value + "|" + record.value));
            }
        } else {
            // Add to hashTableB if no join was performed
            hashTableB.computeIfAbsent(record.key, k -> new ArrayList<>()).add(record);
        }
    }

    public List<Record> getJoinedRecords() {
        return joinedRecords;
    }

    public static void main(String[] args) {
        SymmetricHashJoin joiner = new SymmetricHashJoin();

        // Simulating streaming by processing one record at a time
        joiner.processStreamA(new Record("1", "A1"));
        joiner.processStreamA(new Record("2", "A2"));

        joiner.processStreamB(new Record("2", "B1"));
        joiner.processStreamB(new Record("3", "B2"));

        List<Record> joined = joiner.getJoinedRecords();
        for (Record record : joined) {
            System.out.println(record);
        }
    }
}
