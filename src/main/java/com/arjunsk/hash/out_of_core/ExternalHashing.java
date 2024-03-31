package com.arjunsk.hash.out_of_core;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class ExternalHashing {
    private static final int NUM_BUCKETS = 10; // Initial number of buckets
    private static final String BUCKET_PREFIX = "bucket_";
    private static final String BUCKET_SUFFIX = ".txt";
    private static Map<Integer, Path> keyToBucketMap = new HashMap<>(); // Map to track key hash to bucket path

    public static void main(String[] args) throws IOException {
        String inputFilePath = "/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/left_input_unsorted.csv"; // Adjust to your dataset path
        externalHashing(inputFilePath);
    }

    public static void externalHashing(String inputFilePath) throws IOException {
        // 1. Partitioning to smaller buckets
        List<Path> buckets = createBuckets();
        distributeRecords(inputFilePath, buckets);

        // 2. Rehashing to individual key based buckets.
        rehashBuckets(buckets);

        // 3. Misc
        printBuckets(new ArrayList<>(keyToBucketMap.values()));
        deleteBuckets(new ArrayList<>(keyToBucketMap.values()));
    }

    private static List<Path> createBuckets() throws IOException {
        List<Path> buckets = new ArrayList<>(NUM_BUCKETS);
        for (int i = 0; i < NUM_BUCKETS; i++) {
            Path bucket = Files.createTempFile(BUCKET_PREFIX + i + "_", BUCKET_SUFFIX);
            buckets.add(bucket);
        }
        return buckets;
    }

    private static void distributeRecords(String inputFilePath, List<Path> buckets) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            String record;
            while ((record = reader.readLine()) != null) {
                int bucketIndex = Math.abs(record.hashCode()) % NUM_BUCKETS;
                Path bucket = buckets.get(bucketIndex);
                try (BufferedWriter writer = Files.newBufferedWriter(bucket, StandardOpenOption.APPEND)) {
                    writer.write(record);
                    writer.newLine();
                }
            }
        }
    }

    private static void rehashBuckets(List<Path> buckets) throws IOException {
        for (Path bucket : buckets) {
            List<String> records = Files.readAllLines(bucket);
            for (String record : records) {
                int keyHash = record.hashCode();
                Path keyBucket = keyToBucketMap.get(keyHash);
                if (keyBucket == null) {
                    keyBucket = Files.createTempFile(BUCKET_PREFIX + keyHash + "_", BUCKET_SUFFIX);
                    keyToBucketMap.put(keyHash, keyBucket);
                }
                try (BufferedWriter writer = Files.newBufferedWriter(keyBucket, StandardOpenOption.APPEND)) {
                    writer.write(record);
                    writer.newLine();
                }
            }
            Files.delete(bucket); // Delete the old bucket
        }
    }

    private static void printBuckets(List<Path> buckets) throws IOException {
        for (Path bucket : buckets) {
            System.out.println("Contents of " + bucket.getFileName() + ":");
            Files.lines(bucket).forEach(System.out::println);
            System.out.println();
        }
    }

    private static void deleteBuckets(List<Path> buckets) throws IOException {
        for (Path bucket : buckets) {
            Files.deleteIfExists(bucket);
        }
    }
}
