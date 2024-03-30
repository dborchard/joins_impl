package com.arjunsk.hash.grace_hash_join;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class SourceCodeDriver {

    // NOTE: Run using Java 1.7 or Java 11
    public static void main(String[] args) {

        String leftCsvFile = "/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/left_input.csv";
        String rightCsvFile = "/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/right_input.csv";
        String joinColumnName = "ID";

        SparkSession spark = SparkSession.builder()
                .appName("Grace Hash Join")
                .master("local")
                .getOrCreate();

        // Read the input files into RDDs
        JavaRDD<String> leftRdd = spark.read().textFile(leftCsvFile).javaRDD();
        JavaRDD<String> rightRdd = spark.read().textFile(rightCsvFile).javaRDD();

        // Assuming the first row is the header and finding the index of the join column
        String leftHeader = leftRdd.first();
        String rightHeader = rightRdd.first();
        int leftColumnIndex = getColumnIndex(leftHeader, joinColumnName);
        int rightColumnIndex = getColumnIndex(rightHeader, joinColumnName);

        // Filter out the header row
        leftRdd = leftRdd.filter(row -> !row.equals(leftHeader));
        rightRdd = rightRdd.filter(row -> !row.equals(rightHeader));

        // 1. Partition Phase
        // Add Partition Hash Key to all the rows
        JavaPairRDD<Integer, String> leftPartitioned = leftRdd.mapToPair(getPartitionMapper(leftColumnIndex));
        JavaPairRDD<Integer, String> rightPartitioned = rightRdd.mapToPair(getPartitionMapper(rightColumnIndex));

        // Create Partitions based on those Hash Keys
        JavaPairRDD<Integer, Iterable<String>> groupedLeft = leftPartitioned.groupByKey();
        JavaPairRDD<Integer, Iterable<String>> groupedRight = rightPartitioned.groupByKey();

        // 2. Join Phase: Build and Probe for each partition
        JavaRDD<String> joinedResults = groupedLeft.cogroup(groupedRight).flatMap(
                new FlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Iterable<String>>, Iterable<Iterable<String>>>>, String>() {
                    @Override
                    public Iterator<String> call(Tuple2<Integer, Tuple2<Iterable<Iterable<String>>, Iterable<Iterable<String>>>> cogrouped) throws Exception {
                        // Build phase for left table. Construct HashTable
                        HashMap<String, List<String>> buildPhase = new HashMap<>();
                        for (Iterable<String> leftRows : cogrouped._2()._1()) {
                            for (String row : leftRows) {
                                String key = row.split(",")[leftColumnIndex];
                                buildPhase.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                            }
                        }

                        // Probe phase for right table
                        ArrayList<String> results = new ArrayList<>();
                        for (Iterable<String> rightRows : cogrouped._2()._2()) {
                            for (String row : rightRows) {
                                String key = row.split(",")[rightColumnIndex];
                                List<String> matchedRows = buildPhase.get(key);
                                if (matchedRows != null) {
                                    for (String leftRow : matchedRows) {
                                        results.add(leftRow + "," + row);
                                    }
                                }
                            }
                        }

                        return results.iterator();
                    }
                });


        joinedResults.collect().forEach(System.out::println);
        /*
     -->Input Left:
        ID,Name,Age
        1,John Doe,30
        2,Jane Smith,25
        3,Emily Davis,22
        4,Michael Brown,40

        Input Right:
        ID,Department
        1,Engineering
        1,Marketing
        2,Human Resources
        3,Marketing
        5,Operations

     -->Output:
        2,Jane Smith,25,2,Human Resources
        3,Emily Davis,22,3,Marketing
        1,John Doe,30,1,Engineering
        1,John Doe,30,1,Marketing
        */

        spark.stop();
    }

    private static PairFunction<String, Integer, String> getPartitionMapper(int columnIndex) {
        return (PairFunction<String, Integer, String>) s -> {
            String[] columns = s.split(",");
            String key = columns[columnIndex];
            int partitionId = key.hashCode() % 10; // Simple hash function for demonstration
            return new Tuple2<>(partitionId, s);
        };
    }

    private static int getColumnIndex(String header, String columnName) {
        String[] columns = header.split(",");
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column name " + columnName + " not found in header");
    }
}
