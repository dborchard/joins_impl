package com.arjunsk.sort_merge.parallel;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ParallelMergeSort {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Parallel Merge Sort")
                .config("spark.master", "local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Step 0: Input Data
        List<Integer> data = new ArrayList<>();
        data.add(5);
        data.add(1);
        data.add(9);
        data.add(3);
        data.add(7);
        data.add(4);
        data.add(6);
        data.add(2);
        data.add(8);


        // Step 1: Sample data to determine ranges for partitioning
        JavaRDD<Integer> dataRDD = sc.parallelize(data);
        JavaRDD<Integer> sampledData = dataRDD.sample(false, 0.1);
        List<Integer> collectedSample = sampledData.collect();
        List<Integer> mutableSample = new ArrayList<>(collectedSample);
        Collections.sort(mutableSample);


        // Step 2 (Pass 0): Partition the RDD based on the defined ranges
        // Do shuffling based on the range data is associated with.
        JavaPairRDD<Integer, Integer> pairRDD = dataRDD.mapToPair(
                (PairFunction<Integer, Integer, Integer>) x -> {
                    int partition = determinePartition(x, mutableSample);
                    return new Tuple2<>(partition, x);
                });
        // Shuffle and sort within partitions
        JavaPairRDD<Integer, Integer> partitionedAndSorted = pairRDD
                .partitionBy(new Partitioner() {
                    @Override
                    public int numPartitions() {
                        return mutableSample.size();
                    }

                    @Override
                    public int getPartition(Object key) {
                        return (int) key;
                    }
                })
                .mapPartitionsToPair(iter -> {
                    // Read the Machine's data and sort locally
                    List<Tuple2<Integer, Integer>> partitionData = new ArrayList<>();
                    iter.forEachRemaining(partitionData::add);
                    partitionData.sort((t1, t2) -> {
                        //sort by the second element of the tuple, which is the actual data value
                        return t1._2.compareTo(t2._2);
                    });
                    return partitionData.iterator();
                }, true);


        // Pass 1: Merge sort results from all partitions
        // This could involve collecting sorted partitions to a single machine or performing a distributed merge
        List<Tuple2<Integer, Integer>> sortedData = partitionedAndSorted.collect();
        System.out.println("Size: "+sortedData.size());
        sortedData.forEach(tuple -> System.out.println("Partition: " + tuple._1 + ", Value: " + tuple._2));

        sc.close();
    }

    private static int determinePartition(Integer x, List<Integer> rangeBoundaries) {
        int partition = 0;
        for (int boundary : rangeBoundaries) {
            if (x < boundary) {
                return partition;
            }
            partition++;
        }
        return Math.min(partition, rangeBoundaries.size() - 1); // Ensure it does not exceed the max index
    }
}
