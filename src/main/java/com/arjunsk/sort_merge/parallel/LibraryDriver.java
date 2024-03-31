package com.arjunsk.sort_merge.parallel;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class LibraryDriver {
    public static void main(String[] args) {
        // Initialize SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("Parallel Sort")
                .config("spark.master", "local") // Use "local" for testing. For cluster mode, set your cluster's master.
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Example data
        List<Integer> data = Arrays.asList(5, 1, 3, 2, 4);

        // Parallelize the data and create an RDD
        JavaRDD<Integer> distData = jsc.parallelize(data);

        // Sort the RDD
        JavaRDD<Integer> sortedData = distData.sortBy(
            new Function<Integer, Integer>() {
                public Integer call(Integer value) {
                    return value;
                }
            }, true, 1); // The second argument (true) specifies ascending order. Change to false for descending.

        // Collect and print the sorted data
        List<Integer> sortedList = sortedData.collect();
        System.out.println(sortedList);

        // Stop the Spark context
        jsc.stop();
    }
}
