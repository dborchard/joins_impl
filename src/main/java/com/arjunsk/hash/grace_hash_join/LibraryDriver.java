package com.arjunsk.hash.grace_hash_join;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LibraryDriver {
    // NOTE: Run using Java 1.7 or Java 11
    public static void main(String[] args) {

        String leftCsvFile = "/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/left_input.csv";
        String rightCsvFile = "/Users/xvamp/IdeaProjects/joins_impl/src/main/resources/right_input.csv";
        String joinColumnName = "ID";

        SparkSession spark = SparkSession.builder()
                .appName("Grace Hash Map Join")
                .master("local") // Use local for testing, remove or change for production
                .getOrCreate();

        // Load CSV files into DataFrames
        Dataset<Row> leftDataFrame = spark.read().option("header", "true").csv(leftCsvFile);
        Dataset<Row> rightDataFrame = spark.read().option("header", "true").csv(rightCsvFile);

        // Perform the join operation
        Dataset<Row> joinedDataFrame = leftDataFrame.join(rightDataFrame, leftDataFrame.col(joinColumnName).equalTo(rightDataFrame.col(joinColumnName)));

        // Show the result
        joinedDataFrame.show();
        /*

        Output:
        +---+-----------+---+---+---------------+
        | ID|       Name|Age| ID|     Department|
        +---+-----------+---+---+---------------+
        |  1|   John Doe| 30|  1|    Engineering|
        |  1|   John Doe| 30|  1|      Marketing|
        |  2| Jane Smith| 25|  2|Human Resources|
        |  3|Emily Davis| 22|  3|      Marketing|
        +---+-----------+---+---+---------------+
        */



        spark.stop();
    }
}
