package br.sc.senai.datasciencejava;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class dataScience {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("DataScience").master("local[*]").getOrCreate();

        Dataset<Row> dataset = spark.read()
                .format("com.crealytics.spark.excel")
                .option("header", "true")
                .load("telecom_users.csv");

        dataset.show();

        spark.stop();
    }
}
