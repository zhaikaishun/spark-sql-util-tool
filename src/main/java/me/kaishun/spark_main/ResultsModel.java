package me.kaishun.spark_main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class ResultsModel {
    private ArrayList<Dataset<Row>> rowDatasetLists;
    private SparkSession spark;

    public ResultsModel(ArrayList<Dataset<Row>> rowDatasetLists, SparkSession spark) {
        this.rowDatasetLists = rowDatasetLists;
        this.spark = spark;
    }
}
