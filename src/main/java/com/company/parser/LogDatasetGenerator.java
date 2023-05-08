package com.company.parser;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface LogDatasetGenerator {

    Dataset<Row> getDatasetFromFile(SparkSession sparkSession, String filePath);
}
