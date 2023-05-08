package com.company.parser;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ApacheWebLogDatasetGenerator implements LogDatasetGenerator {

    public static final String TIMESTAMP_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";

    @Override
    public Dataset<Row> getDatasetFromFile(SparkSession sparkSession, String filePath) {
        Dataset<Row> schema = sparkSession.read()
                .textFile(filePath)
                .select(
                        to_timestamp(regexp_extract(col("value"), "^.*\\[([\\w:/]+\\s[+\\-]\\d{4})\\]", 1), TIMESTAMP_FORMAT).alias("timestamp"),
                        regexp_extract(col("value"), "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", 0).as("ip"),
                        regexp_extract(col("value"), "https?(:\\/?\\/?)[^\\s]+", 0).as("request"),
                        trim(regexp_extract(col("value"), "\\s\\/(\\/?\\/?)[^\\s]+", 0)).as("endpoint"),
                        trim(regexp_extract(col("value"), "\\s(?:2\\d{2}|3\\d{2}|4\\d{2}|5\\d{2})", 0)).cast("int").as("response code"),
                        regexp_extract(col("value"), "(GET|POST|PUT|DELETE)", 0).as("method"),
                        regexp_extract(col("value"), "[a-zA-Z0-9-_]{40}", 0).as("user_id")
                )
                .withColumn("file", input_file_name())
                .cache();
        return schema;
    }
}
