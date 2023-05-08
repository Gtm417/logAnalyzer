package com.company.analyzer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.round;

public class StatisticAnalyzer {
    private static final int MINUTES_IN_DAY = 1440;


    public Dataset<Row> gatherStats(Dataset<Row> schema) {
        Dataset<Row> schemaPerDay = schema
                .withColumn("date", to_date(col("timestamp")));

        Dataset<Row> statisticResultPerDay =  schemaPerDay.groupBy("date")
                    .agg(
                            count("*").as("totalRequests"),
                            countDistinct("user_id").as("uniqueUsers"),
                            sum(when(col("response code").lt(400), 1).otherwise(0)).as("success"),
                            sum(when(col("response code").geq(400), 1).otherwise(0)).as("totalErrors"),
                            sum(when(col("response code").geq(400).and(col("response code").lt(500)), 1).otherwise(0)).as("clientErrors"),
                            sum(when(col("response code").geq(500), 1).otherwise(0)).as("serverErrors")
                    )
                    .withColumn("fail rate", round(col("totalErrors").divide(col("totalRequests")).multiply(100), 1))
                    .withColumn("requests/minute", round(col("totalRequests").divide(MINUTES_IN_DAY),1));
        Dataset<Row> statisticResult =
                schemaPerDay.groupBy("date").count().withColumnRenamed("count", "numberOfLogs");
        statisticResult = statisticResult.join(statisticResultPerDay, "date");
        return statisticResult;
    }
}
