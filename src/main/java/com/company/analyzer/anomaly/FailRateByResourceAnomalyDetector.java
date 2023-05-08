package com.company.analyzer.anomaly;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.desc;

public class FailRateByResourceAnomalyDetector implements AnomalyDetector{
    @Override
    public Dataset<Row> detect(Dataset<Row> schema) {
        Dataset<Row> totalByEndpoint = schema.groupBy("endpoint").count()
                .filter((FilterFunction<Row>) row -> (Long) row.getAs("count") > 1).toDF("endpoint", "totalCount");

        Dataset<Row> failedLogsByEndpoint = schema
                .filter(col("response code").isNotNull().and(col("response code").geq(400)))
                .groupBy("endpoint")
                .count()
                .toDF("endpoint1", "failedCount");
        Dataset<Row> joined = totalByEndpoint.join(failedLogsByEndpoint,
                totalByEndpoint.col("endpoint").equalTo(failedLogsByEndpoint.col("endpoint1")));
        Dataset<Row> result = joined
                .withColumn("failure rate", round(col("failedCount").divide(col("totalCount")).multiply(100)))
                .sort(desc("failure rate"), desc("totalCount"))
                .filter(col("failure rate").geq(3))
                .drop("endpoint1");

        result = result.withColumn("anomaly_message",
                format_string("Fail rate increased more than 3%% (actual{fail rate=%s, request count=%s, failed=%s }) for resource %s",
                        col("failure rate"), col("totalCount"),  col("failedCount"), col("endpoint")))
                .drop("endpoint", "totalCount", "failedCount", "failure rate");
        return result;
    }
}
