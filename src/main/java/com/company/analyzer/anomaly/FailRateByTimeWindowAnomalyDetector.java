package com.company.analyzer.anomaly;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class FailRateByTimeWindowAnomalyDetector implements AnomalyDetector{
    @Override
    public Dataset<Row> detect(Dataset<Row> schema) {
        Dataset<Row> totalForWindow = schema.groupBy(
                window(col("timestamp"), "30 minutes", "15 minutes")
        ).count().sort(desc("count")).toDF("window", "totalCount");
        Dataset<Row> failedLogsForWindow = schema
                .filter(col("response code").isNotNull().and(col("response code").geq(400)))
                .groupBy(window(col("timestamp"), "30 minutes", "15 minutes"))
                .count()
                .toDF("window2", "failedCount");
        Dataset<Row> joined = totalForWindow.join(failedLogsForWindow,
                totalForWindow.col("window").equalTo(failedLogsForWindow.col("window2")));
        Dataset<Row> result = joined
                .withColumn("failure rate", round(col("failedCount").divide(col("totalCount")).multiply(100)))
                .sort(desc("failure rate"), desc("totalCount"))
                .filter(col("failure rate").geq(3))
                .drop("window2");
        result = result.withColumn("anomaly_message",
                        format_string("Fail rate increased more than 3%% (actual{fail rate=%s, request count=%s, failed=%s }) in time from %s to %s",
                                col("failure rate"), col("totalCount"),  col("failedCount"),
                                date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"),
                                date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss")))
                .drop("window", "totalCount", "failedCount", "failure rate");
        return result;
    }
}
