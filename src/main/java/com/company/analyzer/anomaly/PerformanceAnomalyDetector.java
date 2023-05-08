package com.company.analyzer.anomaly;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class PerformanceAnomalyDetector implements AnomalyDetector {
    @Override
    public Dataset<Row> detect(Dataset<Row> schema) {
        return systemLoadByWindow(schema);
    }

    private Dataset<Row> systemLoadByWindow(Dataset<Row> schema) {

        Dataset<Row> window = schema.groupBy(
                window(col("timestamp"), "60 minutes", "30 minutes")
        ).count().sort(desc("count"));
        double averageNumOfRequests = window.agg(avg(col("count"))).first().getDouble(0);
        window = window.withColumn("load",
                when(col("count").gt(averageNumOfRequests + averageNumOfRequests * 0.75), lit("HIGH LOAD"))
                .when(col("count").lt(averageNumOfRequests - averageNumOfRequests * 0.75), lit("LOW LOAD"))
                .otherwise("AVERAGE LOAD"));
        Dataset<Row> filter = window.filter(col("load").notEqual("AVERAGE LOAD"));
        Dataset<Row> anomalies = filter.withColumn("anomaly_message",
                format_string("Unexpected %s was detected from %s to %s, number of requests equal to %s", col("load"),
                        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss"), date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss"), col("count")));
        anomalies = anomalies.drop("window", "count", "load");

        return anomalies;
    }
}
