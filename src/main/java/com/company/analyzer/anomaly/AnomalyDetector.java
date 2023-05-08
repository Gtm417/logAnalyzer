package com.company.analyzer.anomaly;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface AnomalyDetector {

    Dataset<Row> detect(Dataset<Row> schema);
}
