package com.company.analyzer;

import com.company.analyzer.anomaly.AnomalyDetector;
import com.company.analyzer.anomaly.FailRateByResourceAnomalyDetector;
import com.company.analyzer.anomaly.FailRateByTimeWindowAnomalyDetector;
import com.company.analyzer.anomaly.PerformanceAnomalyDetector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class AnomalyAnalyzer {
    private static final List<AnomalyDetector> anomalyDetectors =
            List.of(new PerformanceAnomalyDetector(), new FailRateByTimeWindowAnomalyDetector(), new FailRateByResourceAnomalyDetector());

    private SparkSession sparkSession;

    public AnomalyAnalyzer(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> gatherAnomalies(Dataset<Row> schema) {
        StructType struct = new StructType(new StructField[] {
                new StructField("anomaly_message", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> anomalies = sparkSession.createDataFrame(new ArrayList<>(), struct);
        for (AnomalyDetector anomalyDetector : anomalyDetectors) {
           anomalies = anomalies.union(anomalyDetector.detect(schema));
        }
        return anomalies;
    }
}
