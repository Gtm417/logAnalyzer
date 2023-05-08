package com.company;


import com.company.analyzer.AnomalyAnalyzer;
import com.company.analyzer.StatisticAnalyzer;
import com.company.elastic.ElasticSchemaAppender;
import com.company.parser.ApacheWebLogDatasetGenerator;
import com.company.parser.LogDatasetGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Main {

    private static final String SERVER_LOGS = "C:\\jboss-eap-7.4\\domain\\servers\\EINVOICE-local-server\\log\\access_log*";


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .config("spark.sql.session.timeZone", "UTC")
                .config("es.nodes", "localhost")
                .config("es.port", "9200")
                .config("es.net.http.auth.user", "elastic")
                .config("es.net.http.auth.pass", "elastic")
                .config("es.index.auto.create", "true")
                .config("spark.es.nodes.wan.only", "true")
                .appName("Log Analyzer")
                .getOrCreate();

        LogDatasetGenerator datasetGenerator = new ApacheWebLogDatasetGenerator();
        Dataset<Row> schema =
                datasetGenerator.getDatasetFromFile(spark, SERVER_LOGS);
        ElasticSchemaAppender.saveToEs(schema, "analyzed-logs/index", SaveMode.Overwrite);
        AnomalyAnalyzer anomalyAnalyzer = new AnomalyAnalyzer(spark);
        Dataset<Row> anomaliesResult = anomalyAnalyzer.gatherAnomalies(schema);
        StatisticAnalyzer statisticAnalyzer = new StatisticAnalyzer();
        Dataset<Row> statisticResult = statisticAnalyzer.gatherStats(schema);
        ElasticSchemaAppender.saveToEs(statisticResult, "log-stats/index", SaveMode.Overwrite);
        ElasticSchemaAppender.saveToEs(anomaliesResult, "log-anomalies/index", SaveMode.Overwrite);
        spark.close();
        spark.stop();
    }
}