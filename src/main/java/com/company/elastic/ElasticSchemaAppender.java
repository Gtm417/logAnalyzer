package com.company.elastic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

public class ElasticSchemaAppender {


    public static void saveToEs(Dataset<Row> dataset, String index, SaveMode saveMode) {
        dataset
                .write()
                .format("org.elasticsearch.spark.sql")
                .mode(saveMode)
                .option("es.resource", index)
                .option("es.mapping.date", "timestamp")
                .save();
    }
}
