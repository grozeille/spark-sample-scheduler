package org.grozeille;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.quartz.*;

@Slf4j
public class ScheduledSparkJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobKey key = jobExecutionContext.getJobDetail().getKey();

        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();

        String table = dataMap.getString("inputTable");
        String partitionFilter = dataMap.getString("inputPartitionFilter");
        String outputFolder = dataMap.getString("outputFolder");
        String outputPartitionName = dataMap.getString("outputPartitionName");
        String outputTable = dataMap.getString("outputTable");

        // run the spark job

        SparkSession spark = SparkSession.active();

        spark.sparkContext().setLocalProperty("callSite.short", "Scheduled job " + key.toString());
        spark.sparkContext().setLocalProperty("callSite.long",
                jobExecutionContext.getJobDetail().getDescription() + "\n" +
                jobExecutionContext.getTrigger().toString() + "\n" +
                jobExecutionContext.getJobDetail().toString());

        Dataset<Row> dataset = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("hdfs://lenovo01/user/root/data-1.txt");

        dataset = dataset.map((MapFunction<Row, Row>)  row -> {
            log.info("Simulate long processing for row: " + row.toString());
            Thread.sleep(1000);
            return row;
        }, Encoders.bean(Row.class));
        long count = dataset.count();
        dataMap.put("count", count);
        log.info("Count: "+ count);
    }
}
