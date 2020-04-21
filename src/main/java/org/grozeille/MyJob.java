package org.grozeille;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

@Slf4j
public class MyJob {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");
        System.setProperty("dfs.block.size","134217728");
        System.setProperty("dfs.replication","2");

        SparkSession spark = SparkSession.builder()
                .appName("TestScheduler")
                .master("local[2]")
                .config("spark.eventLog.dir", "hdfs://lenovo01/tmp/spark-logs")
                .config("spark.eventLog.enabled", "true")
                .config("spark.hadoop.dfs.block.size", "134217728")
                .config("spark.hadoop.dfs.replication", "2")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        SchedulerFactory schedFact = new org.quartz.impl.StdSchedulerFactory();

        Scheduler sched = schedFact.getScheduler();

        sched.start();

        // Job 1
        JobDetail job1 = newJob(ScheduledSparkJob.class)
                .withIdentity("sparkJob1", "spark")
                .withDescription("Simulate a first process")
                .build();
        Trigger trigger1 = newTrigger()
                .withIdentity("trigger1", "spark")
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(10)
                        .repeatForever())
                .build();
        sched.scheduleJob(job1, trigger1);

        // Job 2
        JobDetail job2 = newJob(ScheduledSparkJob.class)
                .withIdentity("sparkJob2", "spark")
                .withDescription("Another process")
                .build();
        Trigger trigger2 = newTrigger()
                .withIdentity("trigger2", "spark")
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(30)
                        .repeatForever())
                .build();
        sched.scheduleJob(job2, trigger2);

        // never stop the job
        try {
            while(true) {
                Thread.sleep(1000 * 60);
            }
        }
        catch(InterruptedException e) {
            sched.shutdown();
            spark.stop();
        }
    }
}
