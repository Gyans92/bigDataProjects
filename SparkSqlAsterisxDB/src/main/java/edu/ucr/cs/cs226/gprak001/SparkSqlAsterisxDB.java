package edu.ucr.cs.cs226.gprak001;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedWriter;
import java.io.FileWriter;



public class SparkSqlAsterisxDB
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSql")
                .master("local")
                .getOrCreate();

        StructType schema = new StructType()
                .add("host", "string")
                .add("logname", "string")
                .add("time", "long")
                .add("method", "string")
                .add("URL", "string")
                .add("response", "long")
                .add("bytes", "long")
                .add("referrer", "string")
                .add("useragent", "string");

        Dataset<Row> log_file = spark.read()
                .option("delimiter", "\t")
                .option("header", "true")
                .option("inferSchema", "true")
                .schema(schema)
                .csv(args[0]);
        log_file.show();
        log_file.createOrReplaceTempView("log_lines");
        Dataset<Row> bytesPerCode =
                spark.sql("SELECT response, sum(bytes)/count(*) from log_lines GROUP BY response");
        bytesPerCode.show();

        try {
            FileWriter filew = new FileWriter("taskA");
            BufferedWriter logWriter = new BufferedWriter(filew);
            for( Row i : bytesPerCode.collectAsList()) {
                logWriter.write("Response : " + i.get(0) + " " + " Average bytes : " + i.get(1) + "\n");
                logWriter.flush();
            }
        } catch (Exception e) {
            System.out.println("write file failed :  " + e);
        }

        //Number of log entries
        long time1 = System.nanoTime();
        Dataset<Row> countEntries = spark.sql("SELECT count(*) FROM log_lines WHERE time BETWEEN 804571201 AND 804571305");
        countEntries.show();
        long time2 = System.nanoTime();
        try {
            FileWriter filew1 = new FileWriter("taskB");
            BufferedWriter logWriter = new BufferedWriter(filew1);
            for( Row i : countEntries.collectAsList()) {
                logWriter.write("Total # logs count within 804571201 AND 804571305 timestamps : " + i.get(0) + "\n" + "total time taken to count : " + (time2-time1)/1000000 + " milliseconds");
                logWriter.flush();
            }
        } catch (Exception e) {
            System.out.println("write file failed :  " + e);
        }
//        countEntries.javaRDD().map(x -> x.toString()).saveAsTextFile("taskB.txt");
    }
}
