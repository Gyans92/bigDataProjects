package edu.ucr.cs.cs226.gprak001;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkRDD
{
    public static void main(String[] args) {
        // configure spark
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read text file to RDD
        JavaRDD<String> lines = sc.textFile(args[0]);

        //Get average bytes per status code.
        JavaPairRDD<Integer, Double> bytesByCode = lines.mapToPair(line -> new Tuple2<>(Integer.parseInt(line.split("\t")[5]), Double.parseDouble(line.split("\t")[6])));
        JavaPairRDD<Integer, Double> reducedBytesByCode = bytesByCode.reduceByKey((a,b) -> a+b);
        Map<Integer, Long> count = bytesByCode.countByKey();
        JavaPairRDD<String, String> avgByCode = reducedBytesByCode.mapToPair(code -> new Tuple2<>("Code : " + code._1, "Average number of Bytes =" + code._2/count.get(code._1)));

        avgByCode.saveAsTextFile("task1.txt");

        //Get pairs of line with same URL, host and time in 3600 ns range.
        JavaPairRDD<String, String> hostPairs = lines.mapToPair(line -> new Tuple2<>(line.split("\t")[0] + line.split("\t")[4], line));
        JavaPairRDD<String, Tuple2<String, String>> joined = hostPairs.join(hostPairs);
        JavaRDD<Tuple2<String, String>> pairs = joined.values().filter(line -> line._1 != line._2 && (Math.abs(Long.parseLong(line._1.split("\t")[2]) - Long.parseLong( line._2.split("\t")[2])) <=3600 ));

        pairs.saveAsTextFile("task2.txt");


    }

}

