package edu.ucr.cs.cs226.gprak001;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Hello world!
 */
public class KNN {
    public static void main(String[] args) throws Exception {
        System.out.println(args[0]);
        Configuration conf = new Configuration();
        conf.set("args1", args[2]);
        conf.set("args2", args[3]);
        conf.set("k", args[4]);
        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(KNN.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[5]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, DoubleWritable, Text> {

        private DoubleWritable outKey = new DoubleWritable();
        private  Text outVal= new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Double args1 = Double.parseDouble(conf.get("args1"));
            Double args2 = Double.parseDouble(conf.get("args2"));

            if (((LongWritable) key).get() != 0) {
                double xCoord = Double.parseDouble(value.toString().split(",")[1]);
                double yCoord = Double.parseDouble(value.toString().split(",")[2]);
                long id = Long.parseLong(value.toString().split(",")[0]);
                double distanceFromPoint = calculateDistance(args1, args2, xCoord, yCoord);
                outKey.set(distanceFromPoint);
                outVal.set(String.valueOf(xCoord) + "," + String.valueOf(yCoord));
                context.write(outKey, outVal);
            }
        }

        public double calculateDistance(double x1, double y1, double x2, double y2) {
            double n1 = Math.pow(x2 - x1, 2);
            double n2 = Math.pow(y2 - y1, 2);
            double result = Math.pow(n1 + n2, 0.5);
            return result;
        }

    }

    public static class FilterReducer
            extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        int count = 1;

        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Integer k = Integer.parseInt(conf.get("k"));
            if(count <= k) {
                for (Text val : values) {
                    context.write(key, val);
                    count++;
                }
            }
        }
    }
}
