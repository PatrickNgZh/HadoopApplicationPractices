package com.hadoop.minweatherwithoutcombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MinWeatherWithoutCombiner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MinTemperature <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance(conf, "FindMinTemperature");
        job.setJobName("FindMinTemperature");
        job.setJarByClass(MinWeatherWithoutCombiner.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setMapperClass(MinWeatherMapper.class);
        job.setReducerClass(MinWeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startTime = new Date();
        job.waitForCompletion(true);

        Date endTime = new Date();
        double duration = (endTime.getTime() - startTime.getTime()) / 60000.0;

        System.out.println("begin：" + dateFormat.format(startTime));
        System.out.println("end：" + dateFormat.format(endTime));
        System.out.println("consume：" + String.valueOf(duration) + " min");

        System.exit(job.isSuccessful() ? 0 : -1);
    }
}
