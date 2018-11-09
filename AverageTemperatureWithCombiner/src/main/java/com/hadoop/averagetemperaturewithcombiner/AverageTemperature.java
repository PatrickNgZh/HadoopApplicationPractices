package com.hadoop.averagetemperaturewithcombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author peicong
 * @date 2018/11/9 0009
 */
public class AverageTemperature extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, strings).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("");
            System.err.printf("Usage: %s < input path > < output path > \n", this.getClass().getSimpleName());
            System.err.printf("Example: hadoop jar %s hdfs://localhost:9000/user/grid/in hdfs://localhost:9000/user/grid/output\n", this.getClass().getSimpleName());
            System.exit(-1);
        }

        Job job = Job.getInstance(configuration, "AvgTemperature");
        job.setJarByClass(AverageTemperature.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.setMapperClass(AverageTemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TemperatureAggregator.class);

        job.setCombinerClass(AverageTemperatureCombiner.class);

        job.setReducerClass(AverageTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);

        System.out.println("JobName is：" + job.getJobName());
        System.out.println("JobStatus：" + (job.isSuccessful() ? "successful" : "fail"));
        System.out.println("input number of line："
                + job.getCounters()
                .findCounter("org.apache.hadoop.mapred.Task$Counter",
                        "MAP_INPUT_RECORDS").getValue());
        System.out.println("output number of line："
                + job.getCounters()
                .findCounter("org.apache.hadoop.mapred.Task$Counter",
                        "MAP_OUTPUT_RECORDS").getValue());
        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startTime = new Date();

        int res = ToolRunner.run(new AverageTemperature(), args);
        if (res == -1) {
            System.exit(-1);
        }

        Date endTime = new Date();
        double duration  = (endTime.getTime() - startTime.getTime())/60000.0;

        System.out.println( "begin: " + dateFormat.format(startTime) );
        System.out.println( "end: " + dateFormat.format(endTime) );
        System.out.println( "consume: " + String.valueOf(duration) + " min" );
        System.exit(res);
    }
}
