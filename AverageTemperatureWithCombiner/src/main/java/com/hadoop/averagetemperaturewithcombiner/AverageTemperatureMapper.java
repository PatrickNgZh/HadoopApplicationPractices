package com.hadoop.averagetemperaturewithcombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author peicong
 * @date 2018/11/8 0008
 */
class AverageTemperatureMapper extends Mapper<LongWritable, Text, Text, TemperatureAggregator> {
    public static final String MISSING = "-9999";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String month = line.substring(5, 7);
        String strTemperature = line.substring(13, 19).trim();

        if (!strTemperature.equals(MISSING)) {
            context.write(new Text(month), new TemperatureAggregator(new IntWritable(1),new Text(strTemperature)));
        }
    }
}
