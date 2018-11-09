package com.hadoop.minweatherwithoutcombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinWeatherMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private static final int MISSING = -9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String month = line.substring(5, 7);
        String strAirTemperature = line.substring(13, 19);
        int airTemperature = Integer.parseInt(strAirTemperature.trim());
        if (airTemperature != MISSING) {
            context.write(new Text(month), new IntWritable(airTemperature));
        }
    }
}
