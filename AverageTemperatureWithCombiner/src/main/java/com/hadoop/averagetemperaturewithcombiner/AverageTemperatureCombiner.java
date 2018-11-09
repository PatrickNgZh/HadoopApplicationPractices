package com.hadoop.averagetemperaturewithcombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;

/**
 * @author peicong
 * @date 2018/11/9 0009
 */
public class AverageTemperatureCombiner extends Reducer<Text, TemperatureAggregator,Text,TemperatureAggregator> {
    @Override
    protected void reduce(Text key, Iterable<TemperatureAggregator> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        BigInteger sum = new BigInteger("0");
        for (TemperatureAggregator temperatureAggregator : values) {
            ++num;
            //attention
            sum=sum.add(new BigInteger(String.valueOf(temperatureAggregator.getSum())));
        }
        context.write(key, new TemperatureAggregator(new IntWritable(num), new Text(sum.toString())));
    }
}
