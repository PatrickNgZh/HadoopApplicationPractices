package com.hadoop.averagetemperaturewithcombiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;

/**
 * @author peicong
 * @date 2018/11/9 0009
 */
public class AverageTemperatureReducer extends Reducer<Text, TemperatureAggregator, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<TemperatureAggregator> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        BigInteger sum = new BigInteger("0");
        for (TemperatureAggregator temperatureAggregator : values) {
            num += temperatureAggregator.getNum().get();
            sum=sum.add(new BigInteger(temperatureAggregator.getSum().toString()));
        }
        sum=sum.divide(new BigInteger(String.valueOf(num)));
        context.write(key, new Text(sum.toString()));
    }
}
