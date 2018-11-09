package com.hadoop.averagetemperaturewithcombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author peicong
 * @date 2018/11/9 0009
 */
public class TemperatureAggregator implements Writable {

    private IntWritable num;
    private Text sum;

    public TemperatureAggregator() {
        this.num = new IntWritable();
        this.sum = new Text();
    }

    public TemperatureAggregator(IntWritable num, Text sum) {
        this.num = num;
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "TemperatureAggregator{" +
                "num=" + num +
                ", sum=" + sum +
                '}';
    }

    public IntWritable getNum() {
        return num;
    }

    public void setNum(IntWritable num) {
        this.num = num;
    }

    public Text getSum() {
        return sum;
    }

    public void setSum(Text sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        num.write(dataOutput);
        sum.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        num.readFields(dataInput);
        sum.readFields(dataInput);
    }
}
