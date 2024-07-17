package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            String[] fields = value.toString().split(";");
            context.write(new IntWritable(Integer.parseInt(fields[1])), new IntWritable(1));
        }
        rowCounter++;
    }
}

