package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMaxTreesMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private int lineNumber = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (lineNumber != 0) {
            String[] parts = value.toString().split(";");
            context.write(new IntWritable(Integer.parseInt(parts[1])), new IntWritable(1));
        }
        lineNumber++;
    }
}