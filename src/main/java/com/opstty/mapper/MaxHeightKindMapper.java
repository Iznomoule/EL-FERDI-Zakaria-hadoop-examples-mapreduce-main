package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightKindMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            try {
                String[] fields = value.toString().split(";");
                Float height = Float.parseFloat(fields[6]);
                context.write(new Text(fields[3]), new FloatWritable(height));
            } catch (NumberFormatException e) {
                // Ignore parsing errors
            }
        }
        rowCounter++;
    }
}