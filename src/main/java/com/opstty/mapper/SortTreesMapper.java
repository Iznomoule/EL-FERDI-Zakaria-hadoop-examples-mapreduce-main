package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortTreesMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            try {
                String[] parts = value.toString().split(";");
                Float height = Float.parseFloat(parts[6]);
                String outputValue = parts[11] + " - " + parts[2] + " " + parts[3] + " (" + parts[4] + ")";
                context.write(new FloatWritable(height), new Text(outputValue));
            } catch (NumberFormatException e) {
                // Ignore parsing errors
            }
        }
        rowCounter++;
    }
}
