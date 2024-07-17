package com.opstty.mapper;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeDistrictMapper extends Mapper<Object, Text, NullWritable, MapWritable> {
    private int rowCounter = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (rowCounter != 0) {
            try {
                String[] fields = value.toString().split(";");
                Integer year = Integer.parseInt(fields[5]);
                MapWritable map = new MapWritable();
                map.put(new IntWritable(Integer.parseInt(fields[1])), new IntWritable(year));
                context.write(NullWritable.get(), map);
            } catch (NumberFormatException e) {
                // Ignore parsing errors
            }
        }
        rowCounter++;
    }
}
