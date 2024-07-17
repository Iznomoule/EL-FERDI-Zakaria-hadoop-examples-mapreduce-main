package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable val : values) {
            total += val.get();
        }
        result.set(total);
        context.write(key, result);
    }
}
