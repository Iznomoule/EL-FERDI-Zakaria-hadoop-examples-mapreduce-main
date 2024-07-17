package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistrictMaxTreesReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private List<int[]> districtCounts = new ArrayList<>();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }
        districtCounts.add(new int[]{key.get(), count});
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        int maxTrees = districtCounts.stream().mapToInt(arr -> arr[1]).max().orElse(0);
        districtCounts.stream()
                .filter(arr -> arr[1] == maxTrees)
                .forEach(arr -> {
                    try {
                        context.write(new IntWritable(arr[0]), new IntWritable(maxTrees));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }
}
