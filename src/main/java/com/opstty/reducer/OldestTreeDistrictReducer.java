package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OldestTreeDistrictReducer extends Reducer<NullWritable, MapWritable, IntWritable, IntWritable> {
    public void reduce(NullWritable key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        List<int[]> districtYears = StreamSupport.stream(values.spliterator(), false)
                .map(mw -> new int[]{
                        ((IntWritable) mw.keySet().iterator().next()).get(),
                        ((IntWritable) mw.get(mw.keySet().iterator().next())).get()
                })
                .collect(Collectors.toList());

        int oldestYear = districtYears.stream().mapToInt(arr -> arr[1]).min().orElse(Integer.MAX_VALUE);

        districtYears.stream()
                .filter(arr -> arr[1] == oldestYear)
                .forEach(arr -> {
                    try {
                        context.write(new IntWritable(arr[0]), new IntWritable(oldestYear));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }
}
