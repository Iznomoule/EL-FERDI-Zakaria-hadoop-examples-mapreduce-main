package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MaxHeightKindReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        List<Float> heights = StreamSupport.stream(values.spliterator(), false)
                .map(FloatWritable::get)
                .collect(Collectors.toList());
        Float maxHeight = Collections.max(heights);
        context.write(key, new FloatWritable(maxHeight));
    }
}
