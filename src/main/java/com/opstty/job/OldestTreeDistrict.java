package com.opstty.job;

import com.opstty.mapper.OldestTreeDistrictMapper;
import com.opstty.reducer.OldestTreeDistrictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OldestTreeDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: OldestTreeDistrict <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "oldestTreeDistrict");
        job.setJarByClass(OldestTreeDistrict.class);
        job.setMapperClass(OldestTreeDistrictMapper.class);
        job.setReducerClass(OldestTreeDistrictReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
