package com.opstty.job;

import com.opstty.mapper.DistrictMaxTreesMapper;
import com.opstty.reducer.DistrictMaxTreesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictMaxTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: Districtmaxtrees <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "districtmaxtrees");
        job.setJarByClass(DistrictMaxTrees.class);
        job.setMapperClass(DistrictMaxTreesMapper.class);
        job.setReducerClass(DistrictMaxTreesReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
