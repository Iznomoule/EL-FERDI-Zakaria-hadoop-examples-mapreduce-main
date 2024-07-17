package com.opstty.job;

import com.opstty.mapper.MaxHeightKindMapper;
import com.opstty.reducer.MaxHeightKindReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Maxheightkind {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: Max Height per kind <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "maxheightkind");
        job.setJarByClass(Maxheightkind.class);
        job.setMapperClass(MaxHeightKindMapper.class);
        job.setCombinerClass(MaxHeightKindReducer.class);
        job.setReducerClass(MaxHeightKindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
