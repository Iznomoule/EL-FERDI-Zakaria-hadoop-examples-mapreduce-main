package com.opstty.job;

import com.opstty.mapper.TreeskindMapper;
import com.opstty.reducer.TreeskindReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Treeskind {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: Number trees by kind <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "treeskind");
        job.setJarByClass(Treeskind.class);
        job.setMapperClass(TreeskindMapper.class);
        job.setCombinerClass(TreeskindReducer.class);
        job.setReducerClass(TreeskindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < remainingArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(remainingArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[remainingArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
