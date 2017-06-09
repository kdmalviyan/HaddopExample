package com.kd.example.hadoop.anagrams;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnargamDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(AnargamDriver.class);
        job.setMapperClass(AnargamMapper2.class);
        job.setCombinerClass(AnargamReducer.class);
        job.setReducerClass(AnargamReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:8020/usr/anagrams.txt"));
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://localhost:8020/usr/anargam/output_" + Calendar.getInstance().getTime().getTime()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
