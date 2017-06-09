package com.kd.example.hadoop.wordcount;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WorldCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WorldCountDriver.class);
        job.setMapperClass(WordcountMapper.class);
        job.setCombinerClass(WordcountReducer.class);
        job.setReducerClass(WordcountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:8020/usr/anagrams.txt"));
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://localhost:8020/usr/streamanalytix/output_" + Calendar.getInstance().getTime().getTime()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
