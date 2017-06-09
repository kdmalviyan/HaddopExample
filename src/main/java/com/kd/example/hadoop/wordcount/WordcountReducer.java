package com.kd.example.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        System.out.println("Counting words inside reducer.");
        int counter = 0;
        for (IntWritable i : values) {
            counter = counter + i.get();
        }
        System.out.println(key + " ======== " + counter);
        context.write(new Text(key + " "), new IntWritable(counter));
    }
}
