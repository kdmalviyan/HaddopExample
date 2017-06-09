package com.kd.example.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordcountMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) {
        String words[] = value.toString().split(",");
        System.out.println("Reading file inside mapper: " + value);
        Text t = new Text();
        IntWritable i = new IntWritable();
        for (String word : words) {
            t.set(word);
            i.set(1);
            try {
                context.write(t, i);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
