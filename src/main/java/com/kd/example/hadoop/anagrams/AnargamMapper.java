package com.kd.example.hadoop.anagrams;

import java.io.IOException;
import java.text.Collator;
import java.util.Arrays;
import java.util.Locale;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnargamMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) {
        String words[] = value.toString().split(",");
        System.out.println("Reading file inside mapper: " + value);
        Text t = new Text();
        IntWritable i = new IntWritable();
        for (String word : words) {
            String sortedWord = sortAlphabatically(word);
            t.set(sortedWord);
            i.set(1);
            try {
                context.write(t, i);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private String sortAlphabatically(String word) {
        Collator col = Collator.getInstance(new Locale("en", "EN"));
        String[] s1 = word.split("");
        Arrays.sort(s1, col);
        String sorted = "";
        for (int i = 0; i < s1.length; i++) {
            sorted += s1[i];
        }
        return sorted;
    }
}
