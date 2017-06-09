package com.kd.example.hadoop.anagrams;

import java.io.IOException;
import java.text.Collator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnargamMapper2 extends Mapper<Object, Text, Text, IntWritable> {
    Map<String, Integer> map = new HashMap<>();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) {
        String words[] = value.toString().split(",");
        System.out.println("Reading file inside mapper: " + value);
        for (String word : words) {
            String sortedWord = sortAlphabatically(word);
            if (map.containsKey(sortedWord)) {
                map.put(sortedWord, map.get(sortedWord) + 1);
            } else {
                map.put(sortedWord, 1);
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

    @Override
    protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        for (String key : map.keySet()) {
            context.write(new Text(key), new IntWritable(map.get(key)));
        }
    }
}
