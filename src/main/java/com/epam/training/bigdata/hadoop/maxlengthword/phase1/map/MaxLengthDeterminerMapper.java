package com.epam.training.bigdata.hadoop.maxlengthword.phase1.map;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/14/2016
*/

import com.epam.training.bigdata.hadoop.maxlengthword.util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxLengthDeterminerMapper extends Mapper<Object, Text, LongWritable, LongWritable> {

    private static final LongWritable KEY = new LongWritable(1L);
    private static final LongWritable MAX_VALUE = new LongWritable(Long.MIN_VALUE);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = Utils.getWords(value);

        for (String word : words) {
            int length = word.length();
            if (length > MAX_VALUE.get()) {
                MAX_VALUE.set(length);
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(KEY, MAX_VALUE);
    }

}
