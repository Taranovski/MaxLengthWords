package com.epam.training.bigdata.hadoop.maxlengthword.phase2.map;
/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/14/2016
*/

import com.epam.training.bigdata.hadoop.maxlengthword.util.Utils;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.Scanner;

public class MaxLengthWordsFilterMapper extends Mapper<Object, Text, NullWritable, Text> {

    private static final NullWritable KEY = NullWritable.get();
    private static final LongWritable MAX_LENGTH = new LongWritable(Long.MIN_VALUE);
    private static final Text VALUE = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        for (URI path : cacheFiles) {
            if (path.toString().contains("part-r-00000")) {
                FileSystem fileSystem = FileSystem.get(context.getConfiguration());
                Scanner scanner = new Scanner(new BufferedInputStream(fileSystem.open(new Path(path))));
                long maxLength = scanner.nextLong();
                MAX_LENGTH.set(maxLength);

                System.err.println(MAX_LENGTH);
                break;
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = Utils.getWords(value);

        for (String word : words) {
            if (word.length() == MAX_LENGTH.get()) {
                VALUE.set(word);
                context.write(KEY, VALUE);
            }
        }
    }
}
