package com.epam.training.bigdata.hadoop.maxlengthword.phase1.reduce;
/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/14/2016
*/

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxLengthDeterminerReducer extends Reducer<LongWritable, LongWritable, NullWritable, LongWritable> {

    private final NullWritable KEY = NullWritable.get();
    private final LongWritable MAX_LENGTH_VALUE = new LongWritable(Long.MIN_VALUE);

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable longWritable : values) {
            long length = longWritable.get();
            if (length > MAX_LENGTH_VALUE.get()) {
                MAX_LENGTH_VALUE.set(length);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(KEY, MAX_LENGTH_VALUE);
    }
}
