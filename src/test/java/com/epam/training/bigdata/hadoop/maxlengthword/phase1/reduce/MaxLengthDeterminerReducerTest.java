package com.epam.training.bigdata.hadoop.maxlengthword.phase1.reduce;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/15/2016
*/

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.fail;


public class MaxLengthDeterminerReducerTest {

    private final static MaxLengthDeterminerReducer MAX_LENGTH_DETERMINER_REDUCER = new MaxLengthDeterminerReducer();

    private ReduceDriver<LongWritable, LongWritable, NullWritable, LongWritable> reduceDriver;

    @Before
    public void before() {
        reduceDriver = ReduceDriver.newReduceDriver(MAX_LENGTH_DETERMINER_REDUCER);
        reduceDriver.clearInput();
    }

    @Test
    public void shouldWriteMaxWordLengthFromAllMapOutputs() {
        try {
            reduceDriver
                    .withInput(new LongWritable(1L),
                            Arrays.asList(new LongWritable(1L), new LongWritable(2L), new LongWritable(3L)))
                    .withOutput(NullWritable.get(), new LongWritable(3L))
                    .runTest();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

}