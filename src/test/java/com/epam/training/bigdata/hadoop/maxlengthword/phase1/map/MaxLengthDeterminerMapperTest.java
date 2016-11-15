package com.epam.training.bigdata.hadoop.maxlengthword.phase1.map;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/15/2016
*/

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class MaxLengthDeterminerMapperTest {

    private final static MaxLengthDeterminerMapper MAX_LENGTH_DETERMINER_MAPPER = new MaxLengthDeterminerMapper();

    private MapDriver<Object, Text, LongWritable, LongWritable> mapDriver;

    private static final String ASLEEP_ON_THE_FRONTLINES = "We'll keep on talking this out \n"+
            "But I've already made up your mind \n"+
            "I've been trying to gnaw through my tongue \n"+
            "To stop from confessing my crimes \n"+
            "And these conversations are wearing me down \n"+
            "What did my patience prove \n"+
            "If it's just another bed of nails \n"+
            "Always the silent treatment \n"+
            "Always the easy way out alive \n"+
            "If there's no further questions \n"+
            "I'll be on my way home.\n"+
            "And it's just another sharp pain \n"+
            "And it doesn't hurt like it used to \n"+
            "When I was a desperate man \n"+
            "When I still believed in the meaning of the word \n"+
            "You tried to warn me you and your consequences \n"+
            "If I am outnumbered if I am defenseless \n"+
            "If I come here unarmed in the middle of the night \n"+
            "On my own standing on the front lines \n"+
            "I will die if you invite me down \n"+
            "If it'll please the crowd I only go through this \n"+
            "For your amusement it doesn't hurt like should \n"+
            "When you're throwing your stones around \n"+
            "I'm alone trying to sleep it off \n"+
            "But it's hard not to shake at the sound of it breaking \n"+
            "When you're living in a house of mirrors reflecting all of my failure \n"+
            "I will concede to my replacement congratulations \n"+
            "It's over and over again I was in for the long run \n"+
            "When you cut me down another sharp pain a servant to our thrones \n"+
            "Always the one that got away always apologizing \n"+
            "Always the silent treatment always the stubborn child \n"+
            "I kept my mouth shut tight always the one \n"+
            "That got away always the bed of nails I only have myself to blame";

    @Before
    public void before() {
        mapDriver = MapDriver.newMapDriver(MAX_LENGTH_DETERMINER_MAPPER);
    }

    @Test
    public void shouldWriteTheLengthOfTheWordWithMaxLength(){
        try {
            mapDriver
                    .withInput(new LongWritable(1L), new Text(ASLEEP_ON_THE_FRONTLINES))
                    .withOutput(new LongWritable(1L), new LongWritable(15L))
                    .runTest();
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

}