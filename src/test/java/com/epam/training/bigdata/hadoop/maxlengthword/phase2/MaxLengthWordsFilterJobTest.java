package com.epam.training.bigdata.hadoop.maxlengthword.phase2;
/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/15/2016
*/

import com.epam.training.bigdata.hadoop.maxlengthword.phase2.map.MaxLengthWordsFilterMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.*;

public class MaxLengthWordsFilterJobTest {

    private final MaxLengthWordsFilterMapper maxLengthWordsFilterMapper = new MaxLengthWordsFilterMapper();
    private MapReduceDriver<Object, Text, NullWritable, Text, NullWritable, Text> mapReduceDriver;


    private static final String ASLEEP_ON_THE_FRONTLINES_1 =
            "We'll keep on talking this out \n"+
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
                    "When I still believed in the meaning of the word \n";

    private static final String ASLEEP_ON_THE_FRONTLINES_2 =
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
    public void before(){
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(maxLengthWordsFilterMapper, new Reducer<NullWritable, Text, NullWritable, Text>());
    }

    @Test
    public void shouldWriteTheWordsWithTheLongestLength() throws URISyntaxException {
        URL resource = MaxLengthWordsFilterJobTest.class.getResource("/part-r-00000");
        mapReduceDriver.addCacheFile(resource.toURI());

        try {
            mapReduceDriver
                    .withInput(new LongWritable(1L), new Text(ASLEEP_ON_THE_FRONTLINES_1))
                    .withInput(new LongWritable(2L), new Text(ASLEEP_ON_THE_FRONTLINES_2))
                    .withOutput(NullWritable.get(), new Text("congratulations"))
                    .runTest();
        } catch (IOException e) {
            fail(e.getMessage());
        }


    }
}