package com.epam.training.bigdata.hadoop.maxlengthword.phase1;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/13/2016
*/

import com.epam.training.bigdata.hadoop.maxlengthword.phase1.map.MaxLengthDeterminerMapper;
import com.epam.training.bigdata.hadoop.maxlengthword.phase1.reduce.MaxLengthDeterminerReducer;
import com.epam.training.bigdata.hadoop.maxlengthword.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class MaxLengthDeterminerJob extends Configured implements Tool {

    private static final String INPUT_PATH_CONFIG = "mapreduce.homework1.inputpath";
    private static final String INTERMEDIATE_PATH_CONFIG = "mapreduce.homework1.intermediatepath";

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        String inputPath = Utils.getPath(conf, INPUT_PATH_CONFIG);
        String intermediatePath = Utils.getPath(conf, INTERMEDIATE_PATH_CONFIG);

        Job job = Job.getInstance(conf, "Determine word's max length");
        job.setJarByClass(MaxLengthDeterminerJob.class);

        job.setMapperClass(MaxLengthDeterminerMapper.class);
        job.setReducerClass(MaxLengthDeterminerReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(intermediatePath));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
