package com.epam.training.bigdata.hadoop.maxlengthword.phase2;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/13/2016
*/

import com.epam.training.bigdata.hadoop.maxlengthword.phase2.map.MaxLengthWordsFilterMapper;
import com.epam.training.bigdata.hadoop.maxlengthword.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class MaxLengthWordsFilterJob extends Configured implements Tool {

    private static final String INPUT_PATH_CONFIG = "mapreduce.homework1.inputpath";
    private static final String INTERMEDIATE_PATH_CONFIG = "mapreduce.homework1.intermediatepath";
    private static final String OUTPUT_PATH_CONFIG = "mapreduce.homework1.outputpath";


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        String inputPath = Utils.getPath(conf, INPUT_PATH_CONFIG);
        String intermediatePath = Utils.getPath(conf, INTERMEDIATE_PATH_CONFIG);
        String outputPath = Utils.getPath(conf, OUTPUT_PATH_CONFIG);

        Job job = Job.getInstance(conf, "Filter words with max length");
        job.setJarByClass(MaxLengthWordsFilterJob.class);

        long maxLength = getMaxWordLength(conf, intermediatePath);

        conf.setLong(Utils.MAX_WORD_LENGTH, maxLength);

        job.setMapperClass(MaxLengthWordsFilterMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private long getMaxWordLength(Configuration conf, String intermediatePath) throws IOException {
        URI defaultUri = FileSystem.getDefaultUri(conf);

        FileSystem fileSystem = FileSystem.get(defaultUri, conf);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path(intermediatePath), true);
        LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
        Path path = next.getPath();
        FSDataInputStream open = fileSystem.open(path);
        Scanner scanner = new Scanner(new BufferedInputStream(open));
        return scanner.nextLong();
    }

}
