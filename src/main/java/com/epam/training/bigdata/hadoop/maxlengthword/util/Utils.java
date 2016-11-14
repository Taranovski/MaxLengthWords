package com.epam.training.bigdata.hadoop.maxlengthword.util;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/13/2016
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class Utils {

    public static final String MAX_WORD_LENGTH = "max word length";

    public static String getPath(Configuration conf, String inputPathConfig) {
        String inputPath = conf.get(inputPathConfig);
        if (inputPath == null) {
            throw new RuntimeException("no " + inputPathConfig +
                    " path set, try to set it by adding -D" + inputPathConfig + "=<path>");
        }
        return inputPath;
    }

    public static String[] getWords(Text value) {
        return value.toString().split("\\s");
    }
}
