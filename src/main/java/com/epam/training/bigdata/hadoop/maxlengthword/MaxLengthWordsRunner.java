package com.epam.training.bigdata.hadoop.maxlengthword;

/*
* Created by : Oleksandr_Taranovskyi@epam.com
* Created at : 11/13/2016
*/

import com.epam.training.bigdata.hadoop.maxlengthword.phase1.MaxLengthDeterminerJob;
import com.epam.training.bigdata.hadoop.maxlengthword.phase2.MaxLengthWordsFilterJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MaxLengthWordsRunner {

    private static final int SUCCESS_JOB_EXECUTION_STATUS = 0;




    public static void main(String[] args) throws Exception {

        validateInputParameters(args);

        int firstPhaseResult = ToolRunner.run(new Configuration(), new MaxLengthDeterminerJob(), args);
        if (firstPhaseResult != SUCCESS_JOB_EXECUTION_STATUS) {
            System.exit(firstPhaseResult);
        }
        int secondPhaseResult = ToolRunner.run(new Configuration(), new MaxLengthWordsFilterJob(), args);
        System.exit(secondPhaseResult);
    }

    private static void validateInputParameters(String[] args) {
        //validate input
    }
}
