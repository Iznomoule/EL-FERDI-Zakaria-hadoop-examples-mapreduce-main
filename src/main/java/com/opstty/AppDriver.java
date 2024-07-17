package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("district", District.class,
                    "A map/reduce program that lists the distinct districts containing trees.");
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that lists the different species of trees.");
            programDriver.addClass("treeskind", Treeskind.class,
                    "A map/reduce program that counts the number of trees of each kind.");
            programDriver.addClass("maxheightkind", Maxheightkind.class,
                    "A map/reduce program that calculates the maximum height of each kind of tree.");
            programDriver.addClass("sorttrees", SortTrees.class,
                    "A map/reduce program that sorts trees by height from smallest to largest.");
            programDriver.addClass("oldesttreedistrict", OldestTreeDistrict.class,
                    "A map/reduce program that finds the district with the oldest tree.");
            programDriver.addClass("districtmaxtrees", DistrictMaxTrees.class,
                    "A map/reduce program that finds the district with the most trees.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
