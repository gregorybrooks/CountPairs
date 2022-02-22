package edu.umass.ciir;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.abs;

public class Negin {

    public static void main (String[] args) {

        //String collection = "/u02/negin/shared/data/collectione-tokenized.tsv";
        if (args.length != 3) {
            throw new IllegalArgumentException("Arg 1 should be the pathname of the collection, arg 2 should be the operation--makeFeatureFiles, makeCountFiles or makeFrequencyStatFiles, arg 3 should be counts_ordered_gap, counts_unordered_gap, counts_unordered_inwindow or count_indoc");
        }

        String collection = args[0];
        String operation = args[1];
        String featureType = args[2];
        System.out.println("Processing collection " + collection + ", doing " + operation + ", feature " + featureType);

        /* The order of operations is:
           run this jar with operation=makeCountFiles (map step),
           then import the CSV file created by makeCountFiles into Postgres and create the grouped table
           and export the grouped table as a CSV files (reduce step),
           then run this jar file with operation=makeFeatureFiles, which calculates the "feature" numbers and outputs a CSV file
         */
        try {
            if (operation.equals("makeFrequencyStatFiles")) {   // collection, document and total frequency stats
                MakeCountFiles run = new MakeCountFiles();
                run.gatherFrequencyStats(collection);
            } else if (operation.equals("makeCountFiles")) {  // make the un-grouped CSV files and the freq stats CSV files
//                MakeCountFiles run = new MakeCountFiles();
//                run.featureizeWithStreams(collection, featureType);
                MapReduce mp = new MapReduce(featureType, collection, collection + "." + featureType + ".features.csv");
                if (mp.runJob1()) {
                    mp.runJob2();
                }
            } else if (operation.equals("makeFeatureFiles")) {   // The grouped CSV files, created outside this program, must exist
                MakeFeatureFiles run = new MakeFeatureFiles();
                run.featureizeWithStreams(collection, featureType);
            } else {
                throw new IllegalArgumentException("Invalid operation: " + operation + ". Should be makeFeatureFiles, makeCountFiles or makeFrequencyStatFiles");
            }
        } catch (Exception e) {
            throw new AppException(e);
        }
    }
}
