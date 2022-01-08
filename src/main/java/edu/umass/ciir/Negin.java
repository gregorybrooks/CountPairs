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
        if (args.length != 2) {
            throw new IllegalArgumentException("Arg 1 should be the pathname of the collection, arg 2 should be the operation--makeFeatureFiles or makeCountFiles");
        }

        String collection = args[0];
        String operation = args[1];
        System.out.println("Processing collection " + collection + ", doing " + operation);

        /* The order of operations is: makeCountFiles, then import the CSV files into Postgres and create the grouped
           tables and export the grouped tables as CSV files, then do makeFeatureFiles, which outputs CSV files with
           the "featureized" numbers
         */
        if (operation.equals("makeCountFiles")) {  // make the pre-grouped CSV files and the freq stats CSV files
            MakeCountFiles n = new MakeCountFiles();
            try {
                n.featureizeWithStreams(collection);
                n.gatherFrequencyStats(collection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (operation.equals("makeFeatureFiles")) {   // The grouped CSV files should be there
            MakeFeatureFiles f = new MakeFeatureFiles();
            try {
                f.featureizeWithStreams(collection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new IllegalArgumentException("Invalid operation: " + operation + ". Should be makeFeatureFiles or makeCountFiles");
        }
    }
}
