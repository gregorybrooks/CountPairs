package edu.umass.ciir;

public class Negin {

    public static void main (String[] args) {

        if (args.length != 3) {
            throw new IllegalArgumentException("Three args are required. Arg 1 should be the pathname of the collection, arg 2 should be the operation--makeFeatureFiles, makeCountFiles or makeFrequencyStatFiles, arg 3 should be counts_ordered_gap, counts_unordered_gap, counts_unordered_inwindow or count_indoc");
        }

        String collection = args[0];
        String operation = args[1];
        String featureType = args[2];
        System.out.println("Processing collection " + collection + ", doing " + operation + ", feature " + featureType);

        if (!(operation.equals("makeFrequencyStatFiles") || operation.equals("makeFeatureFile"))) {
            throw new IllegalArgumentException("Invalid operation: " + operation + ". Should be makeFrequencyStatFiles or makeFeatureFile.");
        }
        if (!(featureType.equals("counts_unordered_gap") || featureType.equals("counts_ordered_gap") ||
                featureType.equals("counts_unordered_inwindow") || featureType.equals("count_indoc"))) {
            throw new IllegalArgumentException("Invalid feature type: " + featureType + ". Should be counts_unordered_gap, counts_ordered_gap, counts_unordered_inwindow or count_indoc.");
        }
        /* The order of operations is:
           run this jar with operation=makeFrequencyStatFiles
           then run this jar file with operation=makeFeatureFile, which calculates the "feature" numbers and outputs a CSV file
         */
        try {
            if (operation.equals("makeFrequencyStatFiles")) {   // collection, document and total frequency stats
                MakeFrequencyFiles run = new MakeFrequencyFiles();
                run.gatherFrequencyStats(collection);
            } else if (operation.equals("makeFeatureFile")) {  // make the file containing the feature calculations
                MakeFeatureFile mp = new MakeFeatureFile(featureType, collection, collection + "." + featureType + ".features.csv");
                if (mp.runJob1()) {
                    mp.runJob2();
                }
            }
        } catch (Exception e) {
            throw new AppException(e);
        }
    }
}
