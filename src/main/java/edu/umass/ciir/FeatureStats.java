package edu.umass.ciir;

public class FeatureStats {
    String first;
    String second;
    double[] counts_ordered_gap;
    double[] counts_unordered_gap;
    double[] counts_unordered_inwindow;
    double count_indoc;

    public FeatureStats(String first, String second, double[] counts_ordered_gap, double[] counts_unordered_gap,
                        double[] counts_unordered_inwindow, double count_indoc) {
        this.first = first;
        this.second = second;
        this.counts_ordered_gap = counts_ordered_gap;
        this.counts_unordered_gap = counts_unordered_gap;
        this.counts_unordered_inwindow = counts_unordered_inwindow;
        this.count_indoc = count_indoc;
    }
}
