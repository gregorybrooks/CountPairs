package edu.umass.ciir;

public class Stats {
    String first;
    String second;
    Integer[] counts_ordered_gap;
    Integer[] counts_unordered_gap;
    Integer[] counts_unordered_inwindow;
    Integer count_indoc;

    public Stats(String first, String second, Integer[] counts_ordered_gap, Integer[] counts_unordered_gap,
                 Integer[] counts_unordered_inwindow, Integer count_indoc) {
        this.first = first;
        this.second = second;
        this.counts_ordered_gap = counts_ordered_gap;
        this.counts_unordered_gap = counts_unordered_gap;
        this.counts_unordered_inwindow = counts_unordered_inwindow;
        this.count_indoc = count_indoc;
    }
}
