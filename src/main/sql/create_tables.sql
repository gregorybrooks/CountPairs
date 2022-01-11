create table counts_ordered_gap (first_term varchar(100), second_term varchar(100), index int);
create table counts_unordered_gap (first_term varchar(100), second_term varchar(100), index int);
create table counts_unordered_inwindow (first_term varchar(100), second_term varchar(100), index int);
create table count_indoc (first_term varchar(100), second_term varchar(100));
create table counts_ordered_gap_grouped (first_term varchar(100), second_term varchar(100), index int, int count);
create table counts_unordered_gap_grouped (first_term varchar(100), second_term varchar(100), index int, int count);
create table counts_unordered_inwindow_grouped (first_term varchar(100), second_term varchar(100), index int, int count);
create table count_indoc_grouped (first_term varchar(100), second_term varchar(100), int count);
