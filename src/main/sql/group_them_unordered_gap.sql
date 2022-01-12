drop table if exists counts_unordered_gap;
create table counts_unordered_gap (first_term varchar(100), second_term varchar(100), index int);
copy counts_unordered_gap from '/data/collectione-tokenized.tsv.counts_unordered_gap.counts' with (format csv);
drop table if exists counts_unordered_gap_grouped;
create table counts_unordered_gap_grouped (first_term varchar(100), second_term varchar(100), index int, count int);
insert into counts_unordered_gap_grouped select first_term, second_term, index, count(*) as count from counts_unordered_gap group by first_term, second_term, index;
create index counts_unordered_gap_grouped_x1 on counts_unordered_gap_grouped(first_term);
copy counts_unordered_gap_grouped to '/data/collectione-tokenized.tsv.counts_unordered_gap.grouped.csv' with (format csv);
