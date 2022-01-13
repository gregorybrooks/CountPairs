#!/bin/bash
set -v

JAVA_HOME=/etc/alternatives/java_sdk_11_openjdk /etc/alternatives/java_sdk_11_openjdk/bin/java -jar count-pairs-1.0.0.jar data/collectione-tokenized.tsv makeCountFiles counts_unordered_inwindow

docker run --rm --network postgres-network -v /mnt/scratch/glbrooks/negin:/scripts -v /mnt/scratch/glbrooks/negin/data:/data -e PGPASSWORD=postgres ubuntu/postgres psql -h postgres-container -U postgres -f /scripts/group_them_unordered_inwindow.sql

JAVA_HOME=/etc/alternatives/java_sdk_11_openjdk /etc/alternatives/java_sdk_11_openjdk/bin/java -jar count-pairs-1.0.0.jar data/collectione-tokenized.tsv makeFeatureFiles counts_unordered_inwindow
