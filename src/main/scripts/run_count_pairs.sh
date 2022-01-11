#!/bin/bash
set -v
JAVA_HOME=/etc/alternatives/java_sdk_11_openjdk /etc/alternatives/java_sdk_11_openjdk/bin/java -jar count-pairs-1.0.0.jar data/collectione-tokenized.tsv makeCountFiles
