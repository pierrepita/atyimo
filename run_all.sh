#!/bin/bash

#Compiling comparison_lib
rm -f libcb.so
gcc --shared cb.c -o libcb.so

# Setting the spark_home
#export PATH=$PATH:/path/to/spark/bin

#Preprocessing
spark-submit 01_preprocessing.py

#Enconding
spark-submit 02_bloom_encoding.py

#Pairwise comparison
spark-submit 03_pairwise_comparison.py

#DedupByKey
spark-submit 04_dedupByKey.py

#DedupByKey
spark-submit 05_geraDataMart.py


