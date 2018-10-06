#!/bin/bash

#Preprocessing
#/usr/local/spark-2.0.0/bin/spark-submit 01_preprocessing.py

#CreateBlockKey
#/usr/local/spark-2.0.0/bin/spark-submit 02_createBlockKey.py 

#WriteBlocks
#/usr/local/spark-2.0.0/bin/spark-submit 03_writeBlocks.py 

#Enconding
/usr/local/spark-2.0.0/bin/spark-submit 04_encoding_blocking.py

#Correlation
/usr/local/spark-2.0.0/bin/spark-submit 05_correlation_new_otimizado2.py

#DedupByKey
/usr/local/spark-2.0.0/bin/spark-submit 06_dedupByKey.py

#GeraDataMart
#/usr/local/spark-2.0.0/bin/spark-submit 

## Segundo Round

# Extract
#/usr/local/spark-2.0.0/bin/spark-submit second_round_extract.py

# Encoding
#/usr/local/spark-2.0.0/bin/spark-submit second_round_encoding_blocking.py

# Correlation
#/usr/local/spark-2.0.0/bin/spark-submit second_round_correlation_weighted.py

# Gera DataMart
#/usr/local/spark-2.0.0/bin/spark-submit second_round_merge_geraDataMart.py


