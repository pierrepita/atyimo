#!/bin/env python
# coding: utf-8

import config
from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
# Set everything to be used in preprocessing module
# Destination folder name for standardized files
pp_directory_padronized_files =	str(config.default_folder)+"standardized/"
#Larger file
#Larger file name after standardization
pp_larger_output_file = str(pp_directory_padronized_files)+"larger_file_standardized.csv"
#Smaller file
#Smaller file name after standardization
pp_smaller_output_file = str(pp_directory_padronized_files)+"smaller_file_standardized.csv"
#Boolean indicating the existence of these variables in the larger base
pp_larger_status_index = 1					#Index
#Boolean indicating the existence of these variables in the smaller base
pp_smaller_status_index = 1						#Index

#Item position in the larger input file
pp_larger_col_i = 0		#Index
#Item position in the smaller input file
pp_smaller_col_i = 0		#Index


##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
#set info to be used in encoding module
#Larger input file to the encoding module
e_largest_input_file = str(pp_larger_output_file)
#Smaller input file to the encoding module
e_smaller_input_file = str(pp_smaller_output_file)

#Folder name to bloom files
e_directory_blocks = config.default_folder + "blocks/"
#Folder location for keys used in blocking
e_directory_key_folder = config.default_folder + "blocks/keys/"
#Folder name to bloom files of larger base
e_directory_block_larger =  e_directory_blocks + "block_larger/"
#Folder name to bloom files of smaller base
e_directory_block_smaller =  e_directory_blocks + "block_smaller/"
#File name of blocking keys
e_file_key_blocking = "keyBloking.txt"

#Item position in the standardized files
e_col_i = 0     #Index
e_col_n = 2     #Name
e_col_mn = 3    #Mother name
e_col_bd =4             #Birth date
e_col_g = 7             #Gender
e_col_mr = 6    #Municipality residence
e_col_st = -1   #State


##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
#Set info to be used in correlation module
#Size of bloom filter
c_size_bloom_vector = int(config.e_size_bloom_col_n)+ int(config.e_size_bloom_col_mn) + int(config.e_size_bloom_col_bd) + int(config.e_size_bloom_col_mr)
#Folder name to linkage results
c_directory_results = config.default_folder + "linkage_results/"
#Name of linkage file with dice higher main cutoff
c_file_result = c_directory_results + "result.linkage"
#Name of linkage file with dice higher secondary cutoff and below the main cutoff
c_file_result_rescue = c_directory_results + "result-Rescue.linkage"
#Secondery cutoff (Dice)
c_cutoff_result_rescue = 7000
#Boolean indicating the existence of a rescue linkage file below the c_cutoff_result
c_status_rescue = 0

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
#Set info to be used in deduplication module
#Column used in the deduplication module step 1

d_key_col_step1 = 1
#Column used in the deduplication module step 1
d_key_col_step2 = 2
#Input file to the deduplication module
d_input_file = c_file_result
#Tmp output file to the deduplication module - step1
d_tmp_step1_output_file = c_directory_results +"result_and_rescue_sort_tmp_step1.linkage"
#Tmp output file to the deduplication module - step2
d_tmp_step2_output_file = c_directory_results +"result_and_rescue_sort_tmp_step2.linkage"
#Final output file to the deduplication module - step1
d_output_file_step1 = c_directory_results +"result_and_rescue_dedup_tmp_step1.linkage"
#Final output file for result oficial
d_output_file_step2 = c_directory_results +"result_dedup.linkage"
#Final output file for rescue
d_output_file_step2_rescue = c_directory_results +"rescue_dedup.linkage"

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
#Set info to be used in dataMart module
#Folder name to datamart files
dm_directory_datamarts = config.default_folder + "dataMarts/"
#Input file to the dataMart module
dm_input_file = c_directory_results +"result_dedup.linkage"
#Output file to the dataMart module
dm_output_file1 = dm_directory_datamarts +"dataMart_type1.csv"
dm_output_file2 = dm_directory_datamarts +"dataMart_type2.csv"
dm_output_file3 = dm_directory_datamarts +"dataMart_type3.csv"
dm_output_file4 = dm_directory_datamarts +"dataMart_type4.csv"
#Larger base file used to retrieve the tuples to the datamart
dm_larger_base_for_merge = pp_directory_padronized_files +"larger_file_with_index.csv"
#Smaller base file used to retrieve the tuples to the datamart
dm_smaller_base_for_merge = pp_directory_padronized_files +"smaller_file_with_index.csv"

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
# Set configuration to drive executions over Spark Framework
# To see more about configuration alternatives: http://spark.apache.org/docs/latest/configuration.html
pp_conf = SparkConf().setMaster('local[4]').setAppName('preprocessing').set('spark.executor.cores','4').set('spark.executor.memory','512M').set('spark.driver.memory','512M').set('spark.driver.maxResultSize', '512M').set('spark.eventLog.enabled','false')
en_conf = SparkConf().setMaster('local[4]').setAppName('enconding').set('spark.executor.cores','4').set('spark.executor.memory','512M').set('spark.driver.memory','512M').set('spark.driver.maxResultSize', '512M').set('spark.eventLog.enabled','false')
co_conf = SparkConf().setMaster('local[4]').setAppName('comparison').set("spark.executor.extraJavaOptions", "-XX:+UseG1GC").set('spark.executor.cores','1').set('spark.executor.memory','512M').set('spark.driver.memory','512M').set('spark.driver.maxResultSize','512M').set('spark.eventLog.enabled','false').set('spark.executor.heartbeatInterval','4900s').set('spark.network.timeout','10000000s')
dm_conf = SparkConf().setMaster('local[4]').setAppName('datamart').set('spark.executor.cores','2').set('spark.executor.memory','512M').set('spark.driver.memory','512M').set('spark.driver.maxResultSize', '512M').set('spark.eventLog.enabled','false')
# Set how much the RDD's of larger/smaller databases must be partitioned and parallelized
larger_partitioning = 64
smaller_partitioning = 8

#----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
##Set info to be used on extract step from second round 
sr_ex_directory_padronized_files =	str(config.default_folder)+"second_round_standardized/"
#Larger file
#Larger file name after standardization
sr_ex_larger_output_file = str(sr_ex_directory_padronized_files)+"larger_file_standardized.csv"
#Smaller file
#Smaller file name after standardization
sr_ex_smaller_output_file = str(sr_ex_directory_padronized_files)+"smaller_file_standardized.csv"

sr_e_status_name = 1						#Name
sr_e_status_mother_name = 1				#Mother name
sr_e_status_birth_date = 1					#Birth date
sr_e_status_gender = 1						#Gender
sr_e_status_municipality_residence = 0		#Municipality residence
sr_e_status_state = 0	

#File result_dedup.linkage that is used as reference to collect the records in gray area
sr_ex_reference_file = c_file_result

#----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
#Set info to be used on extract step from second round 
##Folder name to transformed files in second round
sr_e_directory_blocks = config.default_folder + "second_round_blocks/"
#Folder name to transformed files of larger base in second round
sr_e_directory_block_larger =  sr_e_directory_blocks + "block_larger/"
#Folder name to transformed files of smaller base in second round
sr_e_directory_block_smaller =  sr_e_directory_blocks + "block_smaller/"
#File name of blocking keys in second round
sr_e_file_key_blocking = "keyBloking.txt"
## Size of bloom vector in attributes that are transformed
sr_e_size_bloom_col = 100

					#State


sr_directory_results = config.default_folder + "second_round_linkage_results/"
sr_c_file_result = sr_directory_results + "result.linkage"
sr_c_file_result_rescue = sr_directory_results + "result-Rescue.linkage"
sr_c_file_result_final = sr_directory_results + "result_final.linkage"

sr_c_size_bloom_vector = 100
sr_c_boundary_name_1 = 9000
sr_c_boundary_name_2 = 8000
sr_c_boundary_name_3 = 7000
sr_c_boundary_name_4 = 6500
sr_c_boundary_birth = 7000
sr_c_number_of_attributes = 7
sr_c_size_in_bits = 100



#Folder name to datamart files
sr_dm_directory_datamarts = config.default_folder + "second_round_dataMarts/"

sr_dm_input_file = sr_c_file_result

#Output file to the dataMart module
sr_dm_output_file1 = sr_dm_directory_datamarts +"dataMart_type1.csv"
sr_dm_output_file2 = sr_dm_directory_datamarts +"dataMart_type2.csv"
sr_dm_output_file3 = sr_dm_directory_datamarts +"dataMart_type3.csv"
sr_dm_output_file4 = sr_dm_directory_datamarts +"dataMart_type4.csv"
sr_dm_output_file_final = sr_dm_directory_datamarts +"dataMart_type_final.csv"

#Larger base file used to retrieve the tuples to the datamart
sr_dm_larger_base_for_merge = pp_directory_padronized_files +"larger_file_with_index.csv"
#Smaller base file used to retrieve the tuples to the datamart
sr_dm_smaller_base_for_merge = pp_directory_padronized_files +"smaller_file_with_index.csv"
