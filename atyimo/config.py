#!/bin/env python
# coding: utf-8

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
status_larger_base = 1 #Boolean indicating execution of larger base
status_smaller_base = 1 #Boolean indicating execution of small base
default_folder = "/scratch/robespierre.pita/sim1a4_baseline/exe/" #main folder of each execution

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
# Set everything to be used in preprocessing module
#Larger base
#Larger input file to the preprocessing module 
pp_larger_input_file = "/scratch/robespierre.pita/sim1a4_baseline/bases/baseline_menor_7anos_linkage_reorganized.csv"

#Boolean indicating the existence of labels on the larger base
pp_larger_status_label = 1
#Boolean indicating the existence of these variables in the larger base
pp_larger_status_code = 1                               #Code
pp_larger_status_name = 1								#Name
pp_larger_status_birth_date = 1                         #Birth Date
pp_larger_status_gender = 1                             #Gender
pp_larger_status_mother_name = 1                        #Mother Name
pp_larger_status_municipality_residence = 1     		#Municipality Residence
pp_larger_status_state = 0                              #State

#Boolean indicating the date type of larger base
pp_larger_type_date1 = 0 #yearmonthday examples: 19990414  <- STANDARD
pp_larger_type_date2 = 0 #daymonthyear examples: 14041999
pp_larger_type_date3 = 0 #day<"-"||"/">month<"-"||"/">year examples: 14/mar/08 or 14/mar/2008 or 1-Dec-2006 or 1-Dec-06
pp_larger_type_date4 = 1 #year-month-day examples: 1963-01-29  year<"-"||"/">month<"-"||"/">day examples: 1999/mar/08 or 08/mar/29 or 2006-Dec-1 or 06-Dec-01
pp_larger_type_date5 = 0 #month<"-"||"">year examples: 11/13/1965 or nov/13/1965 or 1965-Nov-13

#Boolean indicating the gender type of larger base
pp_larger_type_gender1 = 1 #1 for male and 2 for female  <- STANDARD
pp_larger_type_gender2 = 0 #M for male and F for female
pp_larger_type_gender3 = 0 #1 for male and 3 for female

#Boolean indicating the municipality type of larger base
pp_larger_type_municipality1 = 0  #Ibge code with 4 or 5 digits <- STANDARD
pp_larger_type_municipality2 = 1  #Ibge code with 6 or 7 digits (state included)
pp_larger_type_municipality3 = 0  #Name of municipality

#Item position in the larger input file
pp_larger_col_c = 0 	    	#Code
pp_larger_col_n = 1         	#Name
pp_larger_col_mn = 2			#Mother name
pp_larger_col_bd = 3          	#Birth date
pp_larger_col_g =  5   		#Gender
pp_larger_col_mr = 4      		#Municipality residence
pp_larger_col_st = -1  			#State

#Smaller base
#Smaller input file to the preprocessing module
pp_smaller_input_file = "/scratch/robespierre.pita/sim1a4_baseline/bases/extracao_linkage_1a4anos_somente_sem_missing_nome_18_09_2017.csv"

#Boolean indicating the existence of labels on the smaller base
pp_smaller_status_label = 1 #Boolean indicating the existence of labels on the smaller base

#Boolean indicating the existence of these variables in the smaller base
pp_smaller_status_code = 1					#Code
pp_smaller_status_name = 1					#Name
pp_smaller_status_birth_date = 1				#Birth Date
pp_smaller_status_gender = 1					#Gender
pp_smaller_status_mother_name = 1				#Mother Name
pp_smaller_status_municipality_residence = 1			#Municipality Residence
pp_smaller_status_state = 0					#State

#Boolean indicating the date type of smaller base
pp_smaller_type_date1 = 0 # yearmonthday examples: 19990414  <- PADRÃO
pp_smaller_type_date2 = 0 # daymonthyear examples: 14041999
pp_smaller_type_date3 = 0 # day<"-"||"/">month<"-"||"/">year examples: 14/mar/08 or 14/mar/2008 or 1-Dec-2006 or 1-Dec-06 (only for years after 1999)
pp_smaller_type_date4 = 1 # year-month-day examples: 1963-01-29  year<"-"||"/">month<"-"||"/">day examples: 1999/mar/08 or 08/mar/29 or 2006-Dec-1 or 06-Dec-01
pp_smaller_type_date5 = 0 #month<"-"||"/">day<"-"||"/">year examples: 11/13/1965 or nov/13/1965 or 1965-Nov-13

#Boolean indicating the gender type of smaller base
pp_smaller_type_gender1 = 1  # #1 for male and 2 for female  <- STANDARD
pp_smaller_type_gender2 = 0  # #M for male and F for female
pp_smaller_type_gender3 = 0  #1 for male and 3 for female

#Boolean indicating the municipality type of smaller base
pp_smaller_type_municipality1 = 0  #Ibge code with 4 or 5 digits <- STANDARD
pp_smaller_type_municipality2 = 1  #Ibge code with 6 or 7 digits (state included)
pp_smaller_type_municipality3 = 0  #Name of municipality

#Item position in the smaller input file
pp_smaller_col_c = 6		#Code
pp_smaller_col_n = 0		#Name
pp_smaller_col_mn = 1		#Mother name
pp_smaller_col_bd = 2		#Birth date
pp_smaller_col_g = 3		#Gender
pp_smaller_col_mr = 4		#Municipality residence
pp_smaller_col_st = -1		#State

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
#Set info to be used in correlation module
#Main cutoff (Dice)
c_cutoff_result = 8000

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
#Boolean indicating the existence of these variable in the bloom vector
e_status_name = 1						#Name
e_status_mother_name = 1				#Mother name
e_status_birth_date = 1					#Birth date
e_status_gender = 1						#Gender
e_status_municipality_residence = 1		#Municipality residence
e_status_state = 0						#State

#Size of these variables on the bloom vector
e_size_bloom_col_n = 50 	#Name
e_size_bloom_col_mn = 50 	#Mothers name
e_size_bloom_col_bd = 40 	#Birth date
e_size_bloom_col_mr = 20 	#Municipality Residence
e_size_bloom_col_g = 40		#Gender

##----##----##----##----##----##----##----##----##----##----##----##----##----##--------##----##----##----##----##----##----
#Set info to be used in datamart build module
#Type of resulting datamart. Be free to choose more than one.
dm_type1 = 0			# [default] attributes of larger database ; matched attributes of smaller database
dm_type2 = 0			# attributes of larger database ; flag indicating pairing (true of false)
dm_type3 = 0			# attributes of larger database ; flag indicating pairing (true of false) ; matched attributes of smaller database
dm_type4 = 1			# All matches from smaller database with their highest dices pairs in larger database

#Set info to be used on second round
#Boolean indicating the second round of verification on gray area
sr_status_second_round = 0
#higher cutoff
sr_higher_cutoff = 9500.0
