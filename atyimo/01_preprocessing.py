#!/bin/env python
# coding: utf-8
 
# preprocessing.py: Standardize the variables in a data base
# __author__ =      "Robespierre Dantas Rocha Pita and Clicia dos Santos Pinto"
# emails =          robespierre.pita@ufba.br , cliciasp1@gmail.com
# paper: https://dx.doi.org/10.1109/JBHI.2018.2796941 
# AtyImoLab: http://www.atyimolab.ufba.br/

from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
from unicodedata import normalize
from doctest import testmod
import csv
import time
import unicodedata
import string
import config
import config_static
import os
import commands

print "MAIN PROGRAM MESSAGE (preprocessing):        Preprocessing starting..."
 
ini = time.time()
 
conf = config_static.pp_conf
sc = SparkContext(conf=conf)

def set_variables():
   
    global status_larger_base
    status_larger_base = config.status_larger_base
    global status_smaller_base
    status_smaller_base = config.status_smaller_base
    global default_folder
    default_folder = config.default_folder   

def set_variables_larger():
    print "MAIN PROGRAM MESSAGE (preprocessing):        In set_variables_larger()"

    global input_file
    input_file = directory_padronizedfiles +"larger_file.csv"
    os.system("cp " +str(config.pp_larger_input_file) +" " +directory_padronizedfiles +"larger_file.csv")

    print "MAIN PROGRAM MESSAGE (preprocessing):        Input File: " +input_file

    global partitioning
    partitioning = config_static.larger_partitioning
    global output_file
    output_file = config_static.pp_larger_output_file
    global status_label
    status_label = config.pp_larger_status_label
    global status_index
    status_index = config_static.pp_larger_status_index
    global status_code
    status_code = config.pp_larger_status_code
    global status_name
    status_name = config.pp_larger_status_name
    global status_birth_date
    status_birth_date = config.pp_larger_status_birth_date
    global status_gender
    status_gender = config.pp_larger_status_gender
    global status_mother_name
    status_mother_name = config.pp_larger_status_mother_name
    global status_municipality_residence
    status_municipality_residence = config.pp_larger_status_municipality_residence
    global status_state
    status_state = config.pp_larger_status_state
    global type_date1
    type_date1 = config.pp_larger_type_date1
    global type_date2
    type_date2 = config.pp_larger_type_date2
    global type_date3
    type_date3 = config.pp_larger_type_date3
    global type_date4
    type_date4 = config.pp_larger_type_date4
    global type_date5
    type_date5 = config.pp_larger_type_date5
    global type_gender1
    type_gender1 = config.pp_larger_type_gender1
    global type_gender2
    type_gender2 = config.pp_larger_type_gender2
    global type_gender3
    type_gender3 = config.pp_larger_type_gender3
    global type_municipality1
    type_municipality1 = config.pp_larger_type_municipality1
    global type_municipality2
    type_municipality2 = config.pp_larger_type_municipality2
    global type_municipality3
    type_municipality3 = config.pp_larger_type_municipality3
    global col_i
    col_i = config_static.pp_larger_col_i
    global col_c
    col_c = config.pp_larger_col_c +1
    global col_n
    col_n = config.pp_larger_col_n +1
    global col_mn
    col_mn = config.pp_larger_col_mn +1
    global col_bd
    col_bd = config.pp_larger_col_bd +1
    global col_g
    col_g = config.pp_larger_col_g +1
    global col_mr
    col_mr = config.pp_larger_col_mr +1
    global col_st
    col_st = config.pp_larger_col_st +1
    print "MAIN PROGRAM MESSAGE (preprocessing):        All variables in set_variables_larger() are created"

def set_variables_smaller():
    print "MAIN PROGRAM MESSAGE (preprocessing):        In set_variables_smaller()"
    global input_file
    input_file = directory_padronizedfiles +"smaller_file.csv"
    os.system("cp " +str(config.pp_smaller_input_file) +" " +directory_padronizedfiles +"smaller_file.csv")
    print "MAIN PROGRAM MESSAGE (preprocessing):        Input File: " +input_file

    global partitioning
    partitioning = config_static.smaller_partitioning
    global output_file
    output_file = config_static.pp_smaller_output_file
    global status_label
    status_label = config.pp_smaller_status_label
    global status_index
    status_index = config_static.pp_smaller_status_index
    global status_code
    status_code = config.pp_smaller_status_code
    global status_name
    status_name = config.pp_smaller_status_name
    global status_birth_date
    status_birth_date = config.pp_smaller_status_birth_date
    global status_gender
    status_gender = config.pp_smaller_status_gender
    global status_mother_name
    status_mother_name = config.pp_smaller_status_mother_name
    global status_municipality_residence
    status_municipality_residence = config.pp_smaller_status_municipality_residence
    global status_state
    status_state = config.pp_smaller_status_state
    global type_date1
    type_date1 = config.pp_smaller_type_date1
    global type_date2
    type_date2 = config.pp_smaller_type_date2
    global type_date3
    type_date3 = config.pp_smaller_type_date3
    global type_date4
    type_date4 = config.pp_smaller_type_date4
    global type_date5
    type_date5 = config.pp_smaller_type_date5
    global type_gender1
    type_gender1 = config.pp_smaller_type_gender1
    global type_gender2
    type_gender2 = config.pp_smaller_type_gender2
    global type_gender3
    type_gender3 = config.pp_smaller_type_gender3
    global type_municipality1
    type_municipality1 = config.pp_smaller_type_municipality1
    global type_municipality2
    type_municipality2 = config.pp_smaller_type_municipality2
    global type_municipality3
    type_municipality3 = config.pp_smaller_type_municipality3
    global col_i
    col_i = config_static.pp_smaller_col_i
    global col_c
    col_c = config.pp_smaller_col_c +1
    global col_n
    col_n = config.pp_smaller_col_n +1
    global col_mn
    col_mn = config.pp_smaller_col_mn +1
    global col_bd
    col_bd = config.pp_smaller_col_bd +1
    global col_g
    col_g = config.pp_smaller_col_g +1
    global col_mr
    col_mr = config.pp_smaller_col_mr +1
    global col_st
    col_st = config.pp_smaller_col_st +1
    print "MAIN PROGRAM MESSAGE (preprocessing):        All variables in set_variables_smaller() are created"
    
def cleaner(): #Serial execution
    print "MAIN PROGRAM MESSAGE (preprocessing):        In cleaner()"

    aux = get_file_name()
    output_local_aux = str(directory_padronizedfiles) +aux +"_ctmp_aux.csv"
    output_local_aux2 = str(directory_padronizedfiles) +aux +"_ctmp_aux2.csv"
    output_local_aux3 = str(directory_padronizedfiles) +aux +"_ctmp_aux3.csv"
    output_local_clean = str(directory_padronizedfiles) +aux +"_tmp_clean.csv"
    os.system("cp -fv " +input_file +" " +output_local_aux)
    os.system("cp -fv " +output_local_aux +" " +output_local_aux2)
    os.system("cp -fv " +output_local_aux2 +" " +output_local_clean)
    os.system("cp -fv " +output_local_aux2  +" " +output_file)

 
def get_file_name():

    splited = input_file.split("/")
    last_item = splited[len(splited)-1]
    last_item_splited = last_item.split(".")
    name = last_item_splited[0]
    return name

def create_path():
	print "MAIN PROGRAM MESSAGE (preprocessing):        In create_path"
	global directory_padronizedfiles
	directory_padronizedfiles = config_static.pp_directory_padronized_files
	os.system("mkdir "+directory_padronizedfiles)

def add_index(): #Serial execution
	print "MAIN PROGRAM MESSAGE (preprocessing):        In add_index"
	input_local = get_file_name()

	final_output = str(directory_padronizedfiles) + input_local +"_with_index.csv"

	input_local = str(directory_padronizedfiles) + input_local +"_tmp_clean.csv"
	os.system("touch "+input_local)
	output_local = get_file_name()
	output_local = str(directory_padronizedfiles) + output_local +"_tmp_auxiliar.csv"

	if (status_index):
		k = open(input_local, 'r')
        	p = open(output_local, 'a')
		count = 0
		for row in k:
			linha = row.split(";")
			new = str(count)
			for i in range(len(linha)):
			    new += ";" + str(linha[i])
			count +=1
			#new += "\n"
			p = open(output_local, 'a')
			p.write(new)
		p.close()
		k.close()

		os.system("cp " +output_local + " " +final_output)
		os.system("rm -f " +input_local)
		os.system("rm -f " +output_local)
		os.system("rm -f " +input_file)

		global base_file
		base_file = final_output

def transform_name(name):
    
    name = name.upper()
    safe_chars = string.ascii_letters + string.digits + '_' + ' '
    cleanName=''.join([char if char in safe_chars else '' for char in name])

    codif='utf-8'
    name = normalize('NFKD', cleanName.decode(codif)).encode('ASCII','ignore')
 
    return name

def get_month_number(mes):
	mes = mes.upper()
	trans_month = {
		'JAN':1,
		'FEV':2,
		'MAR':3,
		'ABR':4,
		'MAI':5,
		'JUN':6,
		'JUL':7,
		'AGO':8,
		'SET':9,
		'OUT':10,
		'NOV':11,
		'DEZ':12,
		'FEB':2,
		'MAR':3,
		'APR':4,
		'MAY':5,
		'AUG':8,
		'SEP':9,
		'OCT':10,
		'DEC':12,
		'01':1,
		'02':2,
		'03':3,
		'04':4,
		'05':5,
		'06':6,
		'07':7,
		'08':8,
		'09':9,
		'10':10,
		'11':11,
		'12':12,
	}

	try:
		number_of_month = trans_month[mes]
		return number_of_month
	except Exception:
		return 0
 
def transform_date(date):

    # Padrão esperado: anomesdia

    date = date.strip()

    #TODO: Include other kinds of types here
    if (str(date) == "NA"):
        return 0
        

    if(type_date1):
        return date

    if(type_date2):

        date = str(date[-4:]) + str(date[-6:-4]).zfill(2) + str(date[-8:-6]).zfill(2)
        return date

    if (type_date3):
        try:
            datesplited = date.split("-")

            if (len(datesplited[2]) == 2):
                yeardate = "20"+str(datesplited[2])
            
            else:
                yeardate = str(datesplited[2])

            date = str(yeardate).zfill(4) + str(get_month_number(datesplited[1].zfill(2))).zfill(2)+ str(datesplited[0]).zfill(2)

        except Exception:
            try:
                datesplited = date.split("/")

                if (len(datesplited[2]) == 2):
                    yeardate = "20"+str(datesplited[2])

                else:
                    yeardate = str(datesplited[2])

                date = str(yeardate).zfill(4) + str(get_month_number(datesplited[1].zfill(2))).zfill(2)+ str(datesplited[0]).zfill(2)

            except Exception:
                date = ""
        return date

    if (type_date4):
        try:
            datesplited = date.split("-")

            if (len(datesplited[0]) == 2):
                yeardate = "20"+str(datesplited[0])
                
            else:
                yeardate = str(datesplited[0])

            date = str(yeardate).zfill(4) + str(get_month_number(datesplited[1].zfill(2))).zfill(2)+ str(datesplited[2]).zfill(2)

        except Exception:
            try:
                datesplited = date.split("/")

                if (len(datesplited[0]) == 2):
                    yeardate = "20"+str(datesplited[0])
                else:
                    yeardate = str(datesplited[0])

                date = str(yeardate).zfill(4) + str(get_month_number(datesplited[0].zfill(2))).zfill(2)+ str(datesplited[0]).zfill(2)

            except Exception:
                date = ""
        return date

    if (type_date5):
        try:
            datesplited = date.split("-")

            if (len(datesplited[2]) == 2):
                yeardate = "20"+str(datesplited[0])
                
            else:
                yeardate = str(datesplited[2])

            date = str(yeardate).zfill(4) + str(get_month_number(datesplited[0].zfill(2))).zfill(2)+ str(datesplited[1]).zfill(2)

        except Exception:
            try:
                datesplited = date.split("/")

                if (len(datesplited[2]) == 2):
                    yeardate = "20"+str(datesplited[0])
                else:
                    yeardate = str(datesplited[2])

                date = str(yeardate).zfill(4) + str(get_month_number(datesplited[0].zfill(2))).zfill(2)+ str(datesplited[1]).zfill(2)

            except Exception:
                date = ""
        return date

def transform_gender(gender):
    # Padrão esperado: M e F
    if(type_gender1):
        return gender
    
    if(type_gender2):
        if(str(gender) == "M"):
            gender = 1
        elif(str(gender) == "F"):
            gender = 2
        return gender
    
    if(type_gender3):
        if(str(gender) == "1"):
            gender = 1
        elif(str(gender) == "3"):
            gender = 2
        return gender
    
    # Falta Implementar #porfazer #todo
    if(type_gender4):
        return gender
    # Falta Implementar #porfazer #todo
    if (type_gender5):
        return gender

def transform_municipality(municipality):
    
    if(type_municipality1):
        municipality = municipality[:4].zfill(4)
        return municipality

    if(type_municipality2):
        municipality = municipality[2:6].zfill(4)
        return municipality

    if(type_municipality3):
        municipality = transform_name(municipality)
        return municipality

def transform_state(municipality):
    state = municipality[:2].zfill(2)
    return state


def convert(line):
    q = open(output_file, 'a')
    line = line.split(";")
    if (status_name):        
        new_name = transform_name(str(line[col_n]))        
    if (status_mother_name):
        new_mothers_name = transform_name(str(line[col_mn]))
    if (status_birth_date):
            new_birth_date = transform_date(str(line[col_bd]))
    #Não implementado #por_fazer #todo
    if (status_municipality_residence):
        new_municipality_residence = transform_municipality(str(line[col_mr]))  
    #Não implementado #por_fazer #todo
    if (status_state):
        new_state = str(line[col_st])
    elif(type_municipality2):
        new_state = transform_state(str(line[col_mr]))

    if (status_gender):
        new_gender = transform_gender(str(line[col_g]))   
    #Pode ser removido no futuro, pois espera-se que sempre seja criado este índice, independente da base a ser utilizada
    if (status_index):
        new_line = str(line[col_i])
    new_line += ";"
    if (status_code):
        new_line += str(line[col_c])
    new_line += ";"
    if (status_name):
        new_line += str(new_name)
    new_line += ";"
    if (status_mother_name):
        new_line += str(new_mothers_name)
    new_line += ";"
    if (status_birth_date):
        new_line += str(new_birth_date)
    new_line += ";"
    if (status_state or type_municipality2):
        new_line += str(new_state)
    new_line += ";"
    if (status_municipality_residence):
        new_line += str(new_municipality_residence)
    new_line += ";"
    if (status_gender):
        new_line += str(new_gender)
    new_line += "\n"
    print new_line 
    q.write(new_line)
    return 0

#Starting the firts variables
create_path()
set_variables()

flagl = 1
flags = 1
while(flagl or flags):
  
    if(status_larger_base and flagl): 
        print "MAIN PROGRAM MESSAGE (preprocessing):        Starting round of larger base  (1)"
        set_variables_larger()
        flagl = 0
        if(status_smaller_base == 0):
            flags = 0
    
    elif(status_smaller_base and flags):
        print "MAIN PROGRAM MESSAGE (preprocessing):        Starting round of smaller base (2)"
        set_variables_smaller()
        flags = 0
        flagl = 0
    
    #get_file_name()
    
    cleaner()
    input_local = get_file_name()
    base_file=str(directory_padronizedfiles) + input_local +"_with_index.csv"
    add_index()

    rdd_base_file = sc.textFile(base_file, partitioning)
    colectFile = rdd_base_file.map(convert).collect()

fim = time.time()
approx_time = fim - ini
print "MAIN PROGRAM MESSAGE (preprocessing):        Preprocessing completed in: " + str(approx_time)
