#!/bin/env python
# coding: utf-8

# createBlockKey.py:    In case of using blocking/indexing, this script will help to construct the comparison structure using the blocking keys.  
# __author__      =     "Robespierre Dantas Rocha Pita and Clicia dos Santos Pinto"
# emails =          robespierre.pita@ufba.br , cliciasp1@gmail.com
# paper: https://dx.doi.org/10.1109/JBHI.2018.2796941 
# AtyImoLab: http://www.atyimolab.ufba.br/

from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
from unicodedata import normalize
from doctest import testmod
from operator import add
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import csv
import time
import hashlib
import os
import os.path
import commands
import config
import config_static
ini = time.time() # Iniciando contagem de Tempo

conf = config_static.en_conf
sc = SparkContext(conf=conf)
print "MAIN PROGRAM MESSAGE (createBK):             createBK starting..."

def set_variables():
    print "MAIN PROGRAM MESSAGE (createBK):             In set_variables()"
    global keyFolder
    keyFolder = str(config_static.e_directory_key_folder)
    global status_larger_base
    status_larger_base = config.status_larger_base
    global status_smaller_base
    status_smaller_base = config.status_smaller_base
    global default_folder
    default_folder = config.default_folder      #OK

    global size_bloom_col_n
    size_bloom_col_n = config.e_size_bloom_col_n
    global size_bloom_col_mn
    size_bloom_col_mn = config.e_size_bloom_col_mn
    global size_bloom_col_bd
    size_bloom_col_bd = config.e_size_bloom_col_bd
    global size_bloom_col_mr
    size_bloom_col_mr = config.e_size_bloom_col_mr
    global size_bloom_col_g
    size_bloom_col_g = config.e_size_bloom_col_g    #OK

    global status_name
    status_name = config.e_status_name
    global status_birth_date
    status_birth_date = config.e_status_birth_date
    global status_gender
    status_gender = config.e_status_gender
    global status_mother_name
    status_mother_name = config.e_status_mother_name
    global status_municipality_residence
    status_municipality_residence = config.e_status_municipality_residence
    global status_state
    status_state = config.e_status_state

    global col_i
    col_i = config_static.e_col_i
    global col_n
    col_n = config_static.e_col_n
    global col_mn
    col_mn = config_static.e_col_mn
    global col_bd
    col_bd = config_static.e_col_bd
    global col_g
    col_g = config_static.e_col_g
    global col_mr
    col_mr = config_static.e_col_mr
    global col_st
    col_st = config_static.e_col_st


def set_variables_larger():
    print "MAIN PROGRAM MESSAGE (createBK):             In set_variables_larger()"

    global partitioning
    partitioning = config_static.larger_partitioning
    global input_file
    input_file = config_static.e_smaller_input_file#e_largest_input_file
    print "MAIN PROGRAM MESSAGE (createBK):             Input File: " +input_file
    global outputFolder
    outputFolder = directory_block_larger

    
def set_variables_smaller():
    print "MAIN PROGRAM MESSAGE (createBK):             In set_variables_smaller()"

    global partitioning
    partitioning = config_static.smaller_partitioning
    global input_file
    input_file = config_static.e_smaller_input_file
    print "MAIN PROGRAM MESSAGE (createBK):             Input FIle: " +input_file
    global outputFolder
    outputFolder = directory_block_smaller


def create_path():
    print "MAIN PROGRAM MESSAGE (createBK):             In create_path()"
    global directory_main
    directory_main = config_static.e_directory_blocks
    global directory_block_larger
    directory_block_larger = config_static.e_directory_block_larger
    global directory_block_smaller
    directory_block_smaller = config_static.e_directory_block_smaller


def norm(txt):
    return normalize('NFKD', txt).encode('ASCII','ignore').decode('ASCII').upper()

def is_vowel(char):
    if char in "AEIOU" : return 1
    else : return 0

def metaPTBR(STRING):
    META_KEY = ""
    CURRENT_POS = 0
    STRING_LENGTH = len(STRING)
    END_OF_STRING_POS = STRING_LENGTH-1
    ORIGINAL_STRING = " " + STRING + "00"
    #ORIGINAL_STRING = ORIGINAL_STRING.replace("Ccedilha","SS")
    ORIGINAL_STRING = ORIGINAL_STRING.replace("LH","1")
    ORIGINAL_STRING = ORIGINAL_STRING.replace("NH","3")
    ORIGINAL_STRING = ORIGINAL_STRING.replace("RR","2")
    ORIGINAL_STRING = ORIGINAL_STRING.replace("XC","SS")
    ORIGINAL_STRING = ORIGINAL_STRING.replace("SCH","X")
    ORIGINAL_STRING = ORIGINAL_STRING.replace("TH","T")
    ORIGINAL_STRING = ORIGINAL_STRING.replace("PH","F")
    while (1):
        CURRENT_CHAR = ORIGINAL_STRING[CURRENT_POS]
        if CURRENT_CHAR == "0" : break
        if is_vowel(CURRENT_CHAR) and (CURRENT_POS == 0 or ORIGINAL_STRING[CURRENT_POS - 1] == " "):
            META_KEY += CURRENT_CHAR
            CURRENT_POS += 1
        elif CURRENT_CHAR in "123BDFJKLMPTV" :       
            META_KEY += CURRENT_CHAR
            if ORIGINAL_STRING[CURRENT_POS + 1] == CURRENT_CHAR :
                    CURRENT_POS += 2
            else : CURRENT_POS += 1   
        else:
            if CURRENT_CHAR == "G":
                    if ORIGINAL_STRING[CURRENT_POS+1] == "E" or ORIGINAL_STRING[CURRENT_POS+1] == "I":
                        META_KEY   += "J"
                        CURRENT_POS += 2
                    elif CURRENT_CHAR == "U":
                        META_KEY   += "G"
                        CURRENT_POS += 2
                    elif CURRENT_CHAR == "R":
                        META_KEY +="GR"
                        CURRENT_POS += 2
                    else:
                        META_KEY   += "G"
                        CURRENT_POS += 2
            elif CURRENT_CHAR == "U":
                if is_vowel(ORIGINAL_STRING[CURRENT_POS-1]) :             
                    CURRENT_POS+=1
                    META_KEY+="L"
                else : CURRENT_POS += 1
            elif CURRENT_CHAR == "R":
                if CURRENT_POS==0 or ORIGINAL_STRING[CURRENT_POS - 1] == " " :                
                    CURRENT_POS+=1
                    META_KEY+="2"
                elif CURRENT_POS==END_OF_STRING_POS or ORIGINAL_STRING[CURRENT_POS + 1] == " ":                
                    CURRENT_POS+=1
                    META_KEY+="2"
                elif is_vowel(ORIGINAL_STRING[CURRENT_POS-1]) and is_vowel(ORIGINAL_STRING[CURRENT_POS+1]) :
                    CURRENT_POS+=1
                    META_KEY+="R"
                else:
                    CURRENT_POS += 1
                    META_KEY+="R"
            elif CURRENT_CHAR == "Z":
                if CURRENT_POS >= len(ORIGINAL_STRING)-1 :
                    CURRENT_POS+=1
                    META_KEY+="S"
                elif ORIGINAL_STRING[CURRENT_POS+1]=="Z" :
                    META_KEY+="Z"
                    CURRENT_POS += 2
                else:    
                    CURRENT_POS += 1
                    META_KEY   += "Z"
            elif CURRENT_CHAR == "N":
                if CURRENT_POS >= len(ORIGINAL_STRING)-1 :
                    META_KEY   += "M"
                    CURRENT_POS += 1
                elif ORIGINAL_STRING[CURRENT_POS+1] =="N" :
                    META_KEY   += "N"
                    CURRENT_POS += 2
                else:    
                    META_KEY   += "N"
                    CURRENT_POS += 1
            elif CURRENT_CHAR == "S":
                if ORIGINAL_STRING[CURRENT_POS+1]=="S" or CURRENT_POS==END_OF_STRING_POS or ORIGINAL_STRING[CURRENT_POS+1] ==" " :               
                    META_KEY += "S"
                    CURRENT_POS += 2               
                elif CURRENT_POS==0 or ORIGINAL_STRING[CURRENT_POS-1] == " " :               
                    META_KEY += "S"
                    CURRENT_POS += 1                
                elif is_vowel(ORIGINAL_STRING[CURRENT_POS-1]) and is_vowel(ORIGINAL_STRING[CURRENT_POS+1]) :               
                    META_KEY += "Z"
                    CURRENT_POS += 1
                elif ORIGINAL_STRING[CURRENT_POS+1] =="C" and (ORIGINAL_STRING[CURRENT_POS+2]=="E" or ORIGINAL_STRING[CURRENT_POS+2]=="I") :
                    META_KEY += "S"
                    CURRENT_POS += 3
                elif ORIGINAL_STRING[CURRENT_POS+1] =="C" and (ORIGINAL_STRING[CURRENT_POS+2]=="A" or ORIGINAL_STRING[CURRENT_POS+2]=="O" or ORIGINAL_STRING[CURRENT_POS+2]=="U") :
                    META_KEY += "SC"
                    CURRENT_POS += 3
                else:
                    META_KEY   += "S"
                    CURRENT_POS += 1
            elif CURRENT_CHAR == "X":
                if ORIGINAL_STRING[CURRENT_POS-1] =="E" and CURRENT_POS==1 :
                    META_KEY += "Z"
                    CURRENT_POS += 1
                elif ORIGINAL_STRING[CURRENT_POS-1] =="I" and CURRENT_POS==1 :
                    META_KEY += "X"
                    CURRENT_POS += 1
                elif is_vowel(ORIGINAL_STRING[CURRENT_POS-1]) and CURRENT_POS==1 :
                    META_KEY += "KS"
                    CURRENT_POS += 1
                else:
                    META_KEY += "X"
                    CURRENT_POS += 1
            elif CURRENT_CHAR == "C":
                if ORIGINAL_STRING[CURRENT_POS + 1] == "E" or ORIGINAL_STRING[CURRENT_POS + 1] == "I":
                    META_KEY   += "S"
                    CURRENT_POS += 2
                elif ORIGINAL_STRING[CURRENT_POS + 1]=="H" :
                    META_KEY   += "X"
                    CURRENT_POS += 2
                else:
                    META_KEY   += "K"
                    CURRENT_POS += 1
            elif CURRENT_CHAR == "H":
                if is_vowel(ORIGINAL_STRING[CURRENT_POS + 1]) :
                    META_KEY += ORIGINAL_STRING[CURRENT_POS + 1]
                    CURRENT_POS += 2
                else:
                    CURRENT_POS += 1
            elif CURRENT_CHAR == "Q":
                if ORIGINAL_STRING[CURRENT_POS + 1] == "U" :
                  CURRENT_POS += 2
                else :
                    CURRENT_POS += 1
                META_KEY += "K"
            elif CURRENT_CHAR == "W":
                if is_vowel(ORIGINAL_STRING[CURRENT_POS + 1]) :    
                    META_KEY   += "V"
                    CURRENT_POS += 2
                else:
                    META_KEY   += "U"
                    CURRENT_POS += 2
            else :
                CURRENT_POS += 1
    return META_KEY


def getKeys(line):
    #print line

    line_received = line.split(";")

    estado = str(line_received[col_st])
    cidade = str(line_received[col_mr])
    cidade = cidade.zfill(5)
    cidade = cidade[:4]
   
    nome = str(line_received[col_n])
    dataNascimento = str(line_received[col_bd])
    index = str(line_received[col_i])
    sexo = str(line_received[col_g])
    nomeMae = str(line_received[col_mn])

    data = dataNascimento

    diaMes = str(dataNascimento[6:8]) + str(dataNascimento[4:6])
    diaAno = str(dataNascimento[6:8]) + str(dataNascimento[:4])
    mesAno = str(dataNascimento[4:6]) + str(dataNascimento[:4])
    
    #Separando o nome em fatias
    nomeSeparado = nome.split(" ")
    primeiroNome = nomeSeparado[0]
    ultimoNome = nomeSeparado[(len(nomeSeparado)-1)]
    #Separando a data em fatias
    dataSeparada = dataNascimento.split("-")

    nomeSeparadoMae = nomeMae.split(" ")
    primeiroNomeMae = nomeSeparadoMae[0]
    ultimoNomeMae = nomeSeparadoMae[(len(nomeSeparadoMae)-1)]
    #Separando a data em fatias

    try:
        anoNascimento = dataSeparada[0]
    except Exception:
        anoNascimento = "9999"



    linha = str(parte1) + ";" + str(parte2) + ";" + str(parte3) + ";" + str(parte4) + ";" +str(parte5) + ";" + str(parte6)+ ";" 
 
    numBloco = "BL" + hashlib.md5(linha).hexdigest() 

    novaLinha = linha + str(numBloco) + ";" + "fim"

    return novaLinha

def printFile(line):
    split = line.split(';')
    try:
        l = open(keyFolder+ "keys.txt", 'a')
        l.write(line + "\n")
    except Exception:
        print "Something went wrong: " + str(split[0])

set_variables()
create_path()

fim = time.time()
approx_time = fim - ini
print "MAIN PROGRAM MESSAGE (createBK):          Nothing to do. CreateBlockKey completed in: " + str(approx_time)

fim = time.time()
approx_time = fim - ini
print "MAIN PROGRAM MESSAGE (createBK):          createBlockKey completed in: " + str(approx_time)
