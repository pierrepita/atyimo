#!/bin/env python
# coding: utf-8

# datamart.py:	Returns a pair of tuple that contains the same key that the linkage file
# __author__ =	"Robespierre Dantas Rocha Pita and Clicia dos Santos Pinto"
# emails =          robespierre.pita@ufba.br , cliciasp1@gmail.com
# paper: https://dx.doi.org/10.1109/JBHI.2018.2796941 
# AtyImoLab: http://www.atyimolab.ufba.br/

from pyspark import SparkContext, SparkConf
from pyspark import SparkFiles
import csv
import time
import os
import ConfigParser
import string
import commands
import config
import config_static


ini = time.time() # Iniciando contagem de Tempo

conf = config_static.dm_conf
sc = SparkContext(conf=conf)

fim = time.time()
tempo1 = fim - ini

def set_variables():

	global part_larger
	partitioning = config_static.larger_partitioning

	global part_smaller
	partitioning = config_static.smaller_partitioning

	global arquivoLinkage
	arquivoLinkage = config_static.dm_input_file
	#testar se o nome do arquivo ja existe e criar arquivos numerados #todo

	global arquivoLinkage4
	arquivoLinkage4 = config_static.c_file_result
	
	global resultFile_type1
	resultFile_type1 = config_static.dm_output_file1

	global resultFile_type2
	resultFile_type2 = config_static.dm_output_file2

	global resultFile_type3
	resultFile_type3 = config_static.dm_output_file3

	global resultFile_type4
	resultFile_type4 = config_static.dm_output_file4
	
	name = get_file_name()  #ainda nao serve pra nada.
	
	global dm_type1
	dm_type1 = config.dm_type1

	global dm_type2
	dm_type2 = config.dm_type2

	global dm_type3
	dm_type3 = config.dm_type3

	global dm_type4
	dm_type4 = config.dm_type4

	global cod_larger_base 
	cod_larger_base = config.pp_larger_col_c + 1

	global cod_smaller_base 
	cod_smaller_base = config.pp_smaller_col_c + 1

	global baseMaior
	baseMaior = config_static.dm_larger_base_for_merge

	global baseMenor
	baseMenor = config_static.dm_smaller_base_for_merge
	

def create_path():
	#testar se a pasta ja existe e evitar warning
	global directory_datamart
	directory_datamart = config_static.dm_directory_datamarts
	os.system("mkdir "+directory_datamart)

def get_file_name():
	name = []
	
	splited = config.pp_larger_input_file.split("/")
	last_item = splited[len(splited)-1]
	last_item_splited = last_item.split(".")
	name.append(str(last_item_splited[0]) +"_with_index.csv")
	
	splited = config.pp_smaller_input_file.split("/")
	last_item = splited[len(splited)-1]
	last_item_splited = last_item.split(".")
	name.append(str(last_item_splited[0]) +"_with_index.csv")
	return name

def buscaLinhas(linha):

	linha = linha.split(";")

	chv1 = linha[1]
	chv2 = linha[2]
	
	if (dm_type4):
		r = open(resultFile_type4, 'a')
	else: 
		r = open(resultFile_type1, 'a')
	p = open(baseMaior)
	q = open(baseMenor)
	linhasBaseMaior = p.readlines()
	linhasBaseMenor = q.readlines()
	listaBaseMaior = str(linhasBaseMaior[int(chv2)]).split(";")
	listaBaseMenor = str(linhasBaseMenor[int(chv1)]).split(";")
	
	i = 1
	parte1 = str(listaBaseMaior[0])
	for i in range((len(listaBaseMaior))-2):
		parte1 += ";" + str(listaBaseMaior[i+1])
	my_string1 = str(listaBaseMaior[len(listaBaseMaior)-1])
	my_string1 = my_string1.rstrip()
	
	i = 1
	parte2 = str(listaBaseMenor[0]) 
	for i in range(len((listaBaseMenor))-2):
		parte2 += ";" + str(listaBaseMenor[i+1])
	
	my_string2 = str(listaBaseMenor[len(listaBaseMenor)-1])
	my_string2 = my_string2.rstrip()
	
	par = str(linha[0]) + ";" + str(parte1) +";"+ str(my_string1) +";" + str(parte2) + ";"+ str(my_string2) + "\n"
	r.write(par)
	return 0

def buscaLinhas2(linha):

	linha = linha.split(";")

	chv1 = linha[2]
	chv2 = linha[1]
	
	if (dm_type4):
		r = open(resultFile_type4, 'a')
	else: 
		r = open(resultFile_type1, 'a')
	p = open(baseMaior)
	q = open(baseMenor)
	linhasBaseMaior = p.readlines()
	linhasBaseMenor = q.readlines()
	listaBaseMaior = str(linhasBaseMaior[int(chv2)]).split(";")
	listaBaseMenor = str(linhasBaseMenor[int(chv1)]).split(";")
	
	i = 1
	parte1 = str(listaBaseMaior[0])
	for i in range((len(listaBaseMaior))-2):
		parte1 += ";" + str(listaBaseMaior[i+1])
	my_string1 = str(listaBaseMaior[len(listaBaseMaior)-1])
	my_string1 = my_string1.rstrip()
	
	i = 1
	parte2 = str(listaBaseMenor[0]) 
	for i in range(len((listaBaseMenor))-2):
		parte2 += ";" + str(listaBaseMenor[i+1])
	
	my_string2 = str(listaBaseMenor[len(listaBaseMenor)-1])
	my_string2 = my_string2.rstrip()
	
	par = str(linha[0]) + ";" + str(parte1) +";"+ str(my_string1) +";" + str(parte2) + ";"+ str(my_string2) + "\n"
	r.write(par)
	return 0

def buscadm2(linha):
	r = open(resultFile_type2, 'a')
	if linha[0] not in linkagebc.value:
		r.write(";".join(linha) + ";" + "false" + "\n")
		return ";".join(linha) + ";" + "false"
	else:
		r.write(";".join(linha) + ";" + "true" + "\n")
		return ";".join(linha) + ";" + "true"

def buscadm3(linha):
	line = ""
	r = open(resultFile_type3, 'a')
	line = line + largerbc.value[int(linha[1])] + ";" + "true" + ";" + str(linha[0]) + ";" + smallerbc.value[int(linha[2])]
	r.write(line + "\n")
	return 0

def buscanotinlinkage(linha):
	line = largerbc.value[int(linha)] + ";" + "false"
	r = open(resultFile_type3, 'a')
	r.write(line + "\n")
	return 0

def get_codes(line):
	return line[1]

def get_codes2(line):
	return line[0]

def notin(linha):
	linha = linha.split(";")
	if linha[0] not in codes_in_linkage:
		return linha[0]

create_path()
set_variables()

if (dm_type1):
	rddlinkage = sc.textFile(arquivoLinkage)
	registrosRecuperados = rddlinkage.cache().map(buscaLinhas).collect()


if (dm_type2):
	r = open(arquivoLinkage)
	p = open(baseMaior)
	q = open(baseMenor)

	codes_in_linkage = sc.parallelize(linkage, part_larger).map((lambda line: line.split(";"))).map(get_codes).cache().collect()
	linkagebc = sc.broadcast(codes_in_linkage)
	largerRDD = sc.parallelize(larger, part_larger).map((lambda line: line.split(";"))).map(buscadm2).collect()

if (dm_type3):
	#print "aqui"
	r = open(arquivoLinkage)
	p = open(baseMaior)
	q = open(baseMenor)
	larger = [line.strip() for line in p.readlines()]
	smaller = [line.strip() for line in q.readlines()]
	linkage = [line.strip() for line in r.readlines()]

	codes_in_linkage = sc.parallelize(linkage, part_larger).map((lambda line: line.split(";"))).map(get_codes).cache().collect()
	linkagebc = sc.broadcast(codes_in_linkage)
	#print linkagebc.value

	codes_of_larger = sc.parallelize(larger, part_larger).cache().map((lambda line: line.split(";"))).map(get_codes2).collect()
	codslarger = sc.broadcast(codes_of_larger) 

	colnil = [val for val in codslarger.value if val not in linkagebc.value]

	larger = sc.parallelize(larger, part_larger).cache().collect()
	largerbc = sc.broadcast(larger)
	#print largerbc.value

	smaller = sc.parallelize(smaller, part_smaller).cache().collect() 
	smallerbc = sc.broadcast(smaller)


	linkage = sc.parallelize(linkage, part_larger).map((lambda line: line.split(";"))).cache().map(buscadm3).collect()
	notinlinkage = sc.parallelize(colnil).cache().map(buscanotinlinkage).collect()

if (dm_type4):
        print "MAIN PROGRAM MESSAGE (geraDataMart):             Datamart type 4 has been choose, using file: " + str(arquivoLinkage4)
	rddlinkage = sc.textFile(arquivoLinkage4, config_static.smaller_partitioning)
	registrosRecuperados = rddlinkage.cache().map(buscaLinhas2).collect()

fim = time.time()
tempo = fim - ini


print "MAIN PROGRAM MESSAGE (geraDataMart):		geraDataMart completed in: " + str(tempo)
