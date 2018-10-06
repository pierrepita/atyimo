#!/bin/env python
# coding: utf-8

# dedupByKey.py =   Makes a deduplication by the smaller base's key.
# __author__    =   "Robespierre Dantas Rocha Pita and Clicia dos Santos Pinto"
# emails =          robespierre.pita@ufba.br , cliciasp1@gmail.com
# paper: https://dx.doi.org/10.1109/JBHI.2018.2796941 
# AtyImoLab: http://www.atyimolab.ufba.br/

import config
import config_static
import os
import commands
import time

# correlation.py:       Eliminate duplicate records given a specific key
# __author__    =       "Clicia dos Santos Pinto and Robespierre Dantas Rocha Pita

ini = time.time()
print "MAIN PROGRAM MESSAGE (DedupByKey):             DedupByKey starting..."
def set_variables():
    global dice_col
    dice_col = 0
    global status_rescue
    status_rescue = config_static.c_status_rescue
    global status_dmfour
    status_dmfour = config.dm_type4
    global dm_type4
    dm_type4 = config.dm_type4


def set_variables_step1():
    print "MAIN PROGRAM MESSAGE (DedupByKey):             In set_variables_step1()"
    global input_file
    input_file = config_static.d_input_file
    global output_tmp
    output_tmp = config_static.d_tmp_step1_output_file
    global output_file
    output_file = config_static.d_output_file_step1
    global key_col
    key_col = config_static.d_key_col_step1
    if(dm_type4):
        global key_col
        key_col = config_static.d_key_col_step2        
        
   
def set_variables_step2():
    print "MAIN PROGRAM MESSAGE (DedupByKey):             In set_variables_step2()"
    global input_file
    input_file = config_static.d_output_file_step1
    global output_tmp
    output_tmp = config_static.d_tmp_step2_output_file        
    global output_file
    output_file = config_static.d_output_file_step2    
    global key_col
    key_col = config_static.d_key_col_step2
    

def set_variables_rescue_step1():
    print "MAIN PROGRAM MESSAGE (DedupByKey):             In set_variables_rescue_step1()"
    global input_file
    input_file = config_static.c_file_result_rescue
    global output_tmp
    output_tmp = config_static.d_tmp_step1_output_file
    global output_file
    output_file = config_static.d_output_file_step1
    global key_col
    key_col = config_static.d_key_col_step1
    

def set_variables_rescue_step2():
    print "MAIN PROGRAM MESSAGE (DedupByKey):             In set_variables_rescue_step2()"
    global input_file
    input_file = config_static.d_output_file_step1
    global output_tmp
    output_tmp = config_static.d_tmp_step2_output_file        
    global output_file
    output_file = config_static.d_output_file_step2_rescue
    global key_col
    key_col = config_static.d_key_col_step2

def sort_dice():
    print "MAIN PROGRAM MESSAGE (DedupByKey):             In sort_dice()"
    
    os.system("sort -nr  -t\; -k " +str(key_col+1)+","+str(key_col+1) +" -k "+str(dice_col+1)+","+str(dice_col+1) +" " +input_file +" > " +output_tmp)

def main():
    print "MAIN PROGRAM MESSAGE (DedupByKey):             In drive()"
    p = open(output_tmp, 'r')
    valores = []
    for row in p:
        valores.append(str(row))
 
    TAM = len(valores)
    p.close()
   
    i = 0
    listaAuxiliar = []
    flag = 0
   
    q = open(output_file, 'a')
   
    while (i<TAM-1):
        linhaAtual = valores[i]
        linhaProxima = valores[i+1]
        linhaAtualSplited = linhaAtual.split(";")
        linhaProximaSplited = linhaProxima.split(";")
        elementoAtual = linhaAtualSplited[key_col]
        elementoProximo = linhaProximaSplited[key_col]
       
        if (str(elementoProximo) != str(elementoAtual)):
            if(flag == 0):
                q.write(str(linhaAtual))
            else:
                flag = 0
        else:
            if(flag == 0):
                q.write(str(linhaAtual))
                flag = 1
        i += 1 
   
    if (i == TAM-1):   
        ultimaLinha = valores[i]
        penultimaLinha = valores[i-1]      
        ultimaLinhaSplited = ultimaLinha.split(";")
        penultimaLinhaSplited = penultimaLinha.split(";")
 
        if (str(ultimaLinhaSplited[key_col]) != str(penultimaLinhaSplited[key_col])):
            q.write(str(ultimaLinha))


set_variables()



if (dm_type4):
    print "MAIN PROGRAM MESSAGE (DedupByKey):             Datamart type 4 has been choosen, sort and deduplicate by smaller base"
    #Deduplicando pela coluna da base menor
    set_variables_step1()
    sort_dice()
    main()

    os.system("mv -fv " +config_static.d_output_file_step1 + " " + config_static.d_output_file_step2)

else:
    #Deduplicando pela coluna da base maior
    set_variables_step1()
    sort_dice()
    main()

    #Deduplicando pela coluna da base menor
    set_variables_step2()
    sort_dice()
    main()
    os.system("rm " +config_static.d_tmp_step1_output_file)
    os.system("rm " +config_static.d_tmp_step2_output_file)
    os.system("rm " +config_static.d_output_file_step1)
    os.system("rm " +config_static.c_file_result) 

if(status_rescue):
    #Deduplicando pela coluna da base maior
    set_variables_rescue_step1()
    sort_dice()
    main()

    #Deduplicando pela coluna da base menor
    set_variables_rescue_step2()
    sort_dice()
    main()

    os.system("rm " +config_static.d_tmp_step1_output_file)
    os.system("rm " +config_static.d_tmp_step2_output_file)
    
    os.system("rm " +config_static.d_output_file_step1)    
    os.system("rm " +config_static.c_file_result_rescue)

fim = time.time()
tempo = fim - ini
print "MAIN PROGRAM MESSAGE (dedupByKey):          DedupByKey completed in: " + str(tempo)
