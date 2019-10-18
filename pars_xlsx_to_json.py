# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 12:57:05 2019

@author: Matan
"""
#this function should run on a list of XLSX files we recieve from the user, and
#a tool name list we make

import pandas

def pars_xlsx_to_json(tool_name_list,xlsx_file_list, json_file):
    tuple_list = []
    for i in range(0,len(xlsx_file_list)):
        df = pandas.read_excel(xlsx_file_list[i])
        df.to_json(path_or_buf=json_file, orient='table')
        tuple_ = (tool_name_list[i],df)
        tuple_list.append(tuple_)
    return tuple_list