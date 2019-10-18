#import pandas

#def pars_xlsx_to_json(xlsx_file, json_file):
#    df = pandas.read_excel(xlsx_file)
#    df.to_json(path_or_buf=json_file, orient='table')
#    return

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




