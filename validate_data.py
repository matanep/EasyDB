# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 18:15:44 2019

@author: Matan
"""
def make_df(path): 
    import pandas as pd
    return pd.read_csv(path)

def correct_idn(idn):
    import regex as re
    return re.match(r'^([0-9]{9})+$', str(idn))

def correct_id(id):
    import regex as re
    import math
    
    if type(id) == str:
        if id == "":
            return False
        elif type(id) == int or type(id) == float:
            if id == None:
                return False
        elif type(id).__module__ == 'numpy' and math.isnan(id):
            return False

    return re.match(r'^[0-9]+$', str(id))

def validate_type(id_or_idn, df, file_name):
    import math
        
    if id_or_idn == "id":
        l = df['subject_id']
    else:
        l = df['subject_id_number']

    for j in range(0, len(l)):
        
        idn = l[j]
        
        if (type(idn) == str) and not(idn==""):
            if id_or_idn == "id":
                if not correct_id(idn):
                    raise SystemExit("Not all subject ids in " + str(file_name) + " are correct: " + str(idn)) ###logger
            elif not correct_idn(idn):
                    raise SystemExit("Not all subject id numbers in " + str(file_name) + " are correct: " + str(idn)) ###logger
            else: 
                l[j] = int(l[j])
        
        elif (type(idn).__module__ == 'numpy') and not(math.isnan(idn)):
            idn = int(idn.item())
            if id_or_idn == "id":
                if not correct_id(idn):
                    raise SystemExit("Not all subject ids in " + str(file_name) + " are correct: " + str(idn)) ###logger
            elif not correct_idn(idn):
                raise SystemExit("Not all subject id numbers in " + str(file_name) + " are correct: " + str(idn)) ###logger
            else: 
                l[j] = int(l[j])
                
        elif type(idn) == float and not(idn==None):
            if id_or_idn == "id":
                if not correct_id(idn):
                    raise SystemExit("Not all subject ids in " + str(file_name) + " are correct: " + str(idn)) ###logger
            elif not correct_idn(idn):
                raise SystemExit("Not all subject id numbers in " + str(file_name) + " are correct: " + str(idn)) ###logger
            else: 
                l[j] = int(l[j])
                
        elif type(idn) == int and not(idn==None):
            if id_or_idn == "id":
                if not correct_id(idn):
                    raise SystemExit("Not all subject ids in " + str(file_name) + " are correct: " + str(idn)) ###logger
            elif not correct_idn(idn):
                raise SystemExit("Not all subject id numbers in " + str(file_name) + " are correct: " + str(idn)) ###logger
            else: 
                l[j] = int(l[j])
 
        elif (type(idn) == int and (idn==None)) or (type(idn) == float and (idn==None)) or ((type(idn).__module__ == 'numpy') and math.isnan(idn)) or ((type(idn) == str) and not(idn=="")):
            if id_or_idn == "id":
                raise SystemExit("Some ID numbers' in " + str(file_name) + " type are empty or unrecognized") ###logger                  

        else:
            try:
                l[j] = int(l[j])
            except ValueError:
                raise SystemExit("Some ID numbers' in " + str(file_name) + " type are empty or unrecognized") ###logger

##############################################################################################################
##############################################################################################################
##############################################################################################################

def validate_data(uid,exp_data,mycursor,mydb):
    
    import pandas as pd
    from prepare_data import prepare_data
         
    df_list = []
    idn_df_list = []
    idn_name_list = []
    
    for i in range(0, len(exp_data)):
        df = pd.DataFrame.from_dict(exp_data[i][1])
        df_list.append(df)
        #check which df's contain the subect_id_number field, and mark the one with the longest unique list
        if 'subject_id_number' in df.columns:
            idn_df_list.append(df)
            idn_name_list.append(exp_data[i][0])
            
    #preliminary test that all id numbers are of the correct type. Leave only native integers (convert if needed)            
            validate_type("idn", df, exp_data[i][0])
    
    #all df's must contain a subject_id column
    try:
        subject_id_union = df_list[0]['subject_id']
    except KeyError:
            raise SystemExit("Not all CSVs contain a subject_id column") ###logger
    
    #add validation to check no more than 1 subject_id column exists?
    
    for i in range(1, len(df_list)):
        subject_id_union = pd.concat([subject_id_union, df_list[i]['subject_id']], axis=0,join='inner', ignore_index=True)
     
    subject_id_union = pd.unique(subject_id_union)
    
    #all df's must contain a unique subject_id list
    #subject_id
    for i in range(0, len(df_list)):
        #validate subject id datatype, before deletion of empty values
        validate_type("id", df_list[i], exp_data[i][0])
        #first delete all null values from the lists- they are handled later
        df1 = df_list[i]['subject_id'].dropna()
        df1 = list(df1)
        df1 = list(filter(None, df1)) #delete empty values
        df2 = list(pd.unique(df1)) #leaves only unique values but converts to string
        df2 = list(filter(None, df2)) #delete empty values
        df2 = list(map(int, df2)) #convert back to int
        if df1 != df2:
           raise SystemExit(exp_data[i][0] + " doesn't contain a unique subject_id column")  ###logger
                
    #subject_id_number 
    for i in range(0, len(idn_df_list)):
        #first delete all null values from the lists- they are handled later
        df1 = idn_df_list[i]['subject_id_number'].dropna()
        df1 = list(df1)
        df1 = list(filter(None, df1)) #delete empty values
        df2 = list(pd.unique(df1)) #leaves only unique values but converts to string
        df2 = list(filter(None, df2)) #delete empty values
        df2 = list(map(int, df2)) #convert back to int
        if df1 != df2:
           raise SystemExit(str(idn_name_list[i][0]) + " doesn't contain a unique subject_id_number column")  ###logger
           
    try:
        prepare_data(uid,df_list,idn_df_list,subject_id_union,mycursor,mydb)
    except:
        mycursor.close()
        mydb.close()
        raise SystemExit("Problem with prepare_data")