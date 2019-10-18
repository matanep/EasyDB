# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 18:15:44 2019

@author: Matan
"""

def validate_data(uid,exp_data,mycursor,mydb):
    
    import sys
    sys.path.append(r'C:\Users\Matan\Documents\curiosity DB\scripts\EZDB\functions')
    from ezdb_utils import validate_type
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