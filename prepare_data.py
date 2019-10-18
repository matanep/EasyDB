# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 18:05:30 2019

@author: Matan
"""

def prepare_data(uid,df_list,idn_df_list,subject_id_union,mycursor,mydb):
    
    import sys
    sys.path.append(r'C:\Users\Matan\Documents\curiosity DB\scripts\EZDB\functions')
    from ezdb_utils import generate_rand,generate_single_rand,run_sql
    import pandas as pd
    import math
    import itertools
    from update_tb_subjects import update_tb_subjects
        
    #randomization process    
    if len(idn_df_list) == 0:
        print("No CSV contains a subject_id_number column, therefore random numbers where created")
        #add random numbers if needed
        mycursor.execute("SELECT subject_id_number FROM tb_subjects")
        existing_numbers=list(mycursor.fetchall())
        total_rows = subject_id_union.shape[0]
        # (1) from each df that contains sid, extract subject_id and concat the df list
        sidn_concat = df_list[0]['subject_id']
        for i in range(1, len(df_list)):
            sidn_concat = pd.concat([sidn_concat, df_list[i]['subject_id']], axis=0,join='inner', ignore_index=True)
        # (2) leave only unique cases
        sidn_concat = list(pd.unique(list(sidn_concat)))
        # generate random sidn
        rand_numbers = generate_rand(total_rows, existing_numbers)
        # make a column that indicates the numbers are randomized and not real
        is_real = []
        for i in range (0,total_rows):
            is_real.append("Randomized")
        # make a tuple list and turn list to data frame
        sidn_concat = itertools.zip_longest(sidn_concat,rand_numbers,is_real)
        sidn_concat = pd.DataFrame.from_records(sidn_concat, columns=['subject_id','subject_id_number','is_real'])
    else:
        total_rows = subject_id_union.shape[0]
        # (1) from each df that contains sidn, extract subject_id and subject_id_number together 
        # and concat the df list
        sidn_concat = idn_df_list[0][['subject_id','subject_id_number']]
        for i in range(1, len(idn_df_list)):
            sidn_concat = pd.concat([sidn_concat, idn_df_list[i]['subject_id','subject_id_number']], axis=0,join='inner', ignore_index=True)    
    
        
        specific_rands = []
        total_rows = sidn_concat.shape[0]
        
        for i in range(0,total_rows):
            sidn = list(sidn_concat['subject_id_number'])[i] 
            
            # in case the experimenter is missing some id numbers, generate unique random ones
            if math.isnan(sidn):
                mycursor.execute("SELECT subject_id_number FROM tb_subjects")
                existing_numbers=list(mycursor.fetchall())
                existing_numbers.append(list(sidn_concat['subject_id_number']))
                rand = generate_single_rand(existing_numbers)
                sidn_concat.loc[i,'subject_id_number'] = rand
                specific_rands.append(i)
        
        # make a column that indicates the numbers are randomized and not real
        is_real = []
        for i in range (0,total_rows):
            if i in specific_rands:
                is_real.append("Randomized")
            else:
                is_real.append("Real")
                  
    # make a tuple list
    sidn_concat = itertools.zip_longest(sidn_concat['subject_id'].values,sidn_concat['subject_id_number'].values,is_real)
    # (2) leave only unique values and turn list to data frame
    sidn_concat = list(pd.unique(list(sidn_concat)))
    sidn_concat = pd.DataFrame.from_records(sidn_concat, columns=['subject_id','subject_id_number','is_real'])
    
    index_df = sidn_concat
    
    ###Insert data into the DB
    subject_ids=[]
    subject_id_numbers=[]
    
    # exctract the top number to variable = subject_id_new
    mycursor.execute("SELECT subject_id_g FROM tb_subjects ORDER BY subject_id_g DESC LIMIT 1;")
    subject_id_g_new = mycursor.fetchall() 
    subject_id_g_new = subject_id_g_new[0][0]
        
    # Check if subjects_id_temp already exists, if so, drop it
    mycursor.execute("SHOW TABLES like 'subjects_id_temp'")
    result=mycursor.fetchall()
    if result:
        run_sql("DROP TABLE subjects_id_temp")
    
    df = index_df
    rows_len=df.shape[0]
    
    subject_ids = list(df['subject_id'])
    
    # Create the list of subject Id numbers. Search for a column called 'subject_id_number' (it's assumed that it exists)
    subject_id_numbers =  list(df['subject_id_number'])
       
    is_real_col = list(df['is_real'])
    
    # Create new table in the DB called 'subjects_id_temp'
    ###WHy create a table and not use a python variable???
    run_sql("CREATE TABLE subjects_id_temp (subject_id INT PRIMARY KEY, subject_id_number INT(11), is_real VARCHAR(255))")
    
    # Insert subject_ids and subject_id_numbers to 'subjects_id_temp'
    new_fields=[]
    for i in range(0,rows_len):
        #we want to add the subject_id_number to subjects_id_temp only if it doesn't already exist there
        new_fields.append((subject_ids[i],subject_id_numbers[i],is_real_col[i]))
            
    sql = "INSERT INTO subjects_id_temp (subject_id, subject_id_number, is_real) VALUES (%s, %s, %s)"
    mycursor.executemany(sql, new_fields)
    
    try:
        update_tb_subjects(uid,subject_id_g_new,mycursor,mydb)
    except:
        mycursor.close()
        mydb.close()
        raise SystemExit("Problem with update_tb_subjects")