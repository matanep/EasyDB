# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 18:05:30 2019

@author: Matan
"""
def make_df(path): 
        import pandas as pd
        return pd.read_csv(path)

def only_numerics(seq):
    seq_type= type(seq)
    return seq_type().join(filter(seq_type.isdigit, seq))

def is_space(word):
    for i in word:
        if i==" ":
            return False
    return True

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

def generate_rand(nrows, existing_numbers):
# This function gets less effective as the existing numbers list gets bigger. You should consider changing it to a more efficient one
    import random
    new_ids = []
    for i in range(0,nrows):
        rand_number=random.randrange(100000000,999999999,1)
        is_new=True
        for existing_number in existing_numbers:
            if rand_number==existing_number:
               is_new=False
               while is_new==False:
                    rand_number=random.randrange(100000000,999999999,1)
                    is_new==True
                    for existing_number in existing_numbers:
                        if rand_number==existing_number:
                            is_new=False
        if is_new==True:
            new_ids.append(int(rand_number))
    return new_ids

def generate_single_rand(existing_numbers):
# This function gets less effective as the existing numbers list gets bigger. You should consider changing it to a more efficient one
    import random
    rand_number=random.randrange(100000000,999999999,1)
    is_new=True
    for existing_number in existing_numbers:
        if rand_number==existing_number:
           is_new=False
           while is_new==False:
                rand_number=random.randrange(100000000,999999999,1)
                is_new==True
                for existing_number in existing_numbers:
                    if rand_number==existing_number:
                        is_new=False
    return int(rand_number)

##############################################################################################################
##############################################################################################################
##############################################################################################################

def prepare_data(uid,df_list,idn_df_list,subject_id_union,mycursor,mydb):
    import pandas as pd
    import math
    import itertools
    from update_tb_subjects import update_tb_subjects
    
    def run_sql (sql):
        mycursor.execute(sql)

        
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