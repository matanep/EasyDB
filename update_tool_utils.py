# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 16:04:21 2019

@author: Matan
"""
def make_df(path): 
        import pandas as pd
        return pd.read_csv(path)

def get_total_rows(table,mycursor):
            sql = "SELECT COUNT(*) FROM "+table+";"
            mycursor.execute(sql)
            total_rows = mycursor.fetchall() 
            total_rows = total_rows[0][0]
            return total_rows
        
def run_sql (sql,mycursor):
    mycursor.execute(sql)
    #mydb.commit()

def only_numerics(seq):
    seq_type= type(seq)
    return seq_type().join(filter(seq_type.isdigit, seq))

def is_space(word):
    for i in word:
        if i==" ":
            return False
    return True

def is_table(tool_name,mycursor):
    mycursor.execute("SHOW TABLES like '"+tool_name+"'")
    result=mycursor.fetchall()
    if result:
        return True
    else:
        return False

def is_sidn_exist(subject_idn,mycursor):
    total_rows = get_total_rows("tb_subjects;")
    subject_idn = str(subject_idn)
    # Get existing id numbers from the DB
    mycursor.execute("SELECT subject_id_number FROM tb_subjects")
    existing_numbers=list(mycursor.fetchall())
    for i in range(0,total_rows):
        existing_numbers[i] = only_numerics(str(existing_numbers[i]))
    if subject_idn in(existing_numbers):
        return True
    else:
        return False
    
def remove_duplicates(tool_name,tbt):
    run_sql("CREATE TABLE "+tool_name+"_temp LIKE "+tool_name+";")
    if  tbt:
        run_sql("INSERT INTO tb_tools_temp SELECT * FROM tb_tools GROUP BY tool_id,tool_name;")
        run_sql("DROP TABLE tb_tools;")
        run_sql("ALTER TABLE tb_tools_temp RENAME TO tb_tools;")
    else:
        run_sql("INSERT INTO "+tool_name+"_temp SELECT * FROM "+tool_name+" GROUP BY subject_id_g,subject_id,experiment_id;")    
        run_sql("DROP TABLE "+tool_name+";")
        run_sql("ALTER TABLE "+tool_name+"_temp RENAME TO "+tool_name+";")
    
def is_exist(var,column,table,mycursor):
#    total_rows = get_total_rows(table)
    # Get existing from the DB
    mycursor.execute("SELECT "+column+" FROM "+table)
    existing_vars=list(mycursor.fetchall())
    for i in range(0,len(existing_vars)):
        existing_vars[i]=existing_vars[i][0]
#    for i in range(0,total_rows):
#        if type(var) != str:
#            existing_vars[i] = only_numerics(str(existing_vars[i]))
    if var in(existing_vars):
        return True
    else:
        return False
    
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

def update_tool(tool_name,
                is_new_exp,
                experiment_name,
                experiment_type,
                experiment_start_date,
                experiment_end_date,
                experiment_owner,
                experiment_owner_mail,
                experiment_id_new_g,uid):
    
    import ezdb_utils as u
    import pandas as pd
    import itertools
    import os   
    import mysql.connector
    import math
    
    # DB schema global variables:
    db_schema = uid
    hostname = '127.0.0.1'
    username = 'root'
    password = 'root'
    
    mydb = mysql.connector.connect(user=username,
                                    password=password,
                                    host=hostname,
                                    database=db_schema)
    mycursor = mydb.cursor()

    #validations section - better move to the driver
    path="testing files/CSV/"
    
    file_list = os.listdir(path) 
    
    ###should be changed to a JSON dictionary:
#    data = [{'name': 'vikash', 'age': 27}, {'name': 'Satyam', 'age': 14}]
#    df = pd.DataFrame.from_dict(data, orient='columns')
    
    ###Modify to a single df validation?
    
    df_list = []
    idn_df_list = []
    idn_name_list = []
    
    for i in range(0, len(file_list)):
        df = make_df(path + file_list[i])
        df_list.append(df)
        #check which df's contain the subect_id_number field, and mark the one with the longest unique list
        if 'subject_id_number' in df.columns:
            idn_df_list.append(df)
            idn_name_list.append(file_list[i])
            
    #preliminary test that all id numbers are of the correct type. Leave only native integers (convert if needed)            
            u.validate_type("idn", df, file_list[i])
    
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
        u.validate_type("id", df_list[i], file_list[i])
        #first delete all null values from the lists- they are handled later
        df1 = df_list[i]['subject_id'].dropna()
        df1 = list(df1)
        df1 = list(filter(None, df1)) #delete empty values
        df2 = list(pd.unique(df1)) #leaves only unique values but converts to string
        df2 = list(filter(None, df2)) #delete empty values
        df2 = list(map(int, df2)) #convert back to int
        if df1 != df2:
           raise SystemExit(file_list[i] + " doesn't contain a unique subject_id column")  ###logger
                
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
           raise SystemExit(str(idn_name_list[i]) + " doesn't contain a unique subject_id_number column")  ###logger
        
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
        rand_numbers = u.generate_rand(total_rows, existing_numbers)
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
                rand = u.generate_single_rand(existing_numbers)
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
    is_real_list = []
    
    # exctract the top number to variable = subject_id_new
    mycursor.execute("SELECT subject_id_g FROM tb_subjects ORDER BY subject_id_g DESC LIMIT 1;")
    subject_id_g_new = mycursor.fetchall() 
    subject_id_g_new = subject_id_g_new[0][0]
        
    # Check if subjects_id_temp already exists, if so, drop it
    mycursor.execute("SHOW TABLES like 'subjects_id_temp'")
    result=mycursor.fetchall()
    if result:
        u.run_sql("DROP TABLE subjects_id_temp")
    
    df = index_df
    rows_len=df.shape[0]
    
    subject_ids = list(df['subject_id'])
    
    # Create the list of subject Id numbers. Search for a column called 'subject_id_number' (it's assumed that it exists)
    subject_id_numbers =  list(df['subject_id_number'])
       
    is_real_col = list(df['is_real'])
    
    # Create new table in the DB called 'subjects_id_temp'
    u.run_sql("CREATE TABLE subjects_id_temp (subject_id INT PRIMARY KEY, subject_id_number INT(11), is_real VARCHAR(255))")
    
    # Insert subject_ids and subject_id_numbers to 'subjects_id_temp'
    new_fields=[]
    for i in range(0,rows_len):
        #we want to add the subject_id_number to subjects_id_temp only if it doesn't already exist there
        new_fields.append((subject_ids[i],subject_id_numbers[i],is_real_col[i]))
            
    sql = "INSERT INTO subjects_id_temp (subject_id, subject_id_number, is_real) VALUES (%s, %s, %s)"
    mycursor.executemany(sql, new_fields)
    
    ###Update tb_subjects
    
    # add subject_id_g column to subjects_id_temp
    u.run_sql("ALTER TABLE "+db_schema+".`subjects_id_temp` ADD COLUMN `subject_id_g` INT(11) DEFAULT NULL FIRST;")
    
    # count rows of subjects_id_temp
    total_rows = u.get_total_rows("subjects_id_temp")
        
    # select table data into python variable
    mycursor.execute("SELECT * FROM "+db_schema+".subjects_id_temp")
    subjects_id_temp_table = mycursor.fetchall()
    
    # the loop extarcts subject_id_number and inserts subject_id_g_new number accordingly
    id_g_counter = subject_id_g_new #highest id_g number in table
    subject_id_g_list = []
    mycursor.execute("SELECT is_real FROM "+db_schema+".subjects_id_temp")
    is_real_list = mycursor.fetchall()
    
    for i in range(0,total_rows):
        
        #if the subject_id_number already exists in tb_subjects: insert existing id_g. else: insert new id_g in ascending order
        current_id_number = u.only_numerics(str(subjects_id_temp_table[i][2]))
        current_id_number_real = str(subjects_id_temp_table[i][3])
        
        if u.is_exist(current_id_number,"subject_id_number","tb_subjects"):
            #add another condition, if the number that exists was randomized, generate a new one, replace the existing randomized one, and keep the real one
            mycursor.execute("SELECT is_real FROM tb_subjects WHERE subject_id_number="+current_id_number+";")
            is_existing_real = mycursor.fetchall()[0][0]
            
            if is_existing_real == "Randomized" and current_id_number_real == "Real":
                mycursor.execute("SELECT subject_id_number FROM tb_subjects")
                existing_numbers=list(mycursor.fetchall())
                mycursor.execute("SELECT subject_id_number FROM subjects_id_temp")
                existing_numbers.append(list(mycursor.fetchall()))
                rand_num = u.generate_single_rand(existing_numbers)
                u.run_sql("UPDATE "+db_schema+".tb_subjects SET subject_id_number="+str(rand_num)+" WHERE subject_id_number ="+str(current_id_number)+";")
                
                id_g_counter = id_g_counter + 1
                subject_id_g_new = id_g_counter
            else:
                mycursor.execute("SELECT subject_id_g FROM "+db_schema+".tb_subjects WHERE subject_id_number="+str(current_id_number))
                subject_id_g_new = mycursor.fetchall()[0][0]
        else:
            id_g_counter = id_g_counter + 1
            subject_id_g_new = id_g_counter    
            
        # set current_id_g with table row number i
        u.run_sql("UPDATE "+db_schema+".subjects_id_temp SET subject_id_g="+u.only_numerics(str(subject_id_g_new))+" WHERE subject_id_number ="+str(current_id_number)+";")
        subject_id_g_list.append(subject_id_g_new)
        
    # update tb_subjects (subject_id_g, subject_id_number) with subjects_id_temp (subject_id_g, subject_id_number) 
    # select table data into python variable
    mycursor.execute("SELECT subject_id_number FROM "+db_schema+".subjects_id_temp")
    subjects_id_temp_table_sidn = list(mycursor.fetchall())
    
    for i in range(0,total_rows):
        current_id_g = subject_id_g_list[i]
        current_id_number = u.only_numerics(str(subjects_id_temp_table_sidn[i]))
        is_real = is_real_list[i][0]
        
        # validate that the number enters tb_subjects only if it's new (including a new experiment)
        if not(u.is_exist(current_id_number,"subject_id_number","tb_subjects")): 
            u.run_sql("INSERT INTO "+db_schema+".`tb_subjects` (`subject_id_g`, `subject_id_number`, `is_real`) VALUES ('"+str(current_id_g)+"', '"+str(current_id_number)+"', '"+str(is_real)+"');")
            #"INSERT INTO tb_subjects (SELECT subject_id_g, subject_id_number FROM subjects_id_temp);" - delete later
    
    ###Update tool
    ###BEGIN

    # Add temporary id_number column to tb_new
    # Original: if (is_new == True and is_id_numbers == False) or (is_new == False)
    u.run_sql("ALTER TABLE "+db_schema+"."+tool_name+" ADD COLUMN `subject_id_number` VARCHAR(45) NULL AFTER `subject_id`;")
        
    # Update id_numbers and id_g in tb_new, get total rows 
    total_rows = u.get_total_rows("subjects_id_temp;")
    
    # select table data into python variable
    mycursor.execute("SELECT * FROM subjects_id_temp")
    subjects_id_temp_table = mycursor.fetchall()
    
    for i in range(0,total_rows):
        # set current_id_number with table row number i
        current_id_g = subjects_id_temp_table[i][0]
        current_id = subjects_id_temp_table[i][1]
        current_id_number = subjects_id_temp_table[i][2]
        
        # add the subject_id_g column values. check if id_g already exists. if so- skip (don't change it). 
        u.run_sql("UPDATE "+tool_name+" SET subject_id_number="+str(current_id_number)+" WHERE subject_id ="+str(current_id)+";")
        u.run_sql("UPDATE "+tool_name+" SET subject_id_g="+str(current_id_g)+" WHERE subject_id ="+str(current_id)+" AND subject_id_g IS NULL;")
            
    # Set new experiment ID
    if is_new_exp:
        mycursor.execute("SELECT experiment_id FROM tb_experiments ORDER BY experiment_id DESC LIMIT 1;")
        experiment_id_new = mycursor.fetchall() 
        experiment_id_new = (experiment_id_new [0][0])+1
        experiment_id_new = str(experiment_id_new)
        
        # Update tb_experiments
        u.run_sql("INSERT INTO "+db_schema+".`tb_experiments` (`experiment_id`, `experiment_name`, `experiment_type`, `experiment_start_date`, `experiment_end_date`, `experiment_owner`, `experiment_owner_mail`) VALUES ("+"'"+experiment_id_new+"'"+", "+"'"+experiment_name+"'"+", "+"'"+experiment_type+"'"+", "+"'"+experiment_start_date+"'"+", "+"'"+experiment_end_date+"'"+", "+"'"+experiment_owner+"'"+", "+"'"+experiment_owner_mail+"'"+");")
    else:
        experiment_id_new=experiment_id_new_g;
        
    #update new_tool with experiment_id
    if is_new_exp:
        if u.is_exist(current_id_g,"subject_id_g",str(tool_name)):
            sql_text = "UPDATE "+tool_name+" SET "+tool_name+".experiment_id="+str(experiment_id_new)+" WHERE experiment_id IS NULL;"
        else:
            sql_text = "UPDATE "+tool_name+" SET "+tool_name+".experiment_id="+str(experiment_id_new)+" WHERE subject_id_g>"+str(subject_id_g_new)+";"
    else:
        new_id_g_list = "("
        for i in range(0,total_rows):        
            new_id_g_list = new_id_g_list + u.only_numerics(str(subjects_id_temp_table[i][0])) + ", "
        
        new_id_g_list = new_id_g_list[:-2]
        new_id_g_list = new_id_g_list + ")"
        sql_text = "UPDATE "+tool_name+" SET "+tool_name+".experiment_id="+str(experiment_id_new)+" WHERE subject_id_g IN "+new_id_g_list+";"
    
    u.run_sql(sql_text)
      
    # Delete id_number column
    u.run_sql("ALTER TABLE "+db_schema+"."+tool_name+" DROP COLUMN `subject_id_number`;")
    
    # delete duplicate entries form tool table
    u.remove_duplicates(tool_name,False)
    #remove_duplicates("tb_tools",True) not sure if required
    
    # Drop temp tables
    mycursor.execute("SHOW TABLES like 'subjects_id_temp'")
    result=mycursor.fetchall()
    if result:
        u.run_sql("DROP TABLE subjects_id_temp")
                
    ###END
    print("DB was successfully updated with "+tool_name+"! Please validate data integrity")
    
    mycursor.close()
    mydb.close()    

#Testing:
tool_name="bfi"
is_new_exp=True
experiment_name="Test"
experiment_type="Type1"
experiment_start_date="Start"
experiment_end_date="End"
experiment_owner="Matan"
experiment_owner_mail="Matan@gmail.com"
experiment_id_new_g="1"
        
update_tool(tool_name,is_new_exp,experiment_name,experiment_type,experiment_start_date,experiment_end_date,experiment_owner,experiment_owner_mail,experiment_id_new_g,'curiosity_db')
















