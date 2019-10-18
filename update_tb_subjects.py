# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 18:23:40 2019

@author: Matan
"""
def only_numerics(seq):
    seq_type= type(seq)
    return seq_type().join(filter(seq_type.isdigit, seq))

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
        
def update_tb_subjects(uid,subject_id_g_new,mycursor,mydb):    
    def run_sql (sql):
        mycursor.execute(sql)
    
    def get_total_rows(table):
            sql = "SELECT COUNT(*) FROM "+table+";"
            mycursor.execute(sql)
            total_rows = mycursor.fetchall() 
            total_rows = total_rows[0][0]
            return total_rows
        
    def is_exist(var,column,table):
    # Get existing from the DB
        mycursor.execute("SELECT "+column+" FROM "+table)
        existing_vars=list(mycursor.fetchall())
        for i in range(0,len(existing_vars)):
            existing_vars[i]=existing_vars[i][0]
        if var in(existing_vars):
            return True
        else:
            return False
    
    # add subject_id_g column to subjects_id_temp
    run_sql("ALTER TABLE "+uid+".`subjects_id_temp` ADD COLUMN `subject_id_g` INT(11) DEFAULT NULL FIRST;")
    
    # count rows of subjects_id_temp
    total_rows = get_total_rows("subjects_id_temp")
        
    # select table data into python variable
    mycursor.execute("SELECT * FROM "+uid+".subjects_id_temp")
    subjects_id_temp_table = mycursor.fetchall()
    
    # the loop extarcts subject_id_number and inserts subject_id_g_new number accordingly
    id_g_counter = subject_id_g_new #highest id_g number in table
    subject_id_g_list = []
    is_real_list = []
    
    mycursor.execute("SELECT is_real FROM "+uid+".subjects_id_temp")
    is_real_list = mycursor.fetchall()
    
    for i in range(0,total_rows):
        
        #if the subject_id_number already exists in tb_subjects: insert existing id_g. else: insert new id_g in ascending order
        current_id_number = only_numerics(str(subjects_id_temp_table[i][2]))
        current_id_number_real = str(subjects_id_temp_table[i][3])
        
        if is_exist(current_id_number,"subject_id_number","tb_subjects"):
            #add another condition, if the number that exists was randomized, generate a new one, replace the existing randomized one, and keep the real one
            mycursor.execute("SELECT is_real FROM tb_subjects WHERE subject_id_number="+current_id_number+";")
            is_existing_real = mycursor.fetchall()[0][0]
            
            if is_existing_real == "Randomized" and current_id_number_real == "Real":
                mycursor.execute("SELECT subject_id_number FROM tb_subjects")
                existing_numbers=list(mycursor.fetchall())
                mycursor.execute("SELECT subject_id_number FROM subjects_id_temp")
                existing_numbers.append(list(mycursor.fetchall()))
                rand_num = generate_single_rand(existing_numbers)
                run_sql("UPDATE "+uid+".tb_subjects SET subject_id_number="+str(rand_num)+" WHERE subject_id_number ="+str(current_id_number)+";")
                
                id_g_counter = id_g_counter + 1
                subject_id_g_new = id_g_counter
            else:
                mycursor.execute("SELECT subject_id_g FROM "+uid+".tb_subjects WHERE subject_id_number="+str(current_id_number))
                subject_id_g_new = mycursor.fetchall()[0][0]
        else:
            id_g_counter = id_g_counter + 1
            subject_id_g_new = id_g_counter    
            
        # set current_id_g with table row number i
        run_sql("UPDATE "+uid+".subjects_id_temp SET subject_id_g="+only_numerics(str(subject_id_g_new))+" WHERE subject_id_number ="+str(current_id_number)+";")
        subject_id_g_list.append(subject_id_g_new)
        
    # update tb_subjects (subject_id_g, subject_id_number) with subjects_id_temp (subject_id_g, subject_id_number) 
    # select table data into python variable
    mycursor.execute("SELECT subject_id_number FROM "+uid+".subjects_id_temp")
    subjects_id_temp_table_sidn = list(mycursor.fetchall())
    
    for i in range(0,total_rows):
        current_id_g = subject_id_g_list[i]
        current_id_number = only_numerics(str(subjects_id_temp_table_sidn[i]))
        is_real = is_real_list[i][0]
        
        # validate that the number enters tb_subjects only if it's new (including a new experiment)
        if not(is_exist(current_id_number,"subject_id_number","tb_subjects")): 
            run_sql("INSERT INTO "+uid+".`tb_subjects` (`subject_id_g`, `subject_id_number`, `is_real`) VALUES ('"+str(current_id_g)+"', '"+str(current_id_number)+"', '"+str(is_real)+"');")
            #"INSERT INTO tb_subjects (SELECT subject_id_g, subject_id_number FROM subjects_id_temp);" - delete later            