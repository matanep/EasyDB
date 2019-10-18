# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 18:30:31 2019

@author: Matan
"""

def update_tool(mycursor,
                tool_name,
                uid,
                is_new_exp,
                experiment_name,
                experiment_type,
                experiment_start_date,
                experiment_end_date,
                experiment_owner,
                experiment_owner_mail,
                experiment_id_new_g,
                subject_id_g_new):
    
    import sys
    sys.path.append(r'C:\Users\Matan\Documents\curiosity DB\scripts\EZDB\functions')
    from ezdb_utils import only_numerics,run_sql,get_total_rows,is_exist,remove_duplicates
        
    # Add temporary id_number column to tb_new
    # Original: if (is_new == True and is_id_numbers == False) or (is_new == False)
    run_sql("ALTER TABLE "+uid+"."+tool_name+" ADD COLUMN `subject_id_number` VARCHAR(45) NULL AFTER `subject_id`;")
        
    # Update id_numbers and id_g in tb_new, get total rows 
    total_rows = get_total_rows("subjects_id_temp;")
    
    # select table data into python variable
    mycursor.execute("SELECT * FROM subjects_id_temp") ###why not use python var?
    subjects_id_temp_table = mycursor.fetchall()
    
    for i in range(0,total_rows):
        # set current_id_number with table row number i
        current_id_g = subjects_id_temp_table[i][0]
        current_id = subjects_id_temp_table[i][1]
        current_id_number = subjects_id_temp_table[i][2]
        
        # add the subject_id_g column values. check if id_g already exists. if so- skip (don't change it). 
        run_sql("UPDATE "+tool_name+" SET subject_id_number="+str(current_id_number)+" WHERE subject_id ="+str(current_id)+";")        
        run_sql("UPDATE "+tool_name+" SET subject_id_g="+str(current_id_g)+" WHERE subject_id="+str(current_id)+" AND subject_id_g IS NULL;")
            
    # Set new experiment ID
    if is_new_exp:
        mycursor.execute("SELECT experiment_id FROM tb_experiments ORDER BY experiment_id DESC LIMIT 1;")
        experiment_id_new = mycursor.fetchall() 
        experiment_id_new = (experiment_id_new [0][0])+1
        experiment_id_new = str(experiment_id_new)
        
        # Update tb_experiments
        run_sql("INSERT INTO "+uid+".`tb_experiments` (`experiment_id`, `experiment_name`, `experiment_type`, `experiment_start_date`, `experiment_end_date`, `experiment_owner`, `experiment_owner_mail`) VALUES ("+"'"+experiment_id_new+"'"+", "+"'"+experiment_name+"'"+", "+"'"+experiment_type+"'"+", "+"'"+experiment_start_date+"'"+", "+"'"+experiment_end_date+"'"+", "+"'"+experiment_owner+"'"+", "+"'"+experiment_owner_mail+"'"+");")
    else:
        experiment_id_new=experiment_id_new_g;
            
    if is_new_exp:
        if is_exist(current_id_g,"subject_id_g",str(tool_name)):
            sql_text = "UPDATE "+tool_name+" SET "+tool_name+".experiment_id="+str(experiment_id_new)+" WHERE experiment_id IS NULL;"
        else:
            sql_text = "UPDATE "+tool_name+" SET "+tool_name+".experiment_id="+str(experiment_id_new)+" WHERE subject_id_g>"+str(subject_id_g_new)+";"
    else:
        new_id_g_list = "("
        for i in range(0,total_rows):        
            new_id_g_list = new_id_g_list + only_numerics(str(subjects_id_temp_table[i][0])) + ", "
        
        new_id_g_list = new_id_g_list[:-2]
        new_id_g_list = new_id_g_list + ")"
        sql_text = "UPDATE "+tool_name+" SET "+tool_name+".experiment_id="+str(experiment_id_new)+" WHERE subject_id_g IN "+new_id_g_list+";"
    
    run_sql(sql_text)
      
    # Delete id_number column
    run_sql("ALTER TABLE "+uid+"."+tool_name+" DROP COLUMN `subject_id_number`;")
    
    # delete duplicate entries form tool table
    remove_duplicates(tool_name,False)
    #remove_duplicates("tb_tools",True) not sure if required
    
    # Drop temp tables
    mycursor.execute("SHOW TABLES like 'subjects_id_temp'")
    result=mycursor.fetchall()
    if result:
        run_sql("DROP TABLE subjects_id_temp")
                
    ###END
    print("DB was successfully updated with "+tool_name+"! Please validate data integrity")