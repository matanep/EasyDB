# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 19:48:14 2019

@author: Matan
"""

########################################################################################################################
########################################################################################################################
########################################################################################################################

def insert_tool(mycursor,
                uid,
                tool_name,
                number_of_items,
                tool_description,
                related_table,
                related_info,
                is_new_exp,
                experiment_name,
                experiment_type,
                experiment_start_date,
                experiment_end_date,
                experiment_owner,
                experiment_owner_mail,
                experiment_id_new_g):
    
#    import utilities as u
    
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

    # Update tb_new with experiment_id and subject_id_number
    run_sql("ALTER TABLE "+uid+"."+tool_name+" ADD COLUMN `experiment_id` INT(11) NULL DEFAULT NULL AFTER `subject_id`;")
    run_sql("ALTER TABLE "+uid+"."+tool_name+" ADD COLUMN `subject_id_number` VARCHAR(45) NULL AFTER `subject_id`;")
        
    # Update tb_tools
    mycursor.execute("SELECT tool_id FROM tb_tools ORDER BY tool_id DESC LIMIT 1;")
    tool_id_new = mycursor.fetchall() 
    tool_id_new = str(tool_id_new [0][0]+1)

    if not is_exist(tool_name,"tool_name","tb_tools"):
        run_sql("INSERT INTO "+uid+".`tb_tools` (`tool_id`, `tool_name`, `number_of_items`, `tool_description`, `related_table`, `related_info`) VALUES ("+"'"+tool_id_new+"'"+", "+"'"+tool_name+"'"+", "+"'"+number_of_items+"'"+", "+"'"+tool_description+"'"+", "+"'"+related_table+"'"+", "+"'"+related_info+"'"+");")
    
    # Add id_g column to tb_new
    run_sql("ALTER TABLE "+uid+"."+tool_name+" ADD COLUMN `subject_id_g` INT(11) DEFAULT NULL FIRST;")
        
#    # Add temporary id_number column to tb_new
#    if (is_new == True and is_id_numbers == False) or (is_new == False):
#        run_sql("ALTER TABLE "+uid+"."+tool_name+" ADD COLUMN `subject_id_number` VARCHAR(45) NULL AFTER `subject_id`;")
        
    # Update id_numbers and id_g in tb_new, get total rows 
    total_rows = get_total_rows("subjects_id_temp;")
    
    # select table data into python variable
    mycursor.execute("SELECT * FROM subjects_id_temp")
    subjects_id_temp_table = mycursor.fetchall()
    
    for i in range(0,total_rows):
        # set current_id_number with table row number i
        current_id_g = subjects_id_temp_table[i][0]
        current_id = subjects_id_temp_table[i][1]
        current_id_number = subjects_id_temp_table[i][2]
        
        # add the subject_id_g column values. check if id_g already exists. if so- skip (don't change it). 
        run_sql("UPDATE "+tool_name+" SET subject_id_number="+str(current_id_number)+" WHERE subject_id ="+str(current_id)+";")
        run_sql("UPDATE "+tool_name+" SET subject_id_g="+str(current_id_g)+" WHERE subject_id ="+str(current_id)+" AND subject_id_g IS NULL;")
            
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
        
    #update new_tool with experiment_id
    sql_text = "UPDATE "+tool_name+" SET "+tool_name+".experiment_id="+str(experiment_id_new)+";"    
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