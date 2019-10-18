# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 10:26:21 2019

@author: Matan
"""
###exp_data structure:
#    list of tuples: ("dict_name",JSON_dict)

def manage_db(uid,action,exp_data,exp_metadata):
    from validate_data import validate_data
    from update_tool import update_tool
    from insert_tool import insert_tool
    import mysql.connector
    
    mydb = mysql.connector.connect(database = uid,
                                   host = '127.0.0.1',
                                   user = 'root',
                                   password = 'root')
        
    mycursor = mydb.cursor()
    
    #Global parameters
    subject_id_g_new = mycursor.fetchall(
            mycursor.execute("SELECT subject_id_g FROM tb_subjects ORDER BY subject_id_g DESC LIMIT 1;")
            )[0][0] 
    
    try:
        validate_data(uid,exp_data,mycursor,mydb)
    except:
        mycursor.close()
        mydb.close()
        raise SystemExit("Problem with validate_data")
#        
    if action=="update":
        try:
            update_tool(mycursor,
                    uid,
                    exp_metadata["tool_name"],
                    exp_metadata["is_new_exp"],
                    exp_metadata["experiment_name"],
                    exp_metadata["experiment_type"],
                    exp_metadata["experiment_start_date"],
                    exp_metadata["experiment_end_date"],
                    exp_metadata["experiment_owner"],
                    exp_metadata["experiment_owner_mail"],
                    exp_metadata["experiment_id_new_g"],
                    exp_metadata["subject_id_g_new"],
                    subject_id_g_new)
        except:
            mycursor.close()
            mydb.close()
            
        raise SystemExit("Problem with validate_data")
    
    elif action=="insert":
        try:
            insert_tool(mycursor,
                    uid,
                    exp_metadata["tool_name"],
                    exp_metadata["number_of_items"],
                    exp_metadata["tool_description"],
                    exp_metadata["related_table"],
                    exp_metadata["related_info"],
                    exp_metadata["is_new_exp"],
                    exp_metadata["experiment_name"],
                    exp_metadata["experiment_type"],
                    exp_metadata["experiment_start_date"],
                    exp_metadata["experiment_end_date"],
                    exp_metadata["experiment_owner"],
                    exp_metadata["experiment_owner_mail"],
                    exp_metadata["experiment_id_new_g"]) 
        except:
            mycursor.close()
            mydb.close()
    
    mycursor.close()
    mydb.close()
    
    
#Testing:
uid="curiosity_db"
action="insert"    
    
exp_metadata={}
exp_metadata["is_new_exp"]=False
exp_metadata["experiment_name"]="Test"
exp_metadata["experiment_type"]="Type1"
exp_metadata["experiment_start_date"]="Start"
exp_metadata["experiment_end_date"]="End"
exp_metadata["experiment_owner"]="Matan"
exp_metadata["experiment_owner_mail"]="Matan@gmail.com"
exp_metadata["experiment_id_new_g"]="1"

exp_metadata["tool_name"]="aq"
exp_metadata["number_of_items"]="50"
exp_metadata["tool_description"]="desc"
exp_metadata["related_table"]="table"
exp_metadata["related_info"]="info"

###need to add data_dict
manage_db(uid,action,exp_metadata)
    
    