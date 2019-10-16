# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 16:11:21 2019

@author: Matan
"""

class utilities:
    
    def __init__(self, connector):
        self.connector = connector
    
    def get_total_rows(self,table,mycursor):
            sql = "SELECT COUNT(*) FROM "+table+";"
            mycursor.execute(sql)
            total_rows = mycursor.fetchall() 
            total_rows = total_rows[0][0]
            return total_rows
        
    def run_sql (self,sql,mycursor):
        mycursor.execute(sql)
        #mydb.commit()
    
    def only_numerics(self,seq):
        seq_type= type(seq)
        return seq_type().join(filter(seq_type.isdigit, seq))
    
    def is_space(self,word):
        for i in word:
            if i==" ":
                return False
        return True
    
    def is_table(self,tool_name,mycursor):
        mycursor.execute("SHOW TABLES like '"+tool_name+"'")
        result=mycursor.fetchall()
        if result:
            return True
        else:
            return False
    
    def is_sidn_exist(self,subject_idn,mycursor):
        total_rows = self.get_total_rows("tb_subjects;")
        subject_idn = str(subject_idn)
        # Get existing id numbers from the DB
        mycursor.execute("SELECT subject_id_number FROM tb_subjects")
        existing_numbers=list(mycursor.fetchall())
        for i in range(0,total_rows):
            existing_numbers[i] = self.only_numerics(str(existing_numbers[i]))
        if subject_idn in(existing_numbers):
            return True
        else:
            return False
        
    def remove_duplicates(self,tool_name,tbt):
        self.run_sql("CREATE TABLE "+tool_name+"_temp LIKE "+tool_name+";")
        if  tbt:
            self.run_sql("INSERT INTO tb_tools_temp SELECT * FROM tb_tools GROUP BY tool_id,tool_name;")
            self.run_sql("DROP TABLE tb_tools;")
            self.run_sql("ALTER TABLE tb_tools_temp RENAME TO tb_tools;")
        else:
            self.run_sql("INSERT INTO "+tool_name+"_temp SELECT * FROM "+tool_name+" GROUP BY subject_id_g,subject_id,experiment_id;")    
            self.run_sql("DROP TABLE "+tool_name+";")
            self.run_sql("ALTER TABLE "+tool_name+"_temp RENAME TO "+tool_name+";")
        
    def is_exist(self,var,column,table,mycursor):
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
        
    def make_df(self,path): 
        import pandas as pd
        return pd.read_csv(path)

    def correct_idn(self,idn):
        import regex as re
        return re.match(r'^([0-9]{9})+$', str(idn))
    
    def correct_id(self,id):
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
    
    def validate_type(self,id_or_idn, df, file_name):
        import math
            
        if id_or_idn == "id":
            l = df['subject_id']
        else:
            l = df['subject_id_number']
    
        for j in range(0, len(l)):
            
            idn = l[j]
            
            if (type(idn) == str) and not(idn==""):
                if id_or_idn == "id":
                    if not self.correct_id(idn):
                        raise SystemExit("Not all subject ids in " + str(file_name) + " are correct: " + str(idn)) ###logger
                elif not self.correct_idn(idn):
                        raise SystemExit("Not all subject id numbers in " + str(file_name) + " are correct: " + str(idn)) ###logger
                else: 
                    l[j] = int(l[j])
            
            elif (type(idn).__module__ == 'numpy') and not(math.isnan(idn)):
                idn = int(idn.item())
                if id_or_idn == "id":
                    if not self.correct_id(idn):
                        raise SystemExit("Not all subject ids in " + str(file_name) + " are correct: " + str(idn)) ###logger
                elif not self.correct_idn(idn):
                    raise SystemExit("Not all subject id numbers in " + str(file_name) + " are correct: " + str(idn)) ###logger
                else: 
                    l[j] = int(l[j])
                    
            elif type(idn) == float and not(idn==None):
                if id_or_idn == "id":
                    if not self.correct_id(idn):
                        raise SystemExit("Not all subject ids in " + str(file_name) + " are correct: " + str(idn)) ###logger
                elif not self.correct_idn(idn):
                    raise SystemExit("Not all subject id numbers in " + str(file_name) + " are correct: " + str(idn)) ###logger
                else: 
                    l[j] = int(l[j])
                    
            elif type(idn) == int and not(idn==None):
                if id_or_idn == "id":
                    if not self.correct_id(idn):
                        raise SystemExit("Not all subject ids in " + str(file_name) + " are correct: " + str(idn)) ###logger
                elif not self.correct_idn(idn):
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
    
    def generate_rand(self,nrows, existing_numbers):
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

    def generate_single_rand(self,existing_numbers):
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
    
    def csv_to_xls(self,path,file_name):
        from pyexcel.cookbook import merge_all_to_a_book
        # import pyexcel.ext.xlsx # no longer required if you use pyexcel >= 0.2.2
        import glob2 as glob
        path = path + "\\"
        merge_all_to_a_book(glob.glob(path+file_name+".csv"),\
        path+file_name+".xlsx")
