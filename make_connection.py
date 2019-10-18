# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 18:09:17 2019

@author: Matan
"""

class make_connection():        
    
    def __init__(self,db_schema,hostname,username,password,uid):
        import mysql.connector        
        self.mydb = mysql.connector.connect(self.db_schema = uid,
                                        self.hostname = '127.0.0.1',
                                         self.username = 'root',
                                        self.password = 'root')
        return self

    def close(self):
        self.mydb.close()
