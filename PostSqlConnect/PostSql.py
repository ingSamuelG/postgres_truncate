import psycopg2
import re
from psycopg2.sql import Identifier, SQL
from datetime import datetime

def delete_null_char(value):
    if isinstance(value, str) and re.search('\x00',value):
        return value.replace("\x00", "\uFFFD")
    else:  
        return value

class PostSqlConnect:
    
    def __init__(self, host, user, password, database,encoding) -> None:
        self.host = host
        self.user = user
        self.db_name= database
        self.password = password
        # self.connection = psycopg2.connect(("dbname='{}' user={} password={}").format(database, user, password))
        self.connection = psycopg2.connect(host= self.host, dbname = self.db_name, user = self.user, password= self.password)
        self.cursor = self.connection.cursor()
        self.encoding = encoding
        self.cursor.execute("set client_encoding = " + encoding)
    
    def drop_existing_table(self, table_name): 
        drop_stament='drop table if EXISTS %s;'%(table_name)
        self.cursor.execute(drop_stament); 
        self.connection.commit()

    def insert_from_old(self, rows,table_name): 
        count_stament = "SELECT column_name FROM information_schema.columns WHERE table_name='{}'".format(table_name)
        self.cursor.execute(count_stament)
        columns = self.cursor.fetchall()
        
        if table_name == "nt_users":
            i = 0
            modified_users = []
            for user in rows:
                user_list = list(user)
                user_list[1] = "name_{}".format(i)
                user_list[2] = "name_{}@gmail.com".format(i)
                user_list[9] = '$2a$11$QTY3UOdJ78QZw/BAMpZGLuasBYlxsyKL0e8kaqYNWmQch08MkCgOm'
                modified_users.append(user_list)
                i+=1
            rows = modified_users

        if rows:
            try:
                args_str = ','.join(self.cursor.mogrify("({})".format(",".join(["%s" for col in columns])),x).decode('utf-8') for x in rows)
            except ValueError:
                formated_rows = []
                for r in rows:
                    formated_rows.append([delete_null_char(n) for n in r])
                args_str = ','.join(self.cursor.mogrify("({})".format(",".join(["%s" for col in columns])),x).decode('utf-8') for x in formated_rows)
            try:
                insert_stament = 'insert into {} values {};'.format(table_name,args_str)
                self.cursor.execute(insert_stament)
            except Exception as e:
                print(e)

    def get_base_rows(self, table_name,limit): 
        select_stament = "select * from {} limit {} offset 0".format(table_name, limit)
        self.cursor.execute(select_stament)
        return self.cursor.fetchall()


    def insert_childs_dependencies_rows_for_temps_v2(self, source_table, ids_string): 
        count_stament = "SELECT column_name FROM information_schema.columns WHERE table_name='{}'".format(source_table)
        self.cursor.execute(count_stament)
        columns = self.cursor.fetchall()
        
        column_string = ",".join(["%s" for col in columns])

        select_stament = "select * from {} where {}".format(source_table, ids_string)
        self.cursor.execute(select_stament)
        source_rows = self.cursor.fetchall()

        if source_rows:
            try:
                args_str = ','.join(self.cursor.mogrify("({})".format(column_string),x).decode('utf-8') for x in source_rows)
            except ValueError:
                formated_rows = []
                for r in source_rows:
                    formated_rows.append([delete_null_char(n) for n in r])
                args_str = ','.join(self.cursor.mogrify("({})".format(column_string),x).decode('utf-8') for x in formated_rows)
            try:
                insert_stament = 'insert into nt_{} values {};'.format(source_table,args_str)
                self.cursor.execute(insert_stament)
            except Exception as e:
                print(e)

    def get_child_tables(self, index_id): # its used
        child_stament = '''
                        SELECT table_name 
                        FROM information_schema.columns 
                        WHERE column_name='{}';
                        '''.format(index_id)
        
        self.cursor.execute(child_stament)
        return [x[0] for x in self.cursor.fetchall()]
    
    def delete_not_in_the_parent(self, child_table_name, ids_string): 
        delete_stament = '''DELETE FROM nt_{} WHERE {};'''.format(child_table_name, ids_string)
        self.cursor.execute(delete_stament)


    def get_table_names(self): 
        tables_stament = '''SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema='public'
                            AND table_type='BASE TABLE'
                            '''

        self.cursor.execute(tables_stament)
        return self.cursor.fetchall()

    def commit_all_transactions(self): 
        try:
            self.connection.commit()
        except Exception as e:
            print (e)
            self.connection.rollback()

    def copy_tables_from_main(self,table_name): 
        copy_stament = '''create table nt_{} (like {} INCLUDING INDEXES);'''.format(table_name,table_name)
        self.cursor.execute(copy_stament); 
    
    def close_conections(self): 
        self.cursor.close()
        self.connection.close()
