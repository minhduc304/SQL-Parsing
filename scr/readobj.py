from codecs import escape_encode
import re
import contextlib
from numpy import true_divide
import pandas as pd
import copy
from .config import Config


class ParsingSQLOwnersAndObjects:
    """ Parses for the owner and object names from stored
        procedure strings"""
    
    
    def __init__(self):
        self.database_file_path = Config().file_all_objects
        self.stored_proc_file_path = Config().file_sql_source
        self.cols_file_path = Config().file_all_tables_and_cols
        self.database = pd.read_csv(self.database_file_path)
        self.stored_proc = open(self.stored_proc_file_path, 'r')
        self.sql_input = Config().single_sql_stmt
        self.big_database = pd.read_excel(self.cols_file_path, usecols='A:C', engine='openpyxl')

    def unique(self, ls):
        result = list(dict.fromkeys(ls))
        return result

    
    
    def split_sql_string(self, sql_string):
        # separates large string into individual statements,
        # capitalises and removes all white space characters
        sql_string = sql_string.upper()
        sql_string = re.sub("\s+", " ", sql_string)
        ar_string = sql_string.split(";")
        
        return ar_string

    def cross_search_for_name_and_index(self, ls, stmt):
        
        output_list = []
        # for every name in ls, if the statement 'stmt' also contains name, then the start and end
        # index of the name is recorded onto an output list
        
        for name in ls:
            if name in stmt:
                concat = [(name, m.start(), m.end()) for m in re.finditer(name, stmt)]
                output_list += concat
                
        return self.remove_overlap(output_list)

    def remove_overlap(self, obj_found):
        obj_found = sorted(obj_found, key=lambda tup: tup[1])
        # print(obj_found)

        def overlap(tuple1, tuple2, overlapThresh = 1):
            len_overlap =min(tuple1[2], tuple2[2]) - max(tuple1[1], tuple2[1]) 

            len_tup1 = tuple1[2]-tuple1[1]
            len_tup2 = tuple2[2]-tuple2[1]
            min_len = min(len_tup1, len_tup2)

            if len_overlap / min_len == overlapThresh:
                return tuple1 if len_tup1 < len_tup2 else tuple2
            else: return False

        obj_found_copy = copy.deepcopy(obj_found)
        for id1, obj1 in enumerate(obj_found):
            for id2, obj2 in enumerate(obj_found):
                if id1 > id2 and overlap(obj1, obj2):
                    a = overlap(obj1, obj2) 
                    if a in obj_found_copy:

                        obj_found_copy.remove(overlap(obj1, obj2))
    
        return obj_found_copy
            
    
    def query_object_type(self, owner, obj, df):
        #
        query_ = ("OWNER == '{}'  and OBJECT_NAME == '{}'").format(owner, obj)
        return df.query(query_)["OBJECT_TYPE"].tolist()

    
    def find_objects_in_sql_stmt(self):
        
        # reads file and store in variable
        # this txt file with stored procedures are separated with '$$'
        content = self.stored_proc.read()

        content = content.split("$$")
    
        #define lists to store unique owners and objects 
        owner_found = []
        obj_found = []
        
        #turns dataframe objects into lists so they can be traversed and compared to other lists
        owners = self.database["OWNER"].tolist()
        owners = self.unique(owners)
        objects = self.database["OBJECT_NAME"].tolist()
        objects = self.unique(objects)
        
        

        
        for stmt in content :
            # print(stmt)
            
            
            # cleans the statement to match format of strings in database
            stmt = self.split_sql_string(stmt) 
            for st in stmt:
                owner_found = self.cross_search_for_name_and_index(owners, st)
                print(owner_found)
                
                obj_found = self.cross_search_for_name_and_index(objects, st)

            # searches for an owner name in each statement by cross referencing distinct owners list
            
                
                # case dot between owner and object
                # eg: SCHEMA.Table, CARS.Toyota
                for owner in owner_found:
                    for obj in obj_found:
                        if owner[2] == obj[1] - 1:
                            print (self.query_object_type(owner[0], obj[0], self.database))



    def single_sql(self, sql_str):
        owners = self.database["OWNER"].tolist()
        owners = self.unique(owners)
        objects = self.database["OBJECT_NAME"].tolist()
        objects = self.unique(objects)

        res = {}
        stmt = self.split_sql_string(sql_str) 
        count = 0
        for st in stmt:
            owner_found = self.cross_search_for_name_and_index(owners, st)
            obj_found = self.cross_search_for_name_and_index(objects, st)

        # searches for an owner name in each statement by cross referencing distinct owners list
        
            
            # case dot between owner and object
            # eg: SCHEMA.Table, CARS.Toyota
            
            for owner in owner_found:
                for obj in obj_found:
                    if owner[2] == obj[1] - 1:
                        res[count] = (owner, obj, self.query_object_type(owner[0], obj[0], self.database))
                        
                        count += 1
                    
        return res






            
if __name__ == '__main__': # for testing on data source
    a = ParsingSQLOwnersAndObjects()
    a.find_objects_in_sql_stmt()
    a.single_sql()

    



    