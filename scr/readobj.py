import re
import contextlib
import pandas as pd


class ParsingSQLOwnersAndObjects:
    """ Parses for the owner and object names from stored
        procedure strings"""
    
    
    def __init__(self, database_file_path, stored_proc_file_path):
        self.database = pd.read_csv(database_file_path)
        self.stored_proc = open(stored_proc_file_path, 'r')
    

    def close_file(self):
        if self.stored_proc:
            self.stored_proc.close()
            self.stored_proc = None
        
    
    def unique(self, ls):
    
        # initialize a null list
        unique_list = []

        # traverse for all elements
        for x in ls:
            # check if exists in unique_list or not
            if x not in unique_list:
                unique_list.append(x)

        return unique_list
    
    
    def clean_sql_string(self, string):
        # separates large string into individual statements,
        # capitalises and removes all white space characters
        string = string.upper()
        string = re.sub("\s+", " ", string)
        string = string.split(";")
        
        return string

    def cross_search_for_name_and_index(self, ls, stmt):
        
        output_list = []
        # for every name in ls, if the statement 'stmt' also contains name, then the start and end
        # index of the name is recorded onto an output list
        
        for name in ls:
            if name in stmt:
                start = stmt.find(name)
                end = start + len(name)
                output_list.append((name, start, end))
                
        return output_list
    
    def query_object_type(self, owner, obj, df):
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
            # cleans the statement to match format of strings in database
            stmt = self.clean_sql_string(stmt)

        for sql in content:
            # searches for an owner name in each statement by cross referencing distinct owners list
            owner_found = self.cross_search_for_name_and_index(owners, sql)
            obj_found = self.cross_search_for_name_and_index(objects, sql)
            
           
        # case dot between owner and object
        # eg: SCHEMA.Table, CARS.Toyota
        for owner in owner_found:
            for obj in obj_found:
                if owner[2] == obj[1] - 1:
                    print(owner, obj)
                    print(self.query_object_type(owner[0], obj[0], self.database))

        
        self.close_file()
        
        


            
if __name__ == '__main__': # for testing on data source
    a = ParsingSQLOwnersAndObjects("datascr/all_objects.csv", "datascr/new 2.txt")
    a.find_objects_in_sql_stmt()