# -*- coding: utf-8 -*-
"""
Created on Tue May 31 22:46:13 2022

@author: Shenghan Zhang
@e-mail: jhan_workspace@163.com
"""
import os
from functools import wraps
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

def write_file(file_target):
    def decorate(a_func):
        @wraps(a_func)
        def wrap_func(*args,**kwargs):
            a_func(*args,**kwargs)
            print("file written successfully!")
            print("stored file type is {}".format(file_target))
        return wrap_func
    return decorate
    
@write_file(file_target='.txt')
def atomic_writeTXT(path,content):
    temp_path='.\\temp.txt'
    f = open(temp_path,mode='w')
    f.write(content)
    f.flush()
    os.fsync(f.fileno())
    f.close()
    try:
        os.remove(path)
    except OSError as reason:
        pass
    finally:
        os.rename(temp_path,path)
    return

@write_file(file_target='.parq')        
def atomic_writeParquet(path,df):
    """
    df format must follow pandas.dataframe
    using temp file as atomic writing method
    """
    temp_path = '.\\temp.parq'
    table = pa.Table.from_pandas(df)
    pq.write_table(table,temp_path)
    try:
        os.remove(path)
    except OSError as reason:
        pass
    finally:
        os.rename(temp_path,path)
    return
        
    


if __name__ == '__main__':
    # ------- test atomic writing for txt file
    path = ".\\test.txt"
    content = "Hi I'm James!!! Nice to meet you!"
    atomic_writeTXT(path,content)
    
    # ------- test atomic writing for parquet file
    path = ".\\test.parq"
    ids = [1,2,3,4]
    list_columns=['a','b','c']
    list_content=[[11,22,33],[44,55,66],[77,88,99],[111,222,333]]
    df=pd.DataFrame(columns=list_columns,index=ids,data = list_content)
    print(df)
    atomic_writeParquet(path,df)
    table2=pq.read_table(path)
    df=table2.to_pandas()
    print(df)

