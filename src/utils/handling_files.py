#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Working With Files in Python
Source: https://realpython.com/working-with-files-in-python/#:~:text=To%20get%20a%20list%20of,scandir()%20in%20Python%203.
"""


__all__ = ["read_csv", "read_xls", "get_list_of_files"]


import os
import pandas as pd


# --------------------------------------------------
def read_csv(PATH:str, names=None, header=None) -> pd.DataFrame:
    """"Read from csv report"""
    df = pd.read_csv(PATH, index_col=None, header=header, 
    na_filter = False, dtype=str, encoding = 'ISO-8859-1',
    on_bad_lines='warn', names=names)
    n_col = df.shape[1]
    df.columns = [str(x) for x in range(n_col)]
    return df

# --------------------------------------------------
def read_xls(PATH:str, header:int = None) -> pd.DataFrame:
    """"Read from xls report"""
    df = pd.read_excel(PATH, index_col=None, header=header, 
    na_filter = False, dtype=str)
    if header is None:
        n_col = df.shape[1]
        df.columns = [str(x) for x in range(n_col)]
    return df

# --------------------------------------------------
# def read_pdf(self, PATH:str, names=None, header=None) -> pd.DataFrame:
#     """"Read from pdf report"""
#     tables = tabula.read_pdf(
#         PATH, pages='all', multiple_tables=False,
#         pandas_options={
#             'index_col':None, 'header':header,'na_filter':False, 
#             'dtype':str, 'on_bad_lines':'warn', 'names':names
#         }
#     )
#     df = pd.DataFrame()
#     # n_col = df.shape[1]
#     # df.columns = [str(x) for x in range(n_col)]
#     # return df
#     for i in range(len(tables)):
#         table = tables[i]
#         n_col = table.shape[1]
#         table.columns = [str(x) for x in range(n_col)]
#         df=pd.concat([df, table],)
#     return df

# --------------------------------------------------
def get_list_of_files(path:str, years: list[str] = None) -> list:
    """Get list of files in a folder
    :param path: folder or file.
    :param years: list of years to filter the files.
    """
    # Check if path is a file and returns it. 
    # Otherwise returns list of files in folder.
    file_list = []
    if os.path.isfile(path):
        file_list.append(path)
    else:
        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            if os.path.isfile(full_path) and (
                years is None or os.path.basename(full_path)[:4] in years
            ):
                file_list.append(full_path)
    print("File list to update:")
    print(file_list)
    return file_list