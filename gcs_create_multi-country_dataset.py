# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 13:20:49 2022

@author: nicolas.loose
"""
import pandas as pd #our data science workhorse
from glob import glob #for getting folder contents 
import os #needed in order to set paths independent of operating system (Mac, Windows)

# define folder and file names for input and output
data_folder = (r"YOUR DATA FOLDER GOES HERE")
complete_dataset_filename = 'GCS_INT_2022_u2.csv'

# get list of available datasets
search_pattern = os.path.join(data_folder, '*'+'upload.csv')
datasets = glob(search_pattern)
print (datasets) 

# define a list of columns to be included or skip this step to include all columns

'''columns = [
    'RespondentID',
    'year',
    'wave',
    'split',
    'v0013b_demo_agecat',
    'v0013b_demo_agecat',
    'v0014_demo_gender'
    ]'''

def read_dataset(file, columns = []):
    """
    Parameters
    ----------
    file : STRING
        Filename of the dataset with path.
    columns : LIST, optional
        List of column names to be included in the merged dataset. The default is [].

    Returns
    -------
    df is a pandas dataframe.

    """
    
    print('reading... '+file)

    # skip first row which contains survey info
    if not columns:
        df=pd.read_csv(file, sep=";", header = 0, skiprows=1, low_memory=False)
    else:
        df=pd.read_csv(file, sep=";", header = 0, skiprows=1, usecols=columns, low_memory=False)
    return(df)

#merge all data frames from all countries into a single dataset
def append_cols(datasets, columns=[]):
    
    appended_data = []
    for infile in datasets:
        data = read_dataset(infile, columns)
        
        # store DataFrame in list
        appended_data.append(data)
    
    print('appending dataframes...')
    
    appended_data = pd.concat(appended_data, axis=0)
    print('done')
    
    return(appended_data)


# df=append_cols(datasets, columns)
df=append_cols(datasets)
df.to_csv(os.path.join(data_folder, complete_dataset_filename), sep=";", index=False)
