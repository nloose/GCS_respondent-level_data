# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 13:20:49 2022

This script reads all gcs data files from a single folder and concatenates them into a single dataset.
If you define a list of columns, only these will be included, which helps reduce file size significantly.

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


# read the dataset info from the first line of an upload / respondent-level dataset
def read_dataset_info(dataset):
    """
    Returns the info on a GCS upload dataset as dictionary.

    Parameters
    ----------
    dataset : STR
        file with full context (folder path).

    Returns
    -------
    dataset_info : DICT
        Dictionary containing the following fields:
            
            "file_id": internal ID
            "description":  HTML dataset description
            "title": dataset / study title, e.g. 'Global Consumer Survey - Indonesia'
            "country": country ISO code, e.g. 'IDN'
            "year" : 4-digit year, e.g. 2021
            "update": single digit update number, 0 if only one update per year
            "questionnaire version": either basic or extended, the latter contains ionfo on brands

    """
    with open(
        dataset, "r", encoding="utf8"
    ) as file:  # read the first line of the dataset file
        firstline = file.readline()
        # convert to list and filter on None to get rid of empty list elements
        firstline = list(filter(None, firstline.split(sep=";")))
        if int(firstline[5]) > 0:
            questionnaire_version = 'basic questionnaire'
        else:
            questionnaire_version = 'extended questionnaire'

        dataset_info = {
            "file_id": firstline[0],
            "description": firstline[1],
            "title": firstline[2],
            "country": firstline[3],
            "year": firstline[4],
            "update": firstline[5],
            "questionnaire version": questionnaire_version
        }

        return dataset_info


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
        dataset_info = read_dataset_info(infile)
        data = read_dataset(infile, columns)
        data.insert(1, 'questionnaire_version', dataset_info['questionnaire version'], True)
        data.insert(1, 'country_iso', dataset_info['country'], True)
        
        # store DataFrame in list
        appended_data.append(data)
    
    print('appending dataframes...')
    
    appended_data = pd.concat(appended_data, axis=0)
    print('done')
    
    return(appended_data)


#df=append_cols(datasets, columns) # use this version if you defined a set of columns
df=append_cols(datasets)
print('saving file...')
df.to_csv(os.path.join(data_folder, complete_dataset_filename), sep=";", index=False)
print('all done')
