"""
functions for listing files in a directory and converting them to a pandas DF
"""


def get_list_csv_files_from_path(files_path: str):
    """
    returns a list of dict of .csv files in the 'files_path' directory
    :param
        files_path:
            string containing path to the directory of the file
    :return:
        list of dict containing:
             {
                'file_name': name of the file (without extension)
                'path': path to the file
             }
    """
    from os import listdir

    file_list = []
    file_extension = '.csv'
    for file in listdir("files"):
        if file.endswith(file_extension):
            file_list.append(
                {
                    'file_name': file.removesuffix(file_extension),
                    'path': f'{files_path}/{file}'
                }
            )

    return file_list


def get_df_from_csv(file_list: list):
    """
    converts csv files and returns them as list containing Pandas DataFrames
    :param
        file_list:
            list of dict containing following keys:
            {
                filename': name the user gives to .csv sheet to load
                'path': directory and filename of the .csv file
            }
    :return:
        list of dict containing:
            {
                'df_name': name of the table
                'date_load': datetime of when the files where loaded
                'dataframe': pandas DataFrame object containing the table data
            }
    """
    from pandas import DataFrame, read_csv
    from datetime import datetime

    file_data = []
    df_load_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%p')

    for file in file_list:
        file_data.append(
            {
                'df_name': file.get('filename'),
                'date_load': df_load_datetime,
                'dataframe': read_csv(file.get('path'), delimiter=';')
            }
        )

    return file_data
