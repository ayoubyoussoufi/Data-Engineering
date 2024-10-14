import os
import math
import re
import yaml
import pandas as pd


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def list_files_in_raw():
    files = os.listdir('./raw')
    return files

def get_tabs_excel(path):
    tabs = pd.read_excel(f'./raw/{path}', None).keys()
    return tabs

def read_file(path, tab_name):
    df_raw = pd.read_excel(f'./raw/{path}', tab_name)
    return df_raw

def get_config():
    with open("config.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    return config

def exclude_df_tab(config, tab):
    if tab in config['tabs_to_exclude']:
        return True
    
    elif config['tabs_keyword_to_exclude']: 
        for tab_keyword_to_exclude in config['tabs_keyword_to_exclude']: 
            if tab_keyword_to_exclude in tab: 
                return True
    else:
        return False

def skip_line_to_headers(df):
    try:
        for i, row in df.iterrows():
            if row.notnull().all():
                data = df.iloc[(i+1):].reset_index(drop=True)
                colnames_without_years = list(df.iloc[i])
                rename_dict = {v:k for k, v in config["headers_renaming"].items()}
                colnames_without_years_renamed = [rename_dict.get(colname, colname) for colname in colnames_without_years]
                years_on_previous_row = df.iloc[(i-1):(i)].values.flatten().tolist()

                if len(colnames_without_years_renamed) == len(years_on_previous_row):
                    for i in range(1, len(years_on_previous_row)): 
                        if pd.isna(years_on_previous_row[i]):
                            years_on_previous_row[i] = years_on_previous_row[i-1]
                        else: 
                            pass

                        full_colnames = []
                        assert len(colnames_without_years_renamed) == len(years_on_previous_row)
                        for k in range(0, len(colnames_without_years_renamed)):
                            if pd.isna(years_on_previous_row[k]): 
                                full_colnames.append(f'{colnames_without_years_renamed[k]}')
                            else:
                                full_colnames.append(f'{colnames_without_years_renamed[k]}_{years_on_previous_row[k]}')
                    print(full_colnames)
                else: 
                    raise Exception(f'{bcolors.FAIL}Wrong format for df, please contact a Data Engineer!!!!')
                data.columns = list(full_colnames)
                break
        return data
    except Exception: 
        print(f'{bcolors.FAIL}!!!!!!')
        print(f'{bcolors.FAIL}!!!Skipping lines returned empty data frame, please check tab for file previously mentionned!!!')
        print(f'{bcolors.FAIL}!!!!!!')

def check_if_sample_is_unit_size_dea(df, colname, values): 
    index_to_split_at_raw = [df[df[colname] == value].index for value in values]
    index_to_split_at = index_to_split_at_raw

def find_indexes_to_split_at(df, colname, values):
    ## This function would not work if values are matched several time in df
    indexes_to_split_at = []
    for value in values : 
        index_to_split_at = df[df[colname] == value].index
        if len(index_to_split_at) == 0:
            continue
        indexes_to_split_at.append(index_to_split_at[0])
    
    indexes_to_split_at.sort()
    return indexes_to_split_at

def cut_df_into_unit_size_dfs(df, indexes):
    df_samples = list()
    previous_index = 0
    for index in indexes:
        df_sample = df[previous_index:index].reset_index(drop=True)
        df_samples.append(df_sample)
        previous_index = index
    df_samples.append(df[index:].reset_index(drop=True))
    return df_samples

def clean_df_sample(df, colname):
    month = df[colname][0]
    df = df.melt(id_vars=colname, var_name='year_raw').sort_values(colname)
    df = df[df['year_raw'].str.contains('quantity|value') == True]
    df = df[df[colname].str.contains('^[0-9]+ - ') == True]
    df.dropna(subset = ['value'], inplace=True)
    if not df.empty:
        df["month"] = month
        df["brand"] = tab
        df['year'] = [i.split('_')[1] for i in df['year_raw']]
        df['var'] = [i.split('_')[0] for i in df['year_raw']]
        df = df.drop(['year_raw'], axis=1)
        df = df.pivot_table(values='value',
                            index=[colname, "month", "brand", "year"],
                            columns='var').reset_index()
        return df


def iterate_df_cleaning(dfs, colname):
    df_clean = pd.DataFrame()
    for _, df in enumerate(dfs):
        if not df.empty:
            df_unit_size_clean = clean_df_sample(df, colname)
            df_clean = pd.concat([df_clean, df_unit_size_clean])    
    return df_clean


if __name__ == "__main__":
    df_clean = pd.DataFrame()
    config = get_config()
    files = list_files_in_raw()
    for file in files: 
        tabs = get_tabs_excel(file)
        for tab in tabs: 
            if not exclude_df_tab(config, tab):
                print(f'{bcolors.OKGREEN}Reading and integrating tab: {tab} from file: {file}')
                df_raw = read_file(file, tab)
                df_header_clean = skip_line_to_headers(df_raw)
                if df_header_clean is not None:
                    if not df_header_clean.empty:
                        indexes_to_split_at = find_indexes_to_split_at(df_header_clean, config['split_parameters']['column_to_look_into'], config['split_parameters']['strings_to_match'])
                        df_samples = cut_df_into_unit_size_dfs(df_header_clean, indexes_to_split_at)
                        df_one_tab_clean = iterate_df_cleaning(df_samples, config['split_parameters']['column_to_look_into'])
                        df_clean = pd.concat([df_clean, df_one_tab_clean])

            else:
                print(f'{bcolors.OKCYAN}Excluding tab:{tab} from file: {file}, according to config.yml')
        df_clean['value'] = [ '%.2f' % elem for elem in df_clean['value'] ]
        df_clean.to_csv('out_clean.csv', index=False)
