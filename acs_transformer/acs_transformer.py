# Import modules

import io
import os
import re
import zipfile
import requests
import pandas as pd


def acs_transformer(year, state_abbr, table, geo_unit, content_type):
    '''
    return a dataframe of American Community Survey 5 year data for a given state, table, and year.
    :param year: The end year of the 5 year period, in 4 digits. E.g. 2015.
    :param state_abbr: The two-letter state abbreviation. Washington, DC is 'dc' and Puerto Rico is 'pr'
    :param table: The table id, e.g. 'B01001'
    :param geo_unit: Select 'block' to return data for all census blocks in the state. Select 'non-block' to return data for 
    all other geographic units in the state.
    :param content_type: Select 'estimate' for counts only and 'margin' for margins of error only.
    :return: dataframe and description of selected table  
    '''

    # Create state name/abbreviation dictionary.

    state_dict = {
          'ak':'alaska',
          'al':'Alabama',
          'ar':'arkansas',
          'az':'arizona',
          'ca':'california',
          'co':'colorado',
          'ct':'connecticut',
          'dc':'districtofcolumbia',
          'de':'delaware',
          'fl':'florida',
          'ga':'georgia',
          'gu':'guam',
          'hi':'hawaii',
          'ia':'iowa',
          'id':'idaho',
          'il':'illinois',
          'in':'indiana',
          'ks':'kansas',
          'ky':'kentucky',
          'la':'louisiana',
          'ma':'massachusetts',
          'md':'maryland',
          'me':'maine',
          'mi':'michigan',
          'mn':'minnesota',
          'mo':'missouri',
          'ms':'mississippi',
          'mt':'montana',
          'na':'national',
          'nc':'northcarolina',
          'nd':'northdakota',
          'ne':'nebraska',
          'nh':'newhampshire',
          'nj':'newjersey',
          'nm':'newmexico',
          'nv':'nevada',
          'ny':'newyork',
          'oh':'ohio',
          'ok':'oklahoma',
          'or':'oregon',
          'pa':'pennsylvania',
          'pr':'puertorico',
          'ri':'rhodeisland',
          'sc':'southcarolina',
          'sd':'southdakota',
          'tn':'tennessee',
          'tx':'texas',
          'ut':'utah',
          'va':'virginia',
          'vi':'virginislands',
          'vt':'vermont',
          'wa':'washington',
          'wi':'wisconsin',
          'wv':'westvirginia',
          'wy':'wyoming'}
    

    ##############################################
    #          Transform arguments and           #
    #         create additional variables        #
    ##############################################
    
    year = str(year) # todo: confirm this is needed
    
    # standardize formatting
    state_abbr_upper = state_abbr.upper() # uppercase used in geographic call later
    state_abbr = state_abbr.lower()
    
    table = table.upper()
    
    # Get geographic level of data to use in data import url
    
    if geo_unit == 'block':
        url_geo_unit = 'Tracts_Block_Groups_Only'
    elif geo_unit == 'non_block':
        url_geo_unit = 'All_Geographies_Not_Tracts_Block_Groups'
    else:
        print('Incorrect geographic unit selected. Choose "block" or "non_block".')
    
    
    # Get full state name. 
    state_name = state_dict[state_abbr]
    
    # Get sequence table, the number that determines the table structure.
    sequence_table_url = 'http://www2.census.gov/programs-surveys/acs/summary_file/{0}/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.xls'.format(year)
    sequence_table_df = pd.read_excel(sequence_table_url, dtype=str)
    
    # clean up column names
    sequence_table_df.columns = sequence_table_df.columns.str.replace(' ', '_').str.lower()
    
    # Get sequence number, which determines which file should be used for table headers.
    sequence = sequence_table_df['sequence_number'].loc[sequence_table_df['table_id'] == table].max()
    
    # Left pad the sequence number for later use
    padded_sequence = '{:0>4}'.format(sequence_table_df['sequence_number'].loc[sequence_table_df['table_id'] == table].max())
    
    # Get the number of columns at the beginning of the dataframe that are common to all tables in the df.
    #last_common_column = int(sequence_table_df['start_position'].loc[sequence_table_df['table_id'] == table].min()) - 1
    
    # Harcoding for now b/c last common column always seems to be 6
    
    last_common_column = 6
    

    ##############################################
    #                 Import files               #
    ##############################################     
    
    # raw data file

    url = 'https://www2.census.gov/programs-surveys/acs/summary_file/{0}/data/5_year_seq_by_state/{1}/{2}/{0}5{3}{4}000.zip'.\
    format(year, state_name, url_geo_unit, state_abbr,padded_sequence)
    
    results = requests.get(url)
    
    zipped = zipfile.ZipFile(io.BytesIO(results.content))
    
    for value in content_type:
        if content_type == 'margin':
            raw_csv = zipped.extract('m{0}5{1}{2}000.txt'.format(year,state_abbr, padded_sequence)) # saves in pwd, unless other is specified
            raw_df = pd.read_csv(raw_csv, dtype=str,header=None)
        elif content_type == 'estimate':
            raw_csv = zipped.extract('e{0}5{1}{2}000.txt'.format(year, state_abbr, padded_sequence))
            raw_df = pd.read_csv(raw_csv, dtype=str,header=None)
        else:
            print('no file') # need this?
    
    zipped.close()
    
    
    # Header file
    
    url = 'https://www2.census.gov/programs-surveys/acs/summary_file/{0}/data/{0}_5yr_Summary_FileTemplates.zip'.format(year)
    results = requests.get(url)
    
    zipped = zipfile.ZipFile(io.BytesIO(results.content))
    header_excel = zipped.extract('Seq{0}.xls'.format(sequence))
    
    # select estimate or margin of error tab
    if content_type == 'estimate':
        header_tab = 'E'
    elif content_type == 'margin':
        header_tab = 'M'
    else:
        print('Incorrect content type selected. Choose "estimate" or "margin".')
        
    header_df = pd.read_excel(header_excel, sheet_name= header_tab, dtype=str)
    
    zipped.close()
    

    # Geographic location name file
    
    geo_df = pd.read_excel('https://www2.census.gov/programs-surveys/acs/summary_file/{0}/documentation/geography/5_year_Mini_Geo.xlsx'.format(year),
                  sheet_name = state_abbr_upper, 
                  dtype=str)


    ##############################################
    #          Join raw and header dfs           #
    ############################################## 
    
    # Change raw_df column names to align with header_df column names
    raw_df.columns = header_df.columns
    
    joined_df = header_df.append(raw_df)
    
    # Reindex becasue line above results in 2 rows with index 0
    joined_df = joined_df.reset_index(drop=True)
        
    
    ##############################################
    #              Select & clean                #
    #        the requested table's columns       #
    ##############################################    
    
    # Select the columns that all tables in the df have in common
    common_df = joined_df.iloc[:, 0: last_common_column]
    
    # select the columns for the table to be returned
    table_df = joined_df.loc[:, joined_df.columns.str.startswith('{0}_'.format(table))]
    
    # final data columns
    df = pd.concat([common_df, table_df],axis=1)
    
    # replace column names with first row.
    df.columns = df.iloc[0]
    df.drop([0], inplace = True)
    
    # remove table name and universe description from column names
    universe = (sequence_table_df['table_title'].loc[(sequence_table_df['table_id'] == table) & \
                                            (sequence_table_df['start_position'] == 'nan') & \
                                            (sequence_table_df['line_number'] == 'nan')]).to_string(index=False)
    
    
    # hardcode replacement of extra '%'
    df.columns = df.columns.str.replace(r'^.*{0}'.format(universe),'', regex=True)
        
 
    ##############################################
    #             Join geographic df             #
    ##############################################
    
    df = df.merge(geo_df, on='LOGRECNO', how='left')
    
    # extract the geographic data columns from the df
    geo_cols = [df.iloc[:,-1], df.iloc[:,-2]]
    df.drop(df.columns[[-1,-2,-3]], axis=1, inplace=True)
    
    # for each element in the list of geo column series, insert it into postion 6, or last common column
    for i in geo_cols:
        df.insert(last_common_column, i.name, i, allow_duplicates=False)
    
    ##############################################
    #                Clean columns               #
    ##############################################
    
    df.columns = df.columns.str.strip()\
          .str.replace(' ','_')\
          .str.replace(':','')\
          .str.replace('%' , '')\
          .str.replace('(' , '_')\
          .str.replace (')' , '_')\
          .str.lower()
    
    df.rename(columns={'name':'geographic_unit'}, inplace=True)
    
    ##############################################
    #      Delete objects and return df          #
    ##############################################
    
    del(sequence_table_df, raw_csv, raw_df, header_excel, header_df, geo_df, common_df, table_df, geo_cols)
    
    print('Dataframe for table {0} for the state of {1}'.format(table,state_name.capitalize()))
    return df