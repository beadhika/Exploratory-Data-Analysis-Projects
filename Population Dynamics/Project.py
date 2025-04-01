# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 15:26:01 2022

@author: Group 7
"""


# Relevant libraries
import pandas as pd
import numpy as np
import requests as r
from bs4 import BeautifulSoup

#URL for scraping
URL = "https://countrycode.org/"

# path for reading the files 
path = "C:/Users/benja/OneDrive/Desktop/BI project files/"

pop_file = "Population.csv"
energy_file = "owid-energy-data 3.csv"
area_file = "Area.csv"
food_file = "Food.csv"
inflation_file = "inflation_1.csv"
GDP_file = "GDP per capita.csv"


## population dataset
df_pop_ori = pd.read_csv(path+pop_file)
df_pop = df_pop_ori.melt('Country Code',value_vars=['2016','2017','2018','2019']) #melting columns 

df_pop = df_pop.rename({'variable': 'Year','value':'Population'}, axis='columns') #renaming columns to standard names 
df_pop['Country Code'] = df_pop['Country Code'].astype(str)   #standardizing the data types
df_pop['Year'] = df_pop['Year'].astype(int)


## energy dataset
df_energy_ori = pd.read_csv(path+energy_file) #reading the data into a dataframe

#The years are already in the rows so we do not need to use melt function
df_energy_transition=df_energy_ori[df_energy_ori['year'].isin([2016,2017,2018,2019])] #filtering the rows by year

# If you look at the dataframe there are 128 columns. We are interested in the total energy consumption.
# We can not look for each column name to decide which gives us energy consumption.
# Lets select only the columns that contain the word 'consumption'. 

energy_col = df_energy_transition.columns.to_list() #making a list of all column names
energy_consumption_col = ['country','year','iso_code'] #initializing a list to make a list of consumption columns
for element in energy_col : #looping through the column name
  if 'consumption' in element:
      energy_consumption_col.append(element) #adding the consumption columns to the list


df_energy_transition2 = df_energy_transition[energy_consumption_col] #new dataframe containing consumption columns
df_energy_transition2.columns

#The 'primary_energy_consumption' column gives the energy consumption of a country.
df_energy = df_energy_transition2[['year','iso_code','primary_energy_consumption']]
df_energy.rename(columns={'iso_code':'Country Code','year':'Year','primary_energy_consumption':'primary_energy_consumption_terrawats_per_hour'},inplace=True)
df_energy['Country Code'] = df_energy['Country Code'].astype(str)
df_energy['Year'] = df_energy['Year'].astype(int)



## area dataset 
df_area_ori = pd.read_csv(path + area_file)
df_area = df_area_ori
df_area.rename(columns={'country':'Country'},inplace=True)
df_area['Country'] = df_area['Country'].astype(str)



## food dataset
df_food_ori = pd.read_csv(path + food_file)
df_food = df_food_ori[df_food_ori['Year'].isin([2016,2017,2018,2019])][['Country','Year','Production (t)']] # select specific columns and filter by year 
df_food.rename({'Production (t)': 'Food Production in tonnes'},axis = 'columns',inplace = True)
df_food['Country'] = df_food['Country'].astype(str)
df_food['Year'] = df_food['Year'].astype(int)



## inflation dataset 
df_inflation_ori = pd.read_csv(path + inflation_file, sep=',',  encoding='latin-1') # encoding type and separator as parameters
df_inflation = df_inflation_ori.melt(id_vars = ['Country Code'],value_vars=['2016','2017','2018','2019'])
df_inflation.rename({'variable': 'Year','value':'Inflation'}, axis='columns',inplace= True)
df_inflation['Country Code'] = df_inflation['Country Code'].astype(str)
df_inflation['Year'] = df_inflation['Year'].astype(int)



## GDP per capita
df_GDP_ori = pd.read_csv(path + GDP_file)
df_GDP_ori.columns
# As we can see that the column names are all unnamed.
# If we look at the full view of the dataframe, the first three rows and the column names are redundant.

df_GDP_ori.rename(columns = df_GDP_ori.iloc[3],inplace =True) #setting the fourth row as column name.
df_GDP_transition = df_GDP_ori.iloc[4:] #selecting only rows starting from fifth as three are redundant and fourth is used as column names
df_GDP = df_GDP_transition.melt(id_vars = ['Country Code'],value_vars =[2016.0,2017.0,2018.0,2019.0])
df_GDP.rename({'variable': 'Year','value': 'GDP per capita'}, axis='columns',inplace =True)
df_GDP['Country Code'] = df_GDP['Country Code'].astype(str)
df_GDP['Year'] = df_GDP['Year'].astype(int)


## Function to scrape the Countries and Codes
def scrape_country_with_code(URL):

    final_countries_info = []
    country_data = {}
    
    res = r.get(URL)
    soup = BeautifulSoup(res.content, 'lxml')
    
    country_table = soup.find('table', 
                          attrs = {'class': "table table-hover table-striped main-table"})

    country_data = country_table.find('tbody')
    country_info = country_data.find_all('tr') 

    for country in country_info:
    
        list_attributes = country.find_all('td')

        current_country_data = {
            'Country': list_attributes[0].text,
            'Country Code': list_attributes[2].text.split('/')[1].strip()
        }

        # Add it to the list
        final_countries_info.append(current_country_data)
    
    return pd.DataFrame(final_countries_info)

# Run the Scraping

df_country = scrape_country_with_code(URL)

# Save all  the cleaned dataframes into CSV formats
df_country.to_csv(path+"Country.csv")
df_pop.to_csv(path+'pop_file_clean.csv') 
df_energy.to_csv(path+'energy_file_clean.csv')
df_area.to_csv(path+'area_file_clean.csv')
df_food.to_csv(path+'food_file_clean.csv')
df_inflation.to_csv(path+'inflation_file_clean.csv')
df_GDP.to_csv(path+'GDP_file_clean.csv')

## merged dataset
df_final = df_country.merge(df_area,how='left',on='Country')\
    .merge(df_food,how='left',on=['Country'])\
    .merge(df_energy,how='left',on=['Country Code','Year'])\
    .merge(df_pop,how='left',on=['Country Code','Year']).merge(df_inflation,how='left',on=['Country Code','Year'])\
        .merge(df_GDP,how='left',on=['Country Code','Year'])

#creating new columns
df_final['Population_density'] = df_final['Population']/df_final['area']
df_final['energy_consumption_terrawats_hour_per_capita'] = df_final['primary_energy_consumption_terrawats_per_hour']/ df_final['Population'] 
df_final['food_production_tonnes_per_capita'] = df_final['Food Production in tonnes']/ df_final['Population']



data = df_final

# Drop rows where Year, Area and Rank are null

def drop_unnecessery_rows(data, columns_interest = ['Year', 'rank', 'area']):

  df_copy = data.copy()

  final_df = df_copy.dropna(subset=columns_interest)

  return final_df

def fill_columns_with_median(data, group_col = 'Country Code'):

  data_copy = data.copy()

  list_countries_data = []
  relevant_data = data_copy.select_dtypes(include='float64')
  data_columns = list(relevant_data.columns)

  country_codes = list(data_copy[group_col].unique())

  for ctr_code in country_codes:

    # Get country of interest
    country_data = data_copy[data_copy[group_col] == ctr_code]

    for col in data_columns:
      median_value = country_data[col].median() 
      
      if(str(median_value)=='nan'):
        median_value = 0

      country_data[col].fillna(value = median_value, inplace = True)

    list_countries_data.append(country_data)

  final_data = pd.concat(list_countries_data)

  return final_data

# Drop unecessary rows 
data_without_bad_rows = drop_unnecessery_rows(data)

# Fill Missing values with median
final_data = fill_columns_with_median(data_without_bad_rows)
percent_missing = final_data.isnull().sum() * 100 / len(final_data)

# Print the final percentage of missing values
print(percent_missing)
# outputing the final dataframe to a file
final_data.to_csv(path+'final.csv')



