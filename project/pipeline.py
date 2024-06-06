import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine
import os
import requests
import zipfile
from io import BytesIO

# List of European countries
european_countries = [
    'Albania', 'Andorra', 'Armenia', 'Austria', 'Azerbaijan', 'Belarus', 'Belgium', 'Bosnia and Herzegovina',
    'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Georgia',
    'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kazakhstan', 'Kosovo', 'Latvia', 'Liechtenstein',
    'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 'Monaco', 'Montenegro', 'Netherlands', 'North Macedonia', 'Norway',
    'Poland', 'Portugal', 'Romania', 'Russia', 'San Marino', 'Serbia', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
    'Switzerland', 'Turkey', 'Ukraine', 'United Kingdom', 'Vatican City'
]

def download_and_extract_zip(url, headers=None):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    with zipfile.ZipFile(BytesIO(response.content)) as zfile:
        file_names = zfile.namelist()
        with zfile.open(file_names[0]) as file:
            df = pd.read_csv(file, encoding='latin1')
            return df.dropna()

def save_dataframe_to_sqlite(df, table_name, db_path='../data/dataset.sqlite'):
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        abs_db_path = os.path.abspath(db_path)
        engine = create_engine(f'sqlite:///{abs_db_path}')
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"DataFrame successfully saved to table '{table_name}' in database '{abs_db_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

def process_temperature_data(df):
    df = df[df['Area'].isin(european_countries)]
    df = df[df['Element'] == 'Temperature change']
    df = df.drop(['Area Code', 'Area Code (M49)', 'Element Code', 'Months Code', 'Unit', 'Element'], axis=1)
    years = range(1961, 2023)
    for year in years:
        df = df[df[f'Y{year}F'] == 'E']
        df = df.drop(f'Y{year}F', axis=1)

    year_columns = [f'Y{year}' for year in years]
    df = pd.melt(df, id_vars=['Area'], value_vars=year_columns, var_name='Year', value_name='Change')
    df['Year'] = df['Year'].str[1:].astype(int)
    return df

def load_data_from_sqlite(table_name, db_path='../data/dataset.sqlite'):
    abs_db_path = os.path.abspath(db_path)
    engine = create_engine(f'sqlite:///{abs_db_path}')
    df = pd.read_sql_table(table_name, con=engine)
    return df

def download_kaggle_dataset(dataset_name, username, key):
    # Initialize Kaggle API
    api = KaggleApi()
   # api.authenticate(username=username, key=key)

    # Download dataset
    api.dataset_download_files(dataset_name, unzip=True)

    # Load the dataset into a DataFrame
    df = pd.read_csv("fao_data_production_indices_data.csv")

    # Rename the column 'country_or_area' to 'Area'
    df = df.rename(columns={'country_or_area': 'Area'})

    # Select only the desired columns
    df = df[['Area', 'year', 'value']]

    # Filter for European countries
    df = df[df['Area'].isin(european_countries)]

    # Remove rows with missing values
    df = df.dropna()

    # Save the DataFrame to SQLite
    save_dataframe_to_sqlite(df, 'food_data', db_path='../data/dataset.sqlite')

    return df['Area'].unique()

def main():
    # Kaggle credentials
    username = "nhbsohel"
    key = "351073502779b168259267ec2964a09a"

    # Download Kaggle dataset and get unique countries
    dataset_name = "unitednations/global-food-agriculture-statistics"
    food_countries = download_kaggle_dataset(dataset_name, username, key)

    # Process temperature data
    temperature_url = 'https://fenixservices.fao.org/faostat/static/bulkdownloads/Environment_Temperature_change_E_All_Data.zip'
    temperature_df = download_and_extract_zip(temperature_url, headers={'User-Agent': 'Mozilla/5.0'})
    processed_temp_df = process_temperature_data(temperature_df)
    save_dataframe_to_sqlite(processed_temp_df, 'temperature_data', db_path='../data/dataset.sqlite')

    # Load the processed data for visualization
    food_df = load_data_from_sqlite('food_data')
    processed_temp_df = load_data_from_sqlite('temperature_data')

if __name__ == '__main__':
    main()
