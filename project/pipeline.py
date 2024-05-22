import os
import pandas as pd
import sqlite3


# Function to load and clean the temperature dataset
def clean_temperature_data(file_path):
    df = pd.read_csv(file_path)
    # Perform any necessary cleaning operations
    df = df.dropna(subset=['AverageTemperature'])
    df['dt'] = pd.to_datetime(df['dt'])
    df = df[
        df['Country'].isin(['Austria', 'Belgium', 'France', 'Germany', 'Italy', 'Netherlands', 'Spain', 'Switzerland'])]
    return df


# Function to load and clean the agricultural data
def clean_agriculture_data(file_path):
    df = pd.read_csv(file_path)
    # Perform any necessary cleaning operations
    df = df.dropna(subset=['value'])
    df['year'] = df['year'].astype(int)
    df = df[
        df['country_or_area'].isin(['Austria', 'Belgium', 'France', 'Germany', 'Italy', 'Netherlands', 'Spain', 'Switzerland'])]
    return df


# Function to save a DataFrame to a SQLite database
def save_to_sqlite(df, db_path, table_name):
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)


# Main function
def main():

    temp_file_path = './GlobalLandTemperaturesByCountry.csv'
    agri_file_path = './fao_data_production_indices_data.csv'

    # Load and clean datasets
    temp_df = clean_temperature_data(temp_file_path)
    agri_df = clean_agriculture_data(agri_file_path)

    # Save cleaned data to SQLite databases
    cleaned_data_dir = './data/cleaned'
    if not os.path.exists(cleaned_data_dir):
        os.makedirs(cleaned_data_dir)

    save_to_sqlite(temp_df, os.path.join(cleaned_data_dir, 'cleaned_temperature_data.sqlite'), 'temperature_data')
    save_to_sqlite(agri_df, os.path.join(cleaned_data_dir, 'cleaned_agriculture_data.sqlite'), 'agriculture_data')


if __name__ == "__main__":
    main()
