import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from sqlalchemy import create_engine
from pipeline import (
    download_and_extract_zip,
    save_dataframe_to_sqlite,
    process_temperature_data,
    load_data_from_sqlite,
    download_kaggle_dataset
)

class TestPipeline(unittest.TestCase):

    @patch('pipeline.requests.get')
    @patch('pipeline.zipfile.ZipFile')
    def test_download_and_extract_zip(self, mock_zipfile, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b'Fake Content'

        mock_zip = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip
        mock_zip.namelist.return_value = ['test.csv']
        mock_file = unittest.mock.mock_open(read_data="Area,Element,Year,Value\nAlbania,Temperature change,2020,1.5")
        mock_zip.open.return_value = mock_file()

        url = "http://example.com/fake.zip"
        df = download_and_extract_zip(url)
        self.assertFalse(df.empty)
        self.assertEqual(df.iloc[0]['Area'], 'Albania')

    @patch('pipeline.create_engine')
    @patch.object(pd.DataFrame, 'to_sql')
    def test_save_dataframe_to_sqlite(self, mock_to_sql, mock_create_engine):
        df = pd.DataFrame({
            'Area': ['Albania'],
            'Year': [2020],
            'Change': [1.5]
        })
        mock_engine = mock_create_engine.return_value
        mock_conn = mock_engine.connect.return_value.__enter__.return_value

        save_dataframe_to_sqlite(df, 'temperature_data')
        mock_to_sql.assert_called_once_with('temperature_data', con=mock_engine, if_exists='replace', index=False)

    def test_process_temperature_data(self):
        years = range(1961, 2023)
        data = {
            'Area': ['Albania', 'Brazil'],
            'Element': ['Temperature change', 'Temperature change'],
            'Area Code': [1, 2],
            'Area Code (M49)': [1, 2],
            'Element Code': [1, 2],
            'Months Code': [1, 2],
            'Unit': ['C', 'C']
        }
        for year in years:
            data[f'Y{year}F'] = ['E', 'N']
            data[f'Y{year}'] = [1.5, 2.5]

        df = pd.DataFrame(data)
        processed_df = process_temperature_data(df)

        self.assertFalse(processed_df.empty)
        self.assertEqual(processed_df.iloc[0]['Area'], 'Albania')
        self.assertNotIn('Brazil', processed_df['Area'].values)
        self.assertNotIn('Element', processed_df.columns)
        self.assertNotIn('Area Code', processed_df.columns)
        self.assertNotIn('Area Code (M49)', processed_df.columns)
        self.assertNotIn('Element Code', processed_df.columns)
        self.assertNotIn('Months Code', processed_df.columns)
        self.assertNotIn('Unit', processed_df.columns)
        self.assertIn('Year', processed_df.columns)
        self.assertIn('Change', processed_df.columns)

    @patch('pipeline.create_engine')
    def test_load_data_from_sqlite(self, mock_create_engine):
        mock_engine = mock_create_engine.return_value
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.fetchall.return_value = [
            {'Area': 'Albania', 'Year': 2020, 'Change': 1.5}
        ]
        mock_conn.execute.return_value.keys.return_value = ['Area', 'Year', 'Change']

        mock_results = [
            {'Area': 'Albania', 'Year': 2020, 'Change': 1.5}
        ]
        mock_df = pd.DataFrame(mock_results)
        mock_conn.execute.return_value = mock_df

        with patch('pipeline.pd.read_sql_table', return_value=mock_df):
            df = load_data_from_sqlite('temperature_data')

        self.assertFalse(df.empty)
        self.assertEqual(df.iloc[0]['Area'], 'Albania')

    @patch('pipeline.KaggleApi')
    @patch('pipeline.pd.read_csv')
    @patch('pipeline.save_dataframe_to_sqlite')
    def test_download_kaggle_dataset(self, mock_save, mock_read_csv, mock_kaggle_api):
        mock_api = mock_kaggle_api.return_value
        mock_df = pd.DataFrame({
            'country_or_area': ['Albania', 'Brazil'],
            'year': [2020, 2020],
            'value': [100, 200]
        })
        mock_read_csv.return_value = mock_df

        dataset_name = "unitednations/global-food-agriculture-statistics"
        username = "fake_username"
        key = "fake_key"

        unique_countries = download_kaggle_dataset(dataset_name, username, key)
        self.assertIn('Albania', unique_countries)
        self.assertNotIn('Brazil', unique_countries)

if __name__ == '__main__':
    unittest.main()
