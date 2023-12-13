import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def create_table(conn):
    conn.execute('''
        CREATE TABLE electric_cars (
            VIN VARCHAR(10),
            County VARCHAR,
            City VARCHAR,
            State VARCHAR,
            Postal_Code INTEGER,
            Model_Year INTEGER,
            Make VARCHAR,
            Model VARCHAR,
            Electric_Vehicle_Type VARCHAR,
            CAFV_Eligibility VARCHAR,
            Electric_Range INTEGER,
            Base_MSRP INTEGER,
            Legislative_District INTEGER,
            DOL_Vehicle_ID INTEGER,
            Vehicle_Location VARCHAR,
            Electric_Utility VARCHAR,
            Census_Tract BIGINT  -- Use BIGINT instead of INTEGER for Census_Tract
        )
    ''')
    return 'electric_cars'

import numpy as np  # Import numpy for nan handling

def load_data(conn):
    table_name = create_table(conn)
    df = pd.read_csv('data/Electric_Vehicle_Population_Data.csv')

    # Replace nan values with a suitable default value (e.g., 0)
    df = df.replace({np.nan: 0})

    for index, row in df.iterrows():
        # Use individual values instead of *df.values.tolist()
        values = tuple(row)
        placeholders = ",".join(["?" for _ in range(len(values))])
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        conn.execute(query, parameters=values)
def write_result_to_parquet(result, output_folder, filename):
    df = result.fetch_df()
    pq.write_table(pa.table(df), f'{filename}.parquet')
def count_cars_by_city(conn):
    result = conn.execute('SELECT Model_Year, City, COUNT(*) AS Car_Count FROM electric_cars GROUP BY Model_Year, City')
    print("1")
    write_result_to_parquet(result, 'count_cars_by_city')

def top_3_models(conn):
    result = conn.execute('SELECT Model, COUNT(*) AS Car_Count FROM electric_cars GROUP BY Model ORDER BY Car_Count DESC LIMIT 3')
    write_result_to_parquet(result, 'top_3_models')

def top_model_by_postal_code(conn):
    result = conn.execute('''
        SELECT Postal_Code, Model, MAX(Car_Count) AS Max_Car_Count
        FROM (
            SELECT Postal_Code, Model, COUNT(*) AS Car_Count
            FROM electric_cars
            GROUP BY Postal_Code, Model
        ) t
        GROUP BY Postal_Code, Model
    ''')
    write_result_to_parquet(result,  'top_model_by_postal_code')

def count_cars_by_year(conn):
    result = conn.execute('SELECT Model_Year, COUNT(*) AS Car_Count FROM electric_cars GROUP BY Model_Year')
    write_result_to_parquet(result, 'count_cars_by_year')

def write_parquet_files(conn):
    for year in range(2010, 2023):
        result = conn.execute(f'SELECT * FROM electric_cars WHERE Model_Year = {year}')
        write_result_to_parquet(result, f'parquet_output/electric_cars_{year}')

def main():
    conn = duckdb.connect(database=':memory:', read_only=False)
    load_data(conn)
    count_cars_by_city(conn)
    top_3_models(conn)
    top_model_by_postal_code(conn)
    count_cars_by_year(conn)
    write_parquet_files(conn)
    conn.close()

if __name__ == "__main__":
    main()
