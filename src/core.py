import os
import json
import requests
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import sqlite3
from ydata_profiling import ProfileReport



def fetch_series_by_date(date: str) -> list:
    url = f"http://api.tvmaze.com/schedule/web?date={date}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error al llamar al API: {response.status_code}")


def save_json(data: list, filename: str, folder: str = "json") -> None:
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Datos guardados en {filepath}")



def load_and_normalize_json_files(folder: str) -> pd.DataFrame:
    dataframes = []

    for filename in os.listdir(folder):
        if filename.endswith(".json"):
            filepath = os.path.join(folder, filename)
            
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            df = pd.json_normalize(
                data,
                sep="_",
                record_path=None 
            )
            dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()    


def generate_profiling_report(df: pd.DataFrame, output_folder: str = 'profile', filename: str = 'file.html') -> None:
    import pdfkit
    os.makedirs(output_folder, exist_ok=True)
    output_html = os.path.join(output_folder, filename)
    output_pdf = os.path.join(output_folder, filename.replace('.html', '.pdf'))
    print(output_pdf)
    profile = ProfileReport(
        df,
        title="Reporte de Profiling - Series TV", 
        explorative=True
    )
    profile.to_file(output_html)
    pdfkit.from_file(output_html, output_pdf)
    print(f"Reporte de profiling guardado en {output_pdf}")


def drop_empty_columns(df):
    df = df.dropna(axis=1, how='all')
    threshold = 0.7
    return df.loc[:, df.isnull().mean() < threshold]
    

def drop_duplicates(df, subset=None):
    return df.drop_duplicates(subset=subset).reset_index(drop=True)


def convert_column_types(df):
    df['airdate'] = pd.to_datetime(df['airdate'], errors='coerce').dt.date  
    df['airstamp'] = pd.to_datetime(df['airstamp'], errors='coerce') 
    # categorical_columns = ['type', 'status', 'language', 'genres']  
    # for col in categorical_columns:
    #     if col in df.columns:
    #         df[col] = df[col].astype('category')
    return df


def fill_missing_values(df):
    df['runtime'].fillna(df['runtime'].median(), inplace=True)
    df['rating_average'].fillna(df['rating_average'].mean(), inplace=True)
    return df


def clean_dataframe(df):
    df = drop_empty_columns(df)
    df = drop_duplicates(df, subset=['id'])
    df = convert_column_types(df)
    return df


def save_to_parquet(df, folder, filename):
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filepath, compression='snappy')
    print(f"Archivo Parquet guardado en {filepath}")



def create_database(db_path, db_name):
    conn = sqlite3.connect(
        os.path.join(db_path, db_name)
    )

    cursor = conn.cursor()
    
    # Crear tabla `episodes`
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS episodes (
        id INTEGER PRIMARY KEY,
        url TEXT,
        name TEXT,
        season INTEGER,
        number REAL,
        type TEXT,
        airdate DATE,
        airtime TEXT,
        airstamp TIMESTAMP,
        runtime REAL,
        show_id INTEGER,
        FOREIGN KEY (show_id) REFERENCES shows (id)
    )
    ''')
    
    # Crear tabla `shows`
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shows (
        id INTEGER PRIMARY KEY,
        name TEXT,
        type TEXT,
        language TEXT,
        status TEXT,
        averageRuntime REAL,
        premiered DATE,
        ended DATE,
        officialSite TEXT,
        weight INTEGER
    )
    ''')

    # Crear tabla `genres`
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS genres (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        genre TEXT UNIQUE
    )
    ''')

    # Crear tabla `show_genres`
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS show_genres (
        show_id INTEGER,
        genre_id INTEGER,
        FOREIGN KEY (show_id) REFERENCES shows (id),
        FOREIGN KEY (genre_id) REFERENCES genres (id)
    )
    ''')

    # Crear tabla `schedule`
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS schedule (
        show_id INTEGER,
        time TEXT,
        days TEXT,
        FOREIGN KEY (show_id) REFERENCES shows (id)
    )
    ''')

    conn.commit()
    conn.close()
    print("Base de datos y tablas creadas exitosamente.")


def load_data_to_sqlite(parquet_path: str, db_path: str, db_name: str) -> None:
    df = pd.read_parquet(parquet_path)
    conn = sqlite3.connect(
        os.path.join(db_path, db_name)
    )
    
    shows_df = df[[ 
        "_embedded_show_id", "_embedded_show_name", "_embedded_show_type",
        "_embedded_show_language", "_embedded_show_status", 
        "_embedded_show_averageRuntime", "_embedded_show_premiered", 
        "_embedded_show_ended", "_embedded_show_officialSite",
        "_embedded_show_weight"
    ]].drop_duplicates().rename(
        columns={
            "_embedded_show_id": "id",
            "_embedded_show_name": "name",
            "_embedded_show_type": "type",
            "_embedded_show_language": "language",
            "_embedded_show_status": "status",
            "_embedded_show_averageRuntime": "averageRuntime",
            "_embedded_show_premiered": "premiered",
            "_embedded_show_ended": "ended",
            "_embedded_show_officialSite": "officialSite",
            "_embedded_show_weight": "weight"
        }
    )
    shows_df.to_sql("shows", conn, if_exists="replace", index=False)
    show_genres_df = df[["_embedded_show_id", "_embedded_show_genres"]].copy()
    show_genres_df["_embedded_show_genres"] = show_genres_df["_embedded_show_genres"].apply(
        lambda x: eval(x) if isinstance(x, str) else x
    )
    
    show_genres_df = show_genres_df.explode("_embedded_show_genres").dropna().rename(
        columns={
            "_embedded_show_id": "show_id",
            "_embedded_show_genres": "genre"
        }
    )
    # Crear tabla genres
    genres_df = pd.DataFrame(show_genres_df["genre"].unique(), columns=["genre"])
    genres_df.to_sql("genres", conn, if_exists="replace", index=False)
    
    show_genres_df = show_genres_df.merge(
        genres_df.reset_index().rename(columns={"index": "genre_id", "genre": "genre"}),
        on="genre",
        how="left"
    )
    show_genres_df[["show_id", "genre_id"]].to_sql("show_genres", conn, if_exists="replace", index=False)
    
    episodes_df = df[[ 
        "id", "url", "name", "season", "number", "type", "airdate", 
        "airtime", "airstamp", "runtime", "_embedded_show_id"
    ]].rename(columns={"_embedded_show_id": "show_id"})
    episodes_df.to_sql("episodes", conn, if_exists="replace", index=False)
    
    schedule_df = df[[
        "_embedded_show_id", "_embedded_show_schedule_time", "_embedded_show_schedule_days"
    ]].copy()
    
    schedule_df["_embedded_show_schedule_days"] = schedule_df["_embedded_show_schedule_days"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else str(x)
    )
    
    schedule_df = schedule_df.drop_duplicates().rename(
        columns={
            "_embedded_show_id": "show_id",
            "_embedded_show_schedule_time": "time",
            "_embedded_show_schedule_days": "days"
        }
    )
    schedule_df.to_sql("schedule", conn, if_exists="replace", index=False)
    
    conn.close()
    print("Datos cargados en la base de datos SQLite.")
