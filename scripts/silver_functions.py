import requests
import pandas as pd
import numpy as np
from collections import Counter

# restructuring metadata
def restructure_metadata(json_metadata):
    options_mapping = {}
    column_names = {}

    # Extract options and names
    for field in json_metadata:
        if "options" in field:
            options_mapping[field["id"]] = field["options"]
            column_names[field["id"]] = field["name"]

    # Create the restructured map
    restructured_mapping = {
        column_names[column_id]: {str(option["id"]): option["label"] for option in options}
        for column_id, options in options_mapping.items()
        if column_id in column_names
    }

    return restructured_mapping

# map Field Names
def map_columns(df, options_mapping):
    for column in df.columns:
        if column in options_mapping:
            df[column] = df[column].apply(lambda x: options_mapping[column].get(str(x), x))
    return df

# map Row Values

def map_columns_by_name(df, mapping):
    non_existing_columns = []  

    for column_name, column_mapping in mapping.items():
        if column_name in df.columns:

            # Convertir los valores a cadena y quitar espacios, además convertir todo a minúsculas
            df[column_name] = df[column_name].astype(str)

            # Mapear las columnas
            column_mapping = {str(k).strip().lower(): v for k, v in column_mapping.items()}
            df[column_name] = (
                df[column_name]
                .map(column_mapping)
                .fillna(df[column_name])  # Rellenar con el valor original si no hay coincidencias
            )

        else:
            non_existing_columns.append(column_name)
    
    if non_existing_columns:
        print(f"Las siguientes columnas no existen en el DataFrame: {non_existing_columns}")
    
    return df


# Function for split grouped date values
def split_column(df, column, new_name):
    if column in df.columns:
        separated_columns = df[column].str.split(', ', expand=True)
        separated_columns.columns = [
            f"{new_name} {i + 1}" for i in range(separated_columns.shape[1])
        ]
        df = pd.concat([df, separated_columns], axis=1)
        df = df.drop(column, axis=1)
    return df

# Function to convert date columns
def process_date_columns(df, prefix_patterns):
    for pattern in prefix_patterns:
        matching_columns = [col for col in df.columns if pattern in col]
        
        for col in matching_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
            if df[col].notna().sum() > 0:
                df[col] = df[col].astype(str).apply(lambda x: x if len(x) > 10 else x + ' 00:00:00')

                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    
    return df

# Function to identify the first kind of activity

def contact_type(row, col_name ,prefix_and_cat):
    for prefix, category in prefix_and_cat.items():
        columns_with_prefix = [col for col in row.index if col.startswith(prefix)]
        for col in columns_with_prefix :
            if row[col] == row[col_name]:
                return category
        non_null_values = [row[col] for col in columns_with_prefix if pd.notna(row[col])]

        if len(non_null_values) == 1:
            return category
    return 'No tuvo'

# Function to calculate days between activities
def time_calculator_min(df, start_date, end_date):
    new_col_name = f'Tiempo {start_date}-{end_date} (min)'
    df[new_col_name] = df[end_date]-df[start_date]
    df[new_col_name] = (df[new_col_name].dt.total_seconds() / 60).round(2)
    return df

# Function to calculate days between activities
def time_calculator_hours(df, start_date, end_date):
    new_col_name = f'Tiempo {start_date}-{end_date} (hr)'
    df[new_col_name] = df[end_date]-df[start_date]
    df[new_col_name] = (df[new_col_name].dt.total_seconds() / 3600).round(2)
    return df

# Function to calculate days between activities
def time_calculator_days(df, start_date, end_date):
    new_col_name = f'Tiempo {start_date}-{end_date} (días)'
    df[new_col_name] = df[end_date]-df[start_date]
    df[new_col_name] = (df[new_col_name].dt.total_seconds() / (24 * 3600)).round(1)
    return df

# Function to calculate and classify events between dates
def count_events(df, start_column, end_column, event_columns):

    # columns to datetime
    df[event_columns] = df[event_columns].apply(pd.to_datetime, errors='coerce')
    df[start_column] = pd.to_datetime(df[start_column], errors='coerce')
    df[end_column] = pd.to_datetime(df[end_column], errors='coerce')

    # Classify columns
    efectivas = [col for col in event_columns if "efectiva" in col and "no efectiva" not in col]
    no_efectivas = [col for col in event_columns if "no efectiva" in col]
    sin_avance = [col for col in event_columns if "sin avance" in col]
    seguimiento_wsp = [col for col in event_columns if "Seguimiento WSP" in col or "Video WSP" in col]

    # new column names
    col_efectiva = f"Efectiva {start_column} - {end_column}"
    col_no_efectiva = f"No Efectiva {start_column} - {end_column}"
    col_sin_avance = f"Sin Avance {start_column} - {end_column}"
    col_seguimiento = f"Seguimiento WSP {start_column} - {end_column}"
    col_total = f"Total Actividades {start_column} - {end_column}"

    new_columns = pd.DataFrame(index=df.index)

    # counts for each type
    new_columns[col_efectiva] = df.apply(
        lambda row: sum(pd.notna(row[col]) and row[start_column] <= row[col] <= row[end_column] for col in efectivas), axis=1
    )
    new_columns[col_no_efectiva] = df.apply(
        lambda row: sum(pd.notna(row[col]) and row[start_column] <= row[col] <= row[end_column] for col in no_efectivas), axis=1
    )
    new_columns[col_sin_avance] = df.apply(
        lambda row: sum(pd.notna(row[col]) and row[start_column] <= row[col] <= row[end_column] for col in sin_avance), axis=1
    )
    new_columns[col_seguimiento] = df.apply(
        lambda row: sum(pd.notna(row[col]) and row[start_column] <= row[col] <= row[end_column] for col in seguimiento_wsp), axis=1
    )

    # total activities
    new_columns[col_total] = (
        new_columns[col_efectiva] +
        new_columns[col_no_efectiva] +
        new_columns[col_sin_avance] +
        new_columns[col_seguimiento]
    )

    # Combine new columns with the original DataFrame
    df = pd.concat([df, new_columns], axis=1)

    return df

# Function to eliminate duplicates from dataframe
def delete_duplicates(df, column):
    counter = Counter(df[column])
    df = df[df[column].map(counter) == 1]
    return df
