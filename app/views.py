from flask import request,jsonify,render_template
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from fileinput import filename
import numpy as np
import logging


logging.basicConfig(level=logging.INFO)

def get_db_connection():
    conn = psycopg2.connect(
        database="flask_db2",
        host="localhost",
        user="postgres",
        password="dev@postgres",
        port="5432"
    )
    conn.set_client_encoding('UTF8')
    return conn

def allowed_file(filename):
    allowed_extensions = ('csv')
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions


# Function to convert each column to a Python-native type (int, float, str, bool)
def convert_numpy_types(df):
    def convert(value):
        if isinstance(value, np.integer):  # Convert numpy integers (e.g., numpy.int64) to Python int
            return int(value)
        elif isinstance(value, np.floating):  # Convert numpy floats (e.g., numpy.float64) to Python float
            return float(value)
        elif isinstance(value, np.bool_):  # Convert numpy bool to Python bool
            return bool(value)
        elif isinstance(value, np.datetime64):  # Convert numpy datetime to Python datetime
            return value.tolist()  # Convert to Python datetime object
        elif pd.isna(value):  # Convert NaN to None (for PostgreSQL compatibility)
            return None
        else:  # Assume it's a string or other type, convert to str
            return str(value)
    print(df.dtypes)
    return df.applymap(convert)

def fix_encoding(value):
    """
    Convert any non-UTF-8 character to proper UTF-8. Handles common encodings.
    """
    if isinstance(value, str):
        try:
            # Ensure UTF-8 encoding by first encoding in 'latin1' or 'windows-1252'
            return value.encode('windows-1252').decode('utf-8', errors='replace')
        except UnicodeDecodeError:
            # If conversion fails, return the original value
            return value
    return value


def get_datatype(file):
    df = file
    # Dynamically determine the column names and data types
    columns = df.columns

    column_data_types = []

    for col in columns:
        # Infer the column's data type based on the data in the column
        dtype = df[col].dtype
        if np.issubdtype(dtype, np.integer):
            column_data_types.append('INTEGER')
        elif np.issubdtype(dtype, np.float64):
            column_data_types.append('FLOAT')
        elif np.issubdtype(dtype, np.bool_):
            column_data_types.append('BOOLEAN')
        elif np.issubdtype(dtype, np.object_):
            column_data_types.append('TEXT')
        elif np.issubdtype(dtype, np.datetime64):
            column_data_types.append('TIMESTAMP')
        elif np.issubdtype(dtype, np.timedelta64):
            column_data_types.append('INTERVAL')
        else:
            column_data_types.append('TEXT')

    return column_data_types


def process_csv_to_postgres(file):
    df = pd.read_csv(file,encoding='utf-8')

    if df.empty:
        raise ValueError('The CSV file is empty.')

    df = convert_numpy_types(df)

    df = df.applymap(fix_encoding)

    columns = df.columns

    table_name = request.form['table_name']

    conn = get_db_connection()
    cursor = conn.cursor()

    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for i, col in enumerate(columns):
        create_table_query += f"{col} {get_datatype(file=df)[i]}, "

    create_table_query = create_table_query.rstrip(', ') + ");"

    cursor.execute(create_table_query)
    conn.commit()

    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

    # Convert DataFrame rows to a list of tuples
    values = [tuple(x) for x in df.to_records(index=False)]

    from psycopg2.extras import execute_values
    execute_values(cursor, insert_query, values)

    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def get_data(table_name):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'''SELECT * FROM {table_name};''')
    data = cur.fetchall()
    cur.close()
    conn.close()

    return data

def index():
    return render_template('form.html')

def upload_csv():
    # Check if file is part of the request
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    # Check if file is empty
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # Process the file if it's a CSV
    if file and allowed_file(file.filename):
        try:
            process_csv_to_postgres(file)

            table_name = request.form['table_name']
            data = get_data(table_name)
            return render_template('index.html',data=data,table_name=table_name ),201
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Only CSV files are allowed.'}), 400




