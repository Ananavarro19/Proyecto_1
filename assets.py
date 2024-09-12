import streamlit as app_ui
import pandas as data_frame
import mysql.connector
from mysql.connector import Error as DatabaseError
from dotenv import load_dotenv as load_env_vars
import os

load_env_vars()

app_ui.title('Upload and Merge Asset and Employee Files')
app_ui.write("Upload the Asset and Employee Excel files (both files are required).")

uploaded_excel_files = app_ui.file_uploader("Choose the Asset and Employee files", type=["xlsx"], accept_multiple_files=True)

def batch_insert_assets(asset_data, table='asset_assignment'):
    db_connection = None
    sql_cursor = None

    try:
        db_connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )

        if db_connection.is_connected():
            sql_cursor = db_connection.cursor()

            insert_sql = f"""
            INSERT INTO {table} (name, document, type, serial)
            VALUES (%s, %s, %s, %s)
            """

            assets_to_insert = asset_data.to_records(index=False).tolist()

            sql_cursor.executemany(insert_sql, assets_to_insert)
            db_connection.commit()

            app_ui.success(f"{sql_cursor.rowcount} rows inserted successfully.")

    except DatabaseError as db_err:
        app_ui.error(f"Error: {db_err}")
        if db_connection:
            db_connection.rollback()

    finally:
        if sql_cursor is not None:
            sql_cursor.close()
        if db_connection is not None and db_connection.is_connected():
            db_connection.close()

if len(uploaded_excel_files) == 2:
    try:
        asset_file = uploaded_excel_files[0]
        employee_file = uploaded_excel_files[1]

        asset_data_frame = data_frame.read_excel(asset_file)
        employee_data_frame = data_frame.read_excel(employee_file)

        app_ui.success('Both files were uploaded successfully!')
        
        app_ui.write("Assets Data:")
        app_ui.dataframe(asset_data_frame)
        app_ui.write("Employees Data:")
        app_ui.dataframe(employee_data_frame)

        merged_data_frame = data_frame.merge(asset_data_frame, employee_data_frame, left_on="RESPONSABLE", right_on="DOCUMENTO", how="inner")

        final_data_frame = merged_data_frame[['NOMBRE', 'DOCUMENTO', 'TIPO', 'SERIAL']]
        final_data_frame.columns = ['EMPLOYEE_NAME', 'DOCUMENT', 'TYPE', 'SERIAL']

        app_ui.write("Combined content:")
        app_ui.dataframe(final_data_frame)

        if app_ui.button('Save to Database'):
            batch_insert_assets(final_data_frame)
    
    except Exception as error:
        app_ui.error(f"Error processing the Excel files: {error}")

else:
    app_ui.warning("Please upload exactly two Excel files.")
