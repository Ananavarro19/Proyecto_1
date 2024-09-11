import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Título de la aplicación
st.title('Upload and Merge Asset and Employee Files')

# Instrucciones
st.write("Upload the Asset and Employee Excel files (both files are required).")

# Subir dos archivos de Excel
uploaded_files = st.file_uploader("Choose the Asset and Employee files", type=["xlsx"], accept_multiple_files=True)

# Función para conectar y realizar la inserción en la base de datos en bloque
def insert_assets_in_bulk(df, table_name='asignacion'):
    connection = None
    cursor = None

    try:
        # Establecer conexión con la base de datos
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Preparar la consulta de inserción
            insert_query = f"""
            INSERT INTO {table_name} (name, document, type, serial)
            VALUES (%s, %s, %s, %s)
            """

            # Convertir DataFrame a una lista de tuplas para ejecutar la inserción en bloque
            assets_data = df.to_records(index=False).tolist()

            # Ejecutar la consulta de inserción en bloque
            cursor.executemany(insert_query, assets_data)
            
            # Confirmar la transacción
            connection.commit()

            st.success(f"{cursor.rowcount} rows inserted successfully.")

    except Error as e:
        st.error(f"Error: {e}")
        if connection:
            connection.rollback()

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()

# Si ambos archivos se suben correctamente
if len(uploaded_files) == 2:
    try:
        # Identificar los archivos
        file1 = uploaded_files[0]
        file2 = uploaded_files[1]

        # Leer ambos archivos de Excel y almacenarlos en DataFrames
        df_assets = pd.read_excel(file1,)
        df_employees = pd.read_excel(file2,)
        
        # Mostrar mensaje de éxito al cargar los archivos
        st.success('Both files were uploaded successfully!')
        
        st.write("Assets Data:")
        st.dataframe(df_assets)
        st.write("Employees Data:")
        st.dataframe(df_employees)

        # Combinar los DataFrames en base a la columna "employee_id" del archivo de activos
        df_combined = pd.merge(df_assets, df_employees, left_on="RESPONSABLE", right_on="DOCUMENTO", how="inner")

        # Seleccionar columnas relevantes para mostrar
        df_final = df_combined[['NOMBRE', 'DOCUMENTO', 'TIPO', 'SERIAL']]
        df_final.columns = ['NOMBRE_EMPLEADO', 'DOCUMENTO', 'TIPO', 'SERIAL']

        # Mostrar el DataFrame final combinado
        st.write("Combined content:")
        st.dataframe(df_final)

        # Botón para guardar los datos en la base de datos
        if st.button('Save to Database'):
            insert_assets_in_bulk(df_final)
    
    except Exception as e:
        st.error(f"Error processing the Excel files: {e}")

else:
    st.warning("Please upload exactly two Excel files.")
