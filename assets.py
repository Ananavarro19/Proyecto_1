import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Título de la aplicación
st.title('Upload and Merge Patient and Responsible Party Files')

# Instrucciones
st.write("Upload the Patient and Responsible Party files (both files are required).")

# Subir ambos archivos de Excel al mismo tiempo
uploaded_files = st.file_uploader("Choose the Patient and Responsible Party files", type=["xlsx"], accept_multiple_files=True)

# Función para conectar y realizar la inserción en la base de datos en bloque
def insert_patients_in_bulk(df, table_name='pacientes_responsables'):
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
            INSERT INTO {table_name} (nombre_paciente, apellido_paciente, diagnostico, responsable, parentesco)
            VALUES (%s, %s, %s, %s, %s)
            """

            # Convertir DataFrame a una lista de tuplas para ejecutar la inserción en bloque
            patients_data = df.to_records(index=False).tolist()

            # Ejecutar la consulta de inserción en bloque
            cursor.executemany(insert_query, patients_data)
            
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
        df_pacientes = pd.read_excel(file1)
        df_responsables = pd.read_excel(file2)
        
        # Mostrar mensaje de éxito al cargar los archivos
        st.success('Both files were uploaded successfully!')
        
        st.write("Patients:")
        st.dataframe(df_pacientes)
        st.write("Responsible Parties:")
        st.dataframe(df_responsables)

        # Combinar los DataFrames en base a la columna "Responsable" del archivo de pacientes
        df_combined = pd.merge(df_pacientes, df_responsables, left_on="Responsable", right_on="Nombre", how="inner")

        # Seleccionar columnas relevantes para mostrar
        df_final = df_combined[['Nombre_x', 'Apellido_x', 'Diagnóstico', 'Responsable', 'Parentesco']]
        df_final.columns = ['Patient Name', 'Patient Last Name', 'Diagnosis', 'Responsible', 'Relationship']

        # Mostrar el DataFrame final combinado
        st.write("Combined content:")
        st.dataframe(df_final)

        # Botón para guardar los datos en la base de datos
        if st.button('Save to Database'):
            insert_patients_in_bulk(df_final)
    
    except Exception as e:
        st.error(f"Error processing the Excel files: {e}")

else:
    st.warning("Please upload exactly two Excel files.")