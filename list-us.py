import requests
from bs4 import BeautifulSoup
import pandas as pd
import hashlib
import logging
import os
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import re # Import regular expressions
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

# --- Configuración ---
SOURCE_URL = "https://www.state.gov/foreign-terrorist-organizations/"
USE_LOCAL_FILE = False # Set to True only for testing with the provided file
LOCAL_FILE_PATH = 'Foreign Terrorist Organizations - United States Department of State.txt'

LOG_FILE = 'state_gov_fto_status.log' # Guarda el último hash y estado
HASH_FILE = 'state_gov_fto.hash' # Archivo para guardar solo el hash

# TODO: Usar variables de entorno o AWS Secrets Manager en producción
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST, # ej. 'xxxxx.rds.amazonaws.com'
    "port": DB_PORT
}
# Decide whether to use the general table or a specific one
# Option 1: Use general table
DB_TABLE_NAME = "sanctions_list_us"
SOURCE_LIST_NAME = "StateDept_FTO"
# Option 2: Use specific table (would require creating it)
# DB_TABLE_NAME = "state_gov_fto"
# SOURCE_LIST_NAME = "StateDept_FTO" # Still useful for logging

# Configuración de Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

# --- Funciones Auxiliares (Reutilizadas y/o Adaptadas) ---

def get_html_content():
    """Obtiene el contenido HTML desde la URL o archivo local."""
    if USE_LOCAL_FILE:
        logging.info(f"Usando archivo local: {LOCAL_FILE_PATH}")
        try:
            with open(LOCAL_FILE_PATH, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            logging.error(f"Archivo local no encontrado: {LOCAL_FILE_PATH}")
            return None
        except Exception as e:
            logging.error(f"Error leyendo archivo local: {e}")
            return None
    else:
        logging.info(f"Consultando URL: {SOURCE_URL}")
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response = requests.get(SOURCE_URL, headers=headers, timeout=45) # Increased timeout
            response.raise_for_status() # Lanza excepción para errores HTTP 4xx/5xx
            # Asegurar codificación correcta (la página parece ser UTF-8)
            response.encoding = response.apparent_encoding
            return response.text
        except requests.exceptions.Timeout:
             logging.error(f"Timeout al obtener la URL {SOURCE_URL}")
             return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error al obtener la URL {SOURCE_URL}: {e}")
            return None

def calculate_hash(data_string):
    """Calcula el hash MD5 de un string."""
    return hashlib.md5(data_string.encode('utf-8')).hexdigest()

def read_last_hash(file_path):
    """Lee el último hash guardado."""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return f.read().strip()
    except Exception as e:
        logging.error(f"Error leyendo archivo de hash {file_path}: {e}")
    return None

def write_current_hash(file_path, current_hash):
    """Escribe el hash actual en el archivo."""
    try:
        with open(file_path, 'w') as f:
            f.write(current_hash)
        logging.info(f"Nuevo hash guardado en {file_path}")
    except Exception as e:
        logging.error(f"Error escribiendo archivo de hash {file_path}: {e}")

def parse_designation_date(date_str):
    """Parsea la fecha de designación 'Month Day, Year'."""
    try:
        # Handle potential inconsistencies like extra spaces
        clean_date_str = re.sub(r'\s+', ' ', date_str).strip()
        dt_object = datetime.strptime(clean_date_str, '%B %d, %Y')

        # Sanity check for future dates (might indicate issues with source data)
        # Allow dates up to today + 1 day buffer just in case
        if dt_object.date() > datetime.now().date() + pd.Timedelta(days=1):
             logging.warning(f"Fecha de designación futura detectada: {date_str}. Se usará igualmente.")
             # Decide action: reject, log warning, use it anyway? Using it for now.

        return dt_object.date()
    except ValueError:
        logging.warning(f"Formato de fecha de designación no reconocido: '{date_str}'. Se dejará como None.")
        return None
    except Exception as e:
        logging.error(f"Error inesperado parseando fecha '{date_str}': {e}")
        return None

def parse_name_and_details(cell_content):
    """Extrae el nombre principal y los alias/enmiendas de la celda."""
    lines = [line.strip() for line in cell_content.split('\n') if line.strip()]
    if not lines:
        return None, None

    main_name = lines[0]
    aliases_amendments = []

    # Extract details from subsequent lines or within the first line
    # Pattern for aka, formerly, Amendments
    patterns = [
        r'\((?:aka|formerly)\s+(.*?)\)', # (aka ...) or (formerly ...)
        r'—\s*(.*?)\s+Amendment\s+\((.*?)\)', # — Amendment Name Amendment (Date)
        r'\((.*?)\s+Amendment\)\s*$', # (Amendment Name Amendment) at the end
    ]

    # Clean main name from initial patterns if present
    for pattern in patterns:
        match = re.search(pattern, main_name, re.IGNORECASE)
        if match:
             # Add extracted alias/amendment info
             if len(match.groups()) > 1:
                 aliases_amendments.append(f"{match.group(1)} (Amendment {match.group(2)})")
             else:
                 aliases_amendments.append(match.group(1))
             # Remove the matched part from the main name
             main_name = main_name.replace(match.group(0), '').strip()

    # Process remaining lines as aliases/amendments
    if len(lines) > 1:
        aliases_amendments.extend(lines[1:])

    # Also check for patterns within the remaining lines
    processed_aliases = []
    for line in aliases_amendments:
         handled = False
         for pattern in patterns:
             match = re.search(pattern, line, re.IGNORECASE)
             if match:
                 if len(match.groups()) > 1:
                     processed_aliases.append(f"{match.group(1)} (Amendment {match.group(2)})")
                 else:
                      processed_aliases.append(match.group(1))
                 # Optionally remove the matched part if the line contains more text?
                 # line = line.replace(match.group(0), '').strip() # Decided against this for now
                 handled = True
                 break # Assume one pattern match per line is enough
         if not handled:
            # If no specific pattern matched, add the line as is (might be an alias)
            processed_aliases.append(line)


    # Clean up the main name (remove trailing commas, etc.)
    main_name = main_name.strip().rstrip(',')

    # Join aliases/amendments with a separator
    aliases_str = '; '.join(filter(None, processed_aliases)) if processed_aliases else None

    return main_name, aliases_str

def parse_html(html_content):
    """Parsea el HTML para extraer la lista de FTO designadas."""
    soup = BeautifulSoup(html_content, 'html.parser')
    entries = []
    current_time = datetime.now()

    # Encontrar el encabezado de la tabla de designados
    designated_header = soup.find('h2', string=re.compile(r'Designated Foreign Terrorist Organizations', re.IGNORECASE))
    if not designated_header:
        logging.error("No se encontró el encabezado 'Designated Foreign Terrorist Organizations'.")
        return None

    # Encontrar la tabla que sigue al encabezado
    table_figure = designated_header.find_next_sibling('figure', class_='wp-block-table')
    if not table_figure:
         table_figure = designated_header.find_next_sibling('table') # Fallback if no figure wrapper
    if not table_figure:
        logging.error("No se encontró la tabla después del encabezado 'Designated Foreign Terrorist Organizations'.")
        return None

    table = table_figure.find('table')
    if not table:
         table = table_figure # If the sibling was the table itself
    if not table or not hasattr(table, 'find'): # Check if table object is valid
         logging.error("No se encontró el elemento table dentro de la figura.")
         return None

    tbody = table.find('tbody')
    if not tbody:
        logging.error("No se encontró el cuerpo (tbody) de la tabla de designados.")
        return None

    rows = tbody.find_all('tr')
    logging.info(f"Encontradas {len(rows)} filas en la tabla de designados.")

    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 2:
            designation_date_str = cols[0].get_text(strip=True)
            name_cell_content = cols[1].get_text(separator='\n', strip=True) # Use newline separator for <br>

            designation_date = parse_designation_date(designation_date_str)
            name, aliases = parse_name_and_details(name_cell_content)

            if name: # Only add if we could parse a name
                entry = {
                    'source_list': SOURCE_LIST_NAME,
                    'entry_type': 'Entity',
                    'name': name,
                    'aliases': aliases,
                    'nationality': None, # FTO list doesn't provide nationality
                    'date_of_birth': None, # Not applicable
                    'place_of_birth': None, # Not applicable
                    'identification_details': None, # Not applicable from this list
                    'other_details': f"Designated: {designation_date_str}", # Store original date string here
                    'designation_date': designation_date, # Store parsed date separately if needed
                    'raw_data': f"Date: {designation_date_str}, Details: {name_cell_content}",
                    'load_timestamp': current_time
                }
                entries.append(entry)
            else:
                 logging.warning(f"No se pudo extraer nombre de la fila con fecha: {designation_date_str}")
        else:
            logging.warning(f"Fila con número inesperado de columnas encontrada: {len(cols)}")


    if not entries:
         logging.warning("No se extrajo ninguna entrada FTO de la lista.")
         # Decide if this is an error or just an empty list scenario
         # Return [] to indicate empty list, None for error finding table/header
         return [] # Return empty list, main function will handle hash comparison

    logging.info(f"Se extrajeron {len(entries)} entradas FTO designadas.")
    return entries


def format_data_for_pandas(parsed_data):
    """Convierte los datos parseados a un DataFrame de Pandas."""
    if not parsed_data:
        return pd.DataFrame(), "vacio" # Devuelve DataFrame vacío y string 'vacio'

    df = pd.DataFrame(parsed_data)

    # Columnas relevantes para la tabla general 'sanctions_list'
    # O adaptar si se usa una tabla específica como 'state_gov_fto'
    general_columns = [
        'source_list', 'entry_type', 'name', 'aliases', 'nationality',
        'date_of_birth', 'place_of_birth', 'identification_details',
        'other_details', 'raw_data', 'last_update_date'
        # 'designation_date' # Podría añadirse a la tabla general o mantenerse en 'other_details'
    ]

    # Asegurar que todas las columnas esperadas existan
    for col in general_columns:
        if col not in df.columns:
            df[col] = None # O pd.NA

    # Añadir la columna designation_date si no está, extrayéndola de la entrada parseada
    if 'designation_date' not in df.columns and 'designation_date' in parsed_data[0]:
         df['designation_date'] = [entry.get('designation_date') for entry in parsed_data]


    # Seleccionar y reordenar columnas para la tabla general
    df = df[general_columns + ['designation_date']] # Añadir fecha de designación

    # Convertir tipos de datos
    df['last_update_date'] = pd.to_datetime(df['last_update_date'])
    df['designation_date'] = pd.to_datetime(df['designation_date'], errors='coerce').dt.date
    df['designation_date'] = df['designation_date'].astype(object).where(pd.notnull(df['designation_date']), None)

    # Crear string canónico para hashing (ordenando por nombre para consistencia)
    df_sorted = df.sort_values(by='name').reset_index(drop=True)
    # Convertir todo a string excepto fechas/timestamps que usan formato ISO
    df_string = df_sorted.astype(str).to_json(orient='records', date_format='iso')

    return df, df_string # Devolver DataFrame original y string para hash

def connect_db(params):
    """Conecta a la base de datos PostgreSQL."""
    conn = None
    try:
        logging.info("Conectando a la base de datos PostgreSQL...")
        conn = psycopg2.connect(**params)
        logging.info("Conexión exitosa.")
    except psycopg2.Error as e:
        logging.error(f"Error conectando a PostgreSQL: {e}")
    return conn

def load_to_postgres(df, table_name, db_conn):
    """Carga el DataFrame en la tabla de PostgreSQL, reemplazando datos antiguos para esta lista."""
    if df.empty:
        logging.warning("DataFrame vacío, no se cargará nada en la base de datos.")
        return False # Considerar si debería borrar registros antiguos si el DF está vacío

    cursor = None
    try:
        cursor = db_conn.cursor()

        # 1. Borrar registros antiguos de ESTA lista específica
        delete_query = f"DELETE FROM {table_name} WHERE source_list = %s;"
        logging.info(f"Borrando registros antiguos para '{SOURCE_LIST_NAME}' de la tabla '{table_name}'...")
        cursor.execute(delete_query, (SOURCE_LIST_NAME,))
        logging.info(f"{cursor.rowcount} registros antiguos eliminados.")

        # 2. Insertar nuevos registros
        logging.info(f"Insertando {len(df)} nuevos registros en la tabla '{table_name}'...")

        # Columnas para la tabla 'sanctions_list'
        # Asegurarse de que coincidan con la estructura de la tabla y el DataFrame
        # Incluir 'designation_date' si la tabla la tiene
        cols_in_db = [
            'source_list', 'entry_type', 'name', 'aliases', 'nationality',
            'date_of_birth', 'place_of_birth', 'identification_details',
            'other_details', 'raw_data', 'last_update_date'
            #, 'designation_date' # Descomentar si la columna existe en la tabla
        ]
        # Filtrar el DataFrame para incluir solo estas columnas en el orden correcto
        df_to_insert = df[[col for col in cols_in_db if col in df.columns]]

        # Preparar datos para inserción (lista de tuplas)
        data_tuples = [tuple(row) for row in df_to_insert.where(pd.notnull(df_to_insert), None).values]

        # Crear la lista de nombres de columnas para la query SQL dinámicamente
        insert_cols_str = ", ".join(df_to_insert.columns)

        insert_query = f"""
            INSERT INTO {table_name} ({insert_cols_str})
            VALUES %s;
        """
        # Usar execute_values para inserción eficiente
        execute_values(cursor, insert_query, data_tuples, page_size=500) # page_size puede ajustarse

        db_conn.commit()
        logging.info(f"{len(df)} registros insertados correctamente.")
        return True

    except (Exception, psycopg2.Error) as e:
        logging.error(f"Error durante la carga a PostgreSQL: {e}")
        if db_conn:
            db_conn.rollback() # Revertir transacción en caso de error
        return False
    finally:
        if cursor:
            cursor.close()

# --- Flujo Principal (Reutilizado) ---
def main():
    logging.info(f"--- Iniciando Script de Consulta Lista FTO State Dept ({SOURCE_LIST_NAME}) ---")

    # 1. Obtener HTML
    html = get_html_content()
    if not html:
        logging.error("Finalizando script debido a error obteniendo HTML.")
        return

    # 2. Parsear HTML
    parsed_data = parse_html(html)
    if parsed_data is None: # Error durante parseo
        logging.error("Finalizando script debido a error de parseo.")
        return
    if not parsed_data: # Lista legítimamente vacía
        logging.warning("El parseo no encontró entradas FTO designadas, verificando cambios de hash con 'vacio'.")
        current_data_string = "vacio"
        df = pd.DataFrame() # Asegurar que el DataFrame esté vacío
    else:
        # 3. Formatear datos y obtener string para hash
        df, current_data_string = format_data_for_pandas(parsed_data)
        if df.empty and parsed_data: # Error en formateo
             logging.error("Error al formatear datos en DataFrame, finalizando.")
             return

    # 4. Calcular hash actual
    current_hash = calculate_hash(current_data_string)
    logging.info(f"Hash actual calculado: {current_hash}")

    # 5. Comparar con hash anterior
    last_hash = read_last_hash(HASH_FILE)
    logging.info(f"Último hash leído: {last_hash}")

    if current_hash == last_hash:
        logging.info("No se detectaron cambios en la lista FTO. No se requiere actualización.")
    else:
        logging.info("Cambios detectados en la lista FTO. Procediendo a actualizar la base de datos.")

        # 6. Conectar a la BD
        conn = connect_db(DB_PARAMS)
        if not conn:
            logging.error("No se pudo conectar a la base de datos. Finalizando.")
            return

        # 7. Cargar datos a PostgreSQL
        # Manejar caso donde la lista ahora está vacía
        if df.empty:
             logging.info(f"La lista {SOURCE_LIST_NAME} actual está vacía. Borrando registros anteriores...")
             cursor = conn.cursor()
             try:
                 cursor.execute(f"DELETE FROM {DB_TABLE_NAME} WHERE source_list = %s;", (SOURCE_LIST_NAME,))
                 conn.commit()
                 logging.info(f"Registros para {SOURCE_LIST_NAME} eliminados.")
                 # Guardar el nuevo hash (de la lista vacía)
                 write_current_hash(HASH_FILE, current_hash)
             except (Exception, psycopg2.Error) as e:
                 logging.error(f"Error borrando registros de lista vacía para {SOURCE_LIST_NAME}: {e}")
                 conn.rollback()
             finally:
                 if cursor: cursor.close()
        # Cargar datos si el DataFrame no está vacío
        elif load_to_postgres(df, DB_TABLE_NAME, conn):
            # 8. Si la carga fue exitosa, guardar el nuevo hash
            write_current_hash(HASH_FILE, current_hash)
        else:
            logging.error(f"La carga a la base de datos para {SOURCE_LIST_NAME} falló. No se actualizará el hash.")

        # Cerrar conexión
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")

    logging.info(f"--- Script {SOURCE_LIST_NAME} Finalizado ---")

if __name__ == "__main__":
    main()