import requests
import pandas as pd
import hashlib
import logging
import os
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import re
import io # Para leer bytes como archivo para pandas
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

# --- Configuración ---
SOURCE_URL = "https://www.dfat.gov.au/sites/default/files/regulation8_consolidated.xlsx"
LOG_FILE = 'dfat_australia_status.log'
HASH_FILE = 'dfat_australia.xlsx.hash' # Hash del archivo XLSX crudo

# TODO: Usar variables de entorno o AWS Secrets Manager en producción
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST, # ej. 'xxxxx.rds.amazonaws.com'
    "port": DB_PORT
}

# Nueva tabla específica
DB_TABLE_NAME = "sanctions_list_au"
SOURCE_LIST_NAME = "DFAT_Australia" # Para logging

# Configuración de Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

# --- Funciones Auxiliares ---

def get_xlsx_content():
    """Obtiene el contenido del archivo XLSX desde la URL como bytes."""
    logging.info(f"Consultando URL: {SOURCE_URL}")
    try:
        headers = { # Simular un navegador común
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(SOURCE_URL, headers=headers, timeout=90) # Timeout más largo para archivos
        response.raise_for_status()
        # Devolver el contenido binario del archivo
        return response.content
    except requests.exceptions.Timeout:
         logging.error(f"Timeout al obtener la URL {SOURCE_URL}")
         return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al obtener la URL {SOURCE_URL}: {e}")
        return None

def calculate_hash(data_bytes):
    """Calcula el hash MD5 de bytes."""
    return hashlib.md5(data_bytes).hexdigest()

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

def parse_control_date(date_str):
    """Parsea la fecha de control (DD/MM/YYYY)."""
    if pd.isna(date_str) or date_str is None: # Check for Pandas NaT/NaN or None
        return None
    # Si la fecha ya es un objeto datetime (pandas a veces lo hace)
    if isinstance(date_str, (datetime, pd.Timestamp)):
        return date_str.date()
    # Si es string
    try:
        # Intentar formato DD/MM/YYYY
        return datetime.strptime(str(date_str).split(' ')[0], '%d/%m/%Y').date()
    except (ValueError, TypeError):
         # Podría haber otros formatos o texto, intentar YYYY-MM-DD como fallback
        try:
             return datetime.strptime(str(date_str).split(' ')[0], '%Y-%m-%d').date()
        except (ValueError, TypeError):
            logging.warning(f"Formato de fecha de control no reconocido: '{date_str}'. Se dejará como None.")
            return None

def extract_base_ref(ref_val):
    """Extrae el número base de la referencia (ej: '2' de '2a')."""
    if pd.isna(ref_val):
        return None
    ref_str = str(ref_val)
    match = re.match(r'^(\d+)', ref_str)
    return match.group(1) if match else ref_str # Devolver el string original si no hay número al inicio


# --- Funciones Principales ---

def process_xlsx(xlsx_bytes):
    """Lee el XLSX, procesa y consolida los datos."""
    consolidated_entries = []
    current_time = datetime.now()
    try:
        # Leer el archivo XLSX desde los bytes en memoria
        # Asumir que la primera hoja es la relevante (index=0)
        # Podría necesitar 'engine='openpyxl'' si pandas no lo maneja por defecto
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=0, engine='openpyxl')
        logging.info(f"Leído XLSX. Columnas encontradas: {df.columns.tolist()}")

        # Limpiar nombres de columnas (quitar espacios extra, convertir a minúsculas para manejo fácil)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        logging.info(f"Columnas normalizadas: {df.columns.tolist()}")

        # Verificar que las columnas esperadas existan (después de normalizar)
        required_cols = ['reference', 'name_of_individual_or_entity', 'type', 'name_type']
        if not all(col in df.columns for col in required_cols):
            logging.error(f"Faltan columnas requeridas en el XLSX. Esperadas (normalizadas): {required_cols}. Encontradas: {df.columns.tolist()}")
            return None

        # Extraer el número base de la referencia para agrupar
        df['base_reference'] = df['reference'].apply(extract_base_ref)

        # Agrupar por la referencia base
        grouped = df.groupby('base_reference')
        logging.info(f"Procesando {len(grouped)} grupos basados en 'base_reference'.")

        for base_ref, group in grouped:
            if base_ref is None:
                logging.warning("Se encontró un grupo con referencia base None, se ignora.")
                continue

            primary_row = group[group['name_type'].str.strip().str.lower() == 'primary name']
            aka_rows = group[group['name_type'].str.strip().str.lower() == 'aka']
            orig_script_rows = group[group['name_type'].str.strip().str.lower() == 'original script']

            if primary_row.empty:
                logging.warning(f"No se encontró fila 'Primary Name' para la referencia base '{base_ref}'. Grupo ignorado.")
                continue

            # Tomar la primera fila 'Primary Name' si hay duplicados (no debería ocurrir)
            primary_data = primary_row.iloc[0]

            # Extraer datos principales
            entry_type = primary_data.get('type', '').strip()
            primary_name = primary_data.get('name_of_individual_or_entity', '').strip()
            dob = str(primary_data.get('date_of_birth', '')) # Guardar como texto
            pob = str(primary_data.get('place_of_birth', ''))
            citizenship = str(primary_data.get('citizenship', ''))
            address = str(primary_data.get('address', ''))
            additional_info = str(primary_data.get('additional_information', ''))
            listing_info = str(primary_data.get('listing_information', ''))
            committees = str(primary_data.get('committees', ''))
            control_date_raw = primary_data.get('control_date')

            # Consolidar alias y nombres originales
            aliases = "; ".join(filter(None, aka_rows['name_of_individual_or_entity'].astype(str).str.strip()))
            orig_scripts = "; ".join(filter(None, orig_script_rows['name_of_individual_or_entity'].astype(str).str.strip()))

            # Parsear fecha de control
            control_date = parse_control_date(control_date_raw)

            consolidated_entry = {
                'reference': base_ref, # Guardar el número base como referencia única
                'entry_type': entry_type if entry_type else None,
                'primary_name': primary_name if primary_name else None,
                'aliases': aliases if aliases else None,
                'original_script_names': orig_scripts if orig_scripts else None,
                'date_of_birth': dob if pd.notna(dob) and dob else None,
                'place_of_birth': pob if pd.notna(pob) and pob else None,
                'citizenship': citizenship if pd.notna(citizenship) and citizenship else None,
                'address': address if pd.notna(address) and address else None,
                'additional_information': additional_info if pd.notna(additional_info) and additional_info else None,
                'listing_information': listing_info if pd.notna(listing_info) and listing_info else None,
                'committees': committees if pd.notna(committees) and committees else None,
                'control_date': control_date,
                'load_timestamp': current_time
            }

            # Solo añadir si hay nombre primario
            if consolidated_entry['primary_name']:
                 consolidated_entries.append(consolidated_entry)
            else:
                 logging.warning(f"Entrada consolidada para ref '{base_ref}' no tiene nombre primario, descartada.")


        logging.info(f"Procesadas {len(consolidated_entries)} entradas consolidadas.")
        return consolidated_entries

    except FileNotFoundError: # Si pandas no puede leer el archivo (aunque usamos BytesIO)
        logging.error("Error procesando XLSX: Archivo no encontrado (inesperado con BytesIO).")
        return None
    except ImportError:
        logging.error("Error procesando XLSX: Falta la biblioteca 'openpyxl'. Instálala con 'pip install openpyxl'")
        return None
    except Exception as e:
        logging.error(f"Error inesperado procesando el archivo XLSX: {e}")
        # Considerar loggear el traceback: logging.exception("Error procesando XLSX")
        return None

def format_data_for_pandas(parsed_data):
    """Convierte la lista de diccionarios consolidados a un DataFrame."""
    # La función process_xlsx ya devuelve la lista lista para el DF
    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)

    # Asegurar que todas las columnas esperadas existan
    expected_columns = [
        'reference', 'entry_type', 'primary_name', 'aliases', 'original_script_names',
        'date_of_birth', 'place_of_birth', 'citizenship', 'address',
        'additional_information', 'listing_information', 'committees',
        'control_date', 'load_timestamp'
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Reordenar y seleccionar columnas
    df = df[expected_columns]

    # Convertir tipos (fechas ya deberían estar como objetos date o None)
    df['load_timestamp'] = pd.to_datetime(df['load_timestamp'])
    df['control_date'] = pd.to_datetime(df['control_date'], errors='coerce').dt.date

    # Reemplazar cualquier NaN restante por None (aunque el proceso ya debería manejarlo)
    df = df.astype(object).where(pd.notnull(df), None)

    return df

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

def create_dfat_table_if_not_exists(db_conn, table_name):
    """Crea la tabla DFAT Australia si no existe."""
    # Usar SQL actualizado
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        reference VARCHAR(50) NOT NULL UNIQUE,
        entry_type VARCHAR(50),
        primary_name TEXT NOT NULL,
        aliases TEXT,
        original_script_names TEXT,
        date_of_birth TEXT,
        place_of_birth TEXT,
        citizenship TEXT,
        address TEXT,
        additional_information TEXT,
        listing_information TEXT,
        committees TEXT,
        control_date DATE,
        load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    -- Índices
    CREATE INDEX IF NOT EXISTS idx_{table_name}_reference ON {table_name} (reference);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name} (primary_name);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_type ON {table_name} (entry_type);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_control_date ON {table_name} (control_date);
    """
    cursor = None
    try:
        cursor = db_conn.cursor()
        logging.info(f"Verificando/creando tabla {table_name}...")
        cursor.execute(create_table_query)
        db_conn.commit()
        logging.info(f"Tabla {table_name} lista.")
    except (Exception, psycopg2.Error) as e:
        logging.error(f"Error creando/verificando tabla {table_name}: {e}")
        if db_conn: db_conn.rollback()
    finally:
        if cursor: cursor.close()

def load_to_postgres(df, table_name, db_conn):
    """Carga el DataFrame en la tabla DFAT, reemplazando datos antiguos."""
    if df.empty and processed_data is None: # Si el procesamiento falló
        logging.warning("DataFrame vacío debido a error de procesamiento XLSX. No se truncará ni cargará.")
        return False

    cursor = None
    try:
        cursor = db_conn.cursor()

        logging.info(f"Truncando tabla '{table_name}' antes de la carga...")
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
        logging.info(f"Tabla '{table_name}' truncada.")

        if df.empty: # Procesamiento OK pero sin datos o lista vacía
            logging.info("DataFrame vacío, la tabla ha sido truncada. Carga completada (tabla vacía).")
            db_conn.commit()
            return True

        logging.info(f"Insertando {len(df)} nuevos registros en la tabla '{table_name}'...")

        table_columns = [ # Columnas en el orden de la tabla SQL
            'reference', 'entry_type', 'primary_name', 'aliases', 'original_script_names',
            'date_of_birth', 'place_of_birth', 'citizenship', 'address',
            'additional_information', 'listing_information', 'committees',
            'control_date', 'load_timestamp'
        ]
        df_to_insert = df[table_columns] # Asegurar orden

        cols_str = ", ".join([f'"{c}"' for c in table_columns])
        # Convertir DF a lista de tuplas, reemplazando NaN/NaT/None por None
        data_tuples = [tuple(row) for row in df_to_insert.astype(object).where(pd.notnull(df_to_insert), None).values]


        insert_query = f"""
            INSERT INTO {table_name} ({cols_str})
            VALUES %s;
        """
        execute_values(cursor, insert_query, data_tuples, page_size=500)

        db_conn.commit()
        logging.info(f"{len(df)} registros insertados correctamente en '{table_name}'.")
        return True

    except (Exception, psycopg2.Error) as e:
        logging.error(f"Error durante la carga a PostgreSQL ({table_name}): {e}")
        if db_conn:
            db_conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()

# --- Flujo Principal ---
# Variable global para estado de procesamiento
processed_data = None

def main():
    global processed_data
    logging.info(f"--- Iniciando Script de Consulta Lista {SOURCE_LIST_NAME} ---")

    xlsx_bytes = get_xlsx_content()
    if not xlsx_bytes:
        logging.error("Finalizando script debido a error obteniendo XLSX.")
        return

    current_hash = calculate_hash(xlsx_bytes)
    logging.info(f"Hash actual del XLSX: {current_hash}")

    last_hash = read_last_hash(HASH_FILE)
    logging.info(f"Último hash leído: {last_hash}")

    if current_hash == last_hash:
        logging.info(f"No se detectaron cambios en el archivo XLSX de {SOURCE_LIST_NAME}. No se requiere actualización.")
    else:
        logging.info(f"Cambios detectados en el archivo XLSX de {SOURCE_LIST_NAME}. Procediendo a procesar y actualizar la base de datos.")

        processed_data = process_xlsx(xlsx_bytes) # Almacenar resultado
        if processed_data is None: # Error durante el procesamiento
            logging.error("Error procesando el XLSX. No se actualizará la base de datos ni el hash.")
            return

        df = format_data_for_pandas(processed_data)
        # DF vacío es válido si processed_data no es None

        conn = connect_db(DB_PARAMS)
        if not conn:
            logging.error("No se pudo conectar a la base de datos. Finalizando.")
            return

        try:
            create_dfat_table_if_not_exists(conn, DB_TABLE_NAME)

            # Pasar df y estado de processed_data a load_to_postgres
            if load_to_postgres(df, DB_TABLE_NAME, conn):
                # Guardar hash solo si la carga/truncate fue exitosa
                write_current_hash(HASH_FILE, current_hash)
            else:
                logging.error(f"La carga a la base de datos ({DB_TABLE_NAME}) falló. No se actualizará el hash.")
        finally:
            if conn:
                conn.close()
                logging.info("Conexión a la base de datos cerrada.")

    logging.info(f"--- Script {SOURCE_LIST_NAME} Finalizado ---")

if __name__ == "__main__":
    main()