import requests
import xml.etree.ElementTree as ET
import pandas as pd
import hashlib
import logging
import os
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import re
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

# --- Configuración ---
SOURCE_URL = "https://international.gc.ca/world-monde/assets/office_docs/international_relations-relations_internationales/sanctions/sema-lmes.xml"
LOG_FILE = 'canada_sema_status.log'
HASH_FILE = 'canada_sema.xml.hash' # Hash the raw XML file content

# TODO: Usar variables de entorno o AWS Secrets Manager en producción
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST, # ej. 'xxxxx.rds.amazonaws.com'
    "port": DB_PORT
}

# Nueva tabla específica para Canada SEMA
DB_TABLE_NAME = "sanctions_list_ca"
SOURCE_LIST_NAME = "Canada_SEMA" # Para logging

# Configuración de Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

# --- Funciones Auxiliares ---

def get_xml_content():
    """Obtiene el contenido XML desde la URL."""
    logging.info(f"Consultando URL: {SOURCE_URL}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(SOURCE_URL, headers=headers, timeout=60)
        response.raise_for_status()
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

def safe_findtext(element, tag_name, default=''):
    """Encuentra texto en un subelemento directo, manejando si no existe."""
    # No se necesita prefijo de namespace basado en el XML completo
    found = element.find(tag_name)
    if found is not None and found.text:
        # Limpiar espacios extraños (como &nbsp; o múltiples espacios)
        text = re.sub(r'\s+', ' ', found.text).strip()
        return text
    return default

def parse_listing_date(date_str):
    """Parsea la fecha de listado (YYYY-MM-DD)."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        logging.warning(f"Formato de fecha de listado no reconocido: '{date_str}'. Se dejará como None.")
        return None

# --- Funciones Principales Modificadas ---

def parse_xml(xml_content_bytes):
    """Parsea el contenido XML de Canada SEMA."""
    entries = []
    current_time = datetime.now()
    try:
        try:
            xml_content_str = xml_content_bytes.decode('utf-8')
        except UnicodeDecodeError:
             logging.warning("Fallo al decodificar XML como UTF-8, intentando Latin-1")
             xml_content_str = xml_content_bytes.decode('latin-1') # O ISO-8859-1

        root = ET.fromstring(xml_content_str)
        # No se necesita namespace según el archivo completo

        records = root.findall('record')
        logging.info(f"Encontrados {len(records)} registros.")

        for record in records:
            entry_type = None
            primary_name = None
            last_name = None
            given_name = None
            entity_or_ship_name = None

            # Determinar tipo y nombre principal
            entity_name_raw = safe_findtext(record, 'EntityOrShip')
            last_name_raw = safe_findtext(record, 'LastName')
            given_name_raw = safe_findtext(record, 'GivenName')

            if entity_name_raw:
                entry_type = 'Entity/Ship' # Podría refinarse si 'TitleOrShip' da más info
                primary_name = entity_name_raw
                entity_or_ship_name = entity_name_raw # Guardar el original
            elif last_name_raw:
                entry_type = 'Individual'
                primary_name = f"{last_name_raw}, {given_name_raw}".strip().rstrip(',')
                last_name = last_name_raw
                given_name = given_name_raw
            else:
                logging.warning(f"Registro sin LastName ni EntityOrShip encontrado. Item: {safe_findtext(record, 'Item')}")
                continue # Saltar este registro si no tiene nombre identificable

            # Extraer otros campos comunes
            country = safe_findtext(record, 'Country')
            schedule = safe_findtext(record, 'Schedule')
            item = safe_findtext(record, 'Item')
            date_of_listing_str = safe_findtext(record, 'DateOfListing')
            dob_or_build_date = safe_findtext(record, 'DateOfBirthOrShipBuildDate')
            aliases = safe_findtext(record, 'Aliases')
            title = safe_findtext(record, 'TitleOrShip') # Puede aplicar a individuos o barcos
            imo_number = safe_findtext(record, 'ShipIMONumber')

            # Si es Entity/Ship y tiene IMO, marcar como 'Ship'? (Opcional, depende de la necesidad)
            # if entry_type == 'Entity/Ship' and imo_number:
            #     entry_type = 'Ship'

            listing_date = parse_listing_date(date_of_listing_str)

            # raw_xml_record_str = ET.tostring(record, encoding='unicode') # Optional

            parsed_entry = {
                'entry_type': entry_type,
                'country_program': country if country else None,
                'primary_name': primary_name,
                'last_name': last_name,
                'given_name': given_name,
                'entity_or_ship_name': entity_or_ship_name,
                'aliases': aliases if aliases else None,
                'title': title if title else None,
                'dob_or_build_date': dob_or_build_date if dob_or_build_date else None,
                'schedule': schedule if schedule else None,
                'item': item if item else None,
                'listing_date': listing_date,
                'imo_number': imo_number if imo_number else None,
                # 'raw_xml_record': raw_xml_record_str, # Optional
                'load_timestamp': current_time
            }
            entries.append(parsed_entry)

        logging.info(f"Parseados {len(entries)} registros SEMA válidos con éxito.")
        return entries

    except ET.ParseError as e:
        logging.error(f"Error parseando XML: {e}. Contenido inicial: {xml_content_bytes[:500]}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado durante el parseo XML: {e}")
        return None

def format_data_for_pandas(parsed_data):
    """Convierte los datos parseados a un DataFrame de Pandas."""
    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)

    # Asegurar que todas las columnas de la tabla existan en el DF
    expected_columns = [
        'entry_type', 'country_program', 'primary_name', 'last_name', 'given_name',
        'entity_or_ship_name', 'aliases', 'title', 'dob_or_build_date',
        'schedule', 'item', 'listing_date', 'imo_number',
        # 'raw_xml_record',
        'load_timestamp'
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Reordenar y seleccionar columnas
    df = df[expected_columns]

    # Convertir tipos de datos
    df['load_timestamp'] = pd.to_datetime(df['load_timestamp'])
    df['listing_date'] = pd.to_datetime(df['listing_date'], errors='coerce').dt.date

    # Reemplazar NaN/NaT/None por None (NULL en BD)
    df = df.astype(object).where(pd.notnull(df), None)

    # Podríamos añadir un ID único combinando Item y Schedule si no hay otro UID
    # df['unique_id'] = df['schedule'].astype(str) + "_" + df['item'].astype(str)
    # df.drop_duplicates(subset=['unique_id'], keep='first', inplace=True) # Si se añade unique_id

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

def create_canada_table_if_not_exists(db_conn, table_name):
    """Crea la tabla Canada SEMA si no existe."""
    # Usar el SQL actualizado con todas las columnas nuevas
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        entry_type VARCHAR(50),
        country_program VARCHAR(255),
        primary_name TEXT NOT NULL,
        last_name TEXT,
        given_name TEXT,
        entity_or_ship_name TEXT,
        aliases TEXT,
        title TEXT,
        dob_or_build_date TEXT,
        schedule VARCHAR(100),
        item VARCHAR(20),
        listing_date DATE,
        imo_number VARCHAR(50),
        -- raw_xml_record TEXT, -- Optional
        load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    -- Add indexes
    CREATE INDEX IF NOT EXISTS idx_{table_name}_entry_type ON {table_name} (entry_type);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name} (primary_name);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_country ON {table_name} (country_program);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_listing_date ON {table_name} (listing_date);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_item ON {table_name} (item);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_schedule ON {table_name} (schedule);
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
    """Carga el DataFrame en la tabla Canada SEMA, reemplazando datos antiguos."""
    if df.empty and parsed_data is None: # Chequear si el parseo falló
         logging.warning("DataFrame vacío debido a error de parseo. No se truncará ni cargará.")
         return False

    cursor = None
    try:
        cursor = db_conn.cursor()

        logging.info(f"Truncando tabla '{table_name}' antes de la carga...")
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
        logging.info(f"Tabla '{table_name}' truncada.")

        if df.empty: # Si el parseo fue ok pero no encontró datos, o si la lista está vacía
            logging.info("DataFrame vacío, la tabla ha sido truncada. Carga completada (tabla vacía).")
            db_conn.commit()
            return True

        logging.info(f"Insertando {len(df)} nuevos registros en la tabla '{table_name}'...")

        # Asegurarse que las columnas del DF coinciden con la tabla
        table_columns = [
            'entry_type', 'country_program', 'primary_name', 'last_name', 'given_name',
            'entity_or_ship_name', 'aliases', 'title', 'dob_or_build_date',
            'schedule', 'item', 'listing_date', 'imo_number', 'load_timestamp'
        ]
        df_to_insert = df[table_columns] # Seleccionar y ordenar columnas

        cols_str = ", ".join([f'"{c}"' for c in table_columns]) # Usar nombres de columna exactos
        # Convertir DF a lista de tuplas, reemplazando NaN/NaT por None
        data_tuples = [tuple(row) for row in df_to_insert.where(pd.notnull(df_to_insert), None).values]

        insert_query = f"""
            INSERT INTO {table_name} ({cols_str})
            VALUES %s;
        """
        execute_values(cursor, insert_query, data_tuples, page_size=1000)

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
# Variable global para rastrear el estado del parseo
parsed_data = None

def main():
    global parsed_data # Permitir modificar la variable global
    logging.info(f"--- Iniciando Script de Consulta Lista {SOURCE_LIST_NAME} ---")

    xml_bytes = get_xml_content()
    if not xml_bytes:
        logging.error("Finalizando script debido a error obteniendo XML.")
        return

    current_hash = calculate_hash(xml_bytes)
    logging.info(f"Hash actual del XML: {current_hash}")

    last_hash = read_last_hash(HASH_FILE)
    logging.info(f"Último hash leído: {last_hash}")

    if current_hash == last_hash:
        logging.info(f"No se detectaron cambios en el archivo XML de {SOURCE_LIST_NAME}. No se requiere actualización.")
    else:
        logging.info(f"Cambios detectados en el archivo XML de {SOURCE_LIST_NAME}. Procediendo a parsear y actualizar la base de datos.")

        parsed_data = parse_xml(xml_bytes) # Almacenar resultado globalmente
        if parsed_data is None:
            logging.error("Error parseando el XML. No se actualizará la base de datos ni el hash.")
            return # Salir si el parseo falló

        df = format_data_for_pandas(parsed_data)
        # El DataFrame vacío es válido si parsed_data no es None (lista vacía encontrada)

        conn = connect_db(DB_PARAMS)
        if not conn:
            logging.error("No se pudo conectar a la base de datos. Finalizando.")
            return

        try:
            create_canada_table_if_not_exists(conn, DB_TABLE_NAME)

            # La función load_to_postgres ahora maneja df vacío si parsed_data no es None
            if load_to_postgres(df, DB_TABLE_NAME, conn):
                # Guardar nuevo hash solo si la carga fue exitosa (o si la tabla fue truncada exitosamente porque el df estaba vacío)
                write_current_hash(HASH_FILE, current_hash)
            else:
                # No actualizar el hash si la carga falla
                logging.error(f"La carga a la base de datos ({DB_TABLE_NAME}) falló. No se actualizará el hash.")
        finally:
            if conn:
                conn.close()
                logging.info("Conexión a la base de datos cerrada.")

    logging.info(f"--- Script {SOURCE_LIST_NAME} Finalizado ---")

if __name__ == "__main__":
    main()