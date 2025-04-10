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
SOURCE_URL = "https://scsanctions.un.org/resources/xml/sp/consolidated.xml"
LOG_FILE = 'un_sanctions_status.log'
HASH_FILE = 'un_sanctions.xml.hash'  # Archivo para guardar el hash del XML

# Parámetros de conexión a la BD (ajustar según se requiera)
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT
}

# Nombre de la nueva tabla para la lista UN
DB_TABLE_NAME = "sanctions_list_un"
SOURCE_LIST_NAME = "UN_Sanctions"

# Configuración de Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

# --- Funciones Auxiliares ---
def get_xml_content():
    """Obtiene el contenido XML desde la URL."""
    logging.info(f"Consultando URL: {SOURCE_URL}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 Python UN Sanctions Scraper'}
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
    """Calcula el hash MD5 de los bytes."""
    return hashlib.md5(data_bytes).hexdigest()

def read_last_hash(file_path):
    """Lee el último hash guardado en el archivo."""
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

def safe_findtext(element, path, default=''):
    """Busca de manera segura el texto en el elemento XML."""
    found = element.find(path)
    if found is not None and found.text:
        return found.text.strip()
    return default

def clean_date(date_str):
    """
    Extrae la parte de la fecha en formato YYYY-MM-DD utilizando una expresión regular.
    Si no se encuentra, retorna el valor original.
    """
    if date_str:
        match = re.match(r'(\d{4}-\d{2}-\d{2})', date_str)
        if match:
            return match.group(1)
    return date_str

# --- Función de Parsing para la Lista UN ---
def parse_un_xml(xml_content_bytes):
    """Parsea el XML de la lista del Comité de Sanciones de la ONU."""
    entries = []
    current_time = datetime.now()
    try:
        xml_content_str = xml_content_bytes.decode('utf-8')
        root = ET.fromstring(xml_content_str)

        # En este XML no se manejan namespaces en los elementos principales
        date_generated = root.attrib.get("dateGenerated", "Unknown")
        logging.info(f"Fecha de generación de la lista UN: {date_generated}")

        # El listado de individuos se encuentra en: ROOT -> INDIVIDUALS -> INDIVIDUAL
        individuals = root.find("INDIVIDUALS")
        if individuals is None:
            logging.error("No se encontró el nodo 'INDIVIDUALS' en el XML.")
            return None

        individual_list = individuals.findall("INDIVIDUAL")
        logging.info(f"Encontrados {len(individual_list)} individuos en la lista UN.")

        for indiv in individual_list:
            dataid = safe_findtext(indiv, "DATAID")
            versionnum = safe_findtext(indiv, "VERSIONNUM")
            first_name = safe_findtext(indiv, "FIRST_NAME")
            second_name = safe_findtext(indiv, "SECOND_NAME")
            third_name = safe_findtext(indiv, "THIRD_NAME")
            un_list_type = safe_findtext(indiv, "UN_LIST_TYPE")
            reference_number = safe_findtext(indiv, "REFERENCE_NUMBER")
            listed_on = safe_findtext(indiv, "LISTED_ON")
            name_original_script = safe_findtext(indiv, "NAME_ORIGINAL_SCRIPT")
            comments1 = safe_findtext(indiv, "COMMENTS1")

            listed_on = clean_date(listed_on)
            
            # Título (solo se espera un VALUE dentro de TITLE)
            title = ""
            title_elem = indiv.find("TITLE")
            if title_elem is not None:
                title = safe_findtext(title_elem, "VALUE")
            
            # Designación(es): puede haber más de un VALUE
            designations = []
            designation_elem = indiv.find("DESIGNATION")
            if designation_elem is not None:
                for val in designation_elem.findall("VALUE"):
                    if val.text:
                        designations.append(val.text.strip())
            designation = "; ".join(designations) if designations else None

            # Nacionalidad y Tipo de Lista (se espera solo un VALUE cada uno)
            nationality = ""
            nationality_elem = indiv.find("NATIONALITY")
            if nationality_elem is not None:
                nationality = safe_findtext(nationality_elem, "VALUE")
            
            list_type = ""
            list_type_elem = indiv.find("LIST_TYPE")
            if list_type_elem is not None:
                list_type = safe_findtext(list_type_elem, "VALUE")
            
            # Última actualización(s): puede tener varios VALUE
            last_day_updated = []
            last_day_elem = indiv.find("LAST_DAY_UPDATED")
            if last_day_elem is not None:
                for val in last_day_elem.findall("VALUE"):
                    if val.text:
                        last_day_updated.append(val.text.strip())
            last_day_updated = "; ".join(last_day_updated) if last_day_updated else None

            # Alias(s) del individuo
            aliases = []
            for alias in indiv.findall("INDIVIDUAL_ALIAS"):
                quality = safe_findtext(alias, "QUALITY")
                alias_name = safe_findtext(alias, "ALIAS_NAME")
                if alias_name:
                    aliases.append(f"{quality}: {alias_name}" if quality else alias_name)
            individual_aliases = "; ".join(aliases) if aliases else None

            # Dirección: puede tener múltiples, en el ejemplo se muestra un <COUNTRY> vacío
            # Se toma el contenido textual de <INDIVIDUAL_ADDRESS>
            individual_address = ""
            address_elem = indiv.find("INDIVIDUAL_ADDRESS")
            if address_elem is not None:
                # Por ejemplo, se puede extraer el COUNTRY
                individual_address = safe_findtext(address_elem, "COUNTRY")
                if individual_address == "":
                    individual_address = None

            # Fecha de nacimiento: se muestra un único nodo <INDIVIDUAL_DATE_OF_BIRTH>
            dob_info = indiv.find("INDIVIDUAL_DATE_OF_BIRTH")
            individual_date_of_birth = ""
            if dob_info is not None:
                type_of_date = safe_findtext(dob_info, "TYPE_OF_DATE")
                year = safe_findtext(dob_info, "YEAR")
                if type_of_date or year:
                    individual_date_of_birth = f"{type_of_date}: {year}" if type_of_date else year
                else:
                    individual_date_of_birth = None
            else:
                individual_date_of_birth = None

            # Lugar(es) de nacimiento: puede haber más de uno
            places = []
            for place in indiv.findall("INDIVIDUAL_PLACE_OF_BIRTH"):
                city = safe_findtext(place, "CITY")
                state = safe_findtext(place, "STATE_PROVINCE")
                country = safe_findtext(place, "COUNTRY")
                parts = list(filter(None, [city, state, country]))
                if parts:
                    places.append(", ".join(parts))
            individual_place_of_birth = "; ".join(places) if places else None

            parsed_entry = {
                'dataid': int(dataid) if dataid.isdigit() else None,
                'versionnum': int(versionnum) if versionnum and versionnum.isdigit() else None,
                'first_name': first_name if first_name else None,
                'second_name': second_name if second_name else None,
                'third_name': third_name if third_name else None,
                'un_list_type': un_list_type if un_list_type else None,
                'reference_number': reference_number if reference_number else None,
                'listed_on': listed_on if listed_on else None,
                'name_original_script': name_original_script if name_original_script else None,
                'comments1': comments1 if comments1 else None,
                'title': title if title else None,
                'designation': designation,
                'nationality': nationality if nationality else None,
                'list_type': list_type if list_type else None,
                'last_day_updated': last_day_updated,
                'individual_aliases': individual_aliases,
                'individual_address': individual_address,
                'individual_date_of_birth': individual_date_of_birth,
                'individual_place_of_birth': individual_place_of_birth,
                'load_timestamp': current_time
            }

            if parsed_entry['dataid'] is None:
                logging.warning(f"Entrada descartada por falta de DATAID: {parsed_entry}")
            else:
                entries.append(parsed_entry)

        logging.info(f"Parseadas {len(entries)} entradas UN válidas con éxito.")
        return entries

    except ET.ParseError as e:
        logging.error(f"Error parseando XML UN: {e}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado durante el parseo del XML UN: {e}")
        return None

def format_data_for_pandas(parsed_data):
    """Convierte la lista de diccionarios en un DataFrame de Pandas y asegura las columnas."""
    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)

    expected_columns = [
        'dataid', 'versionnum', 'first_name', 'second_name', 'third_name',
        'un_list_type', 'reference_number', 'listed_on', 'name_original_script',
        'comments1', 'title', 'designation', 'nationality', 'list_type',
        'last_day_updated', 'individual_aliases', 'individual_address',
        'individual_date_of_birth', 'individual_place_of_birth', 'load_timestamp'
    ]
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    df = df[expected_columns]

    df['load_timestamp'] = pd.to_datetime(df['load_timestamp'])
    df['dataid'] = pd.to_numeric(df['dataid'], errors='coerce').astype('Int64')
    df['versionnum'] = pd.to_numeric(df['versionnum'], errors='coerce').astype('Int64')

    df = df.where(pd.notnull(df), None)
    df.drop_duplicates(subset=['dataid'], keep='first', inplace=True)

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

def create_un_table_if_not_exists(db_conn, table_name):
    """Crea la tabla para la lista UN si no existe."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        dataid INTEGER NOT NULL UNIQUE,
        versionnum INTEGER,
        first_name TEXT,
        second_name TEXT,
        third_name TEXT,
        un_list_type VARCHAR(50),
        reference_number VARCHAR(50),
        listed_on DATE,
        name_original_script TEXT,
        comments1 TEXT,
        title TEXT,
        designation TEXT,
        nationality TEXT,
        list_type TEXT,
        last_day_updated TEXT,
        individual_aliases TEXT,
        individual_address TEXT,
        individual_date_of_birth TEXT,
        individual_place_of_birth TEXT,
        load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_{table_name}_dataid ON {table_name} (dataid);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name} (first_name, second_name, third_name);
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
        if db_conn:
            db_conn.rollback()
    finally:
        if cursor:
            cursor.close()

def load_to_postgres(df, table_name, db_conn):
    """Carga el DataFrame en la tabla de la lista UN."""
    if df.empty:
        logging.warning("DataFrame vacío, no se cargará nada en la base de datos.")
        # Aunque esté vacío se ejecuta el truncate
        pass

    cursor = None
    try:
        cursor = db_conn.cursor()

        logging.info(f"Truncando tabla '{table_name}' antes de la carga...")
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
        logging.info(f"Tabla '{table_name}' truncada.")

        if df.empty:
            logging.info("DataFrame vacío, la tabla ha sido truncada. Carga completada (tabla vacía).")
            db_conn.commit()
            return True

        logging.info(f"Insertando {len(df)} nuevos registros en la tabla '{table_name}'...")

        cols = df.columns.tolist()
        cols_str = ", ".join([f'"{c}"' for c in cols])
        data_tuples = [tuple(row) for row in df.values]

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
def main():
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

        parsed_data = parse_un_xml(xml_bytes)
        if parsed_data is None:
            logging.error("Error parseando el XML. No se actualizará la base de datos ni el hash.")
            return

        df = format_data_for_pandas(parsed_data)

        conn = connect_db(DB_PARAMS)
        if not conn:
            logging.error("No se pudo conectar a la base de datos. Finalizando.")
            return

        try:
            create_un_table_if_not_exists(conn, DB_TABLE_NAME)
            if load_to_postgres(df, DB_TABLE_NAME, conn):
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
