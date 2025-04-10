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
SOURCE_URL = "https://sanctionslistservice.ofac.treas.gov/api/download/sdn.xml"
LOG_FILE = 'ofac_sdn_status.log'
HASH_FILE = 'ofac_sdn.xml.hash' # Hash the raw XML file content

# TODO: Usar variables de entorno o AWS Secrets Manager en producción
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST, # ej. 'xxxxx.rds.amazonaws.com'
    "port": DB_PORT
}
# Nueva tabla específica para OFAC SDN
DB_TABLE_NAME = "ofac_sdn_list"
SOURCE_LIST_NAME = "OFAC_SDN" # Para logging

# Configuración de Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

# --- Funciones Auxiliares (sin cambios excepto parse_xml, format_data, load_to_postgres) ---

def get_xml_content():
    """Obtiene el contenido XML desde la URL."""
    logging.info(f"Consultando URL: {SOURCE_URL}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 Python OFAC Scraper'}
        # Realiza la solicitud GET
        response = requests.get(SOURCE_URL, headers=headers, timeout=60)
        # Verifica si hubo errores (como 404, 500, etc.)
        response.raise_for_status()
        # Devuelve el contenido *directamente* como bytes
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

def safe_findtext(element, path, default='', ns=None): # Added ns parameter
    """Safely find text in an XML element, return default if not found."""
    # Add namespace if provided
    xpath = path
    if ns:
        parts = path.split('/')
        xpath_parts = []
        for part in parts:
            if ':' not in part and part not in ['.', '..'] and part != '*': # Avoid adding ns to prefixes like . or functions
                 xpath_parts.append(f"ns:{part}")
            else:
                 xpath_parts.append(part)
        xpath = '/'.join(xpath_parts)

    found_element = element.find(xpath, namespaces=ns)
    if found_element is not None and found_element.text:
        return found_element.text.strip()
    return default

# --- Funciones Principales (Modificadas) ---

def parse_xml(xml_content_bytes):
    """Parsea el contenido XML de OFAC SDN."""
    entries = []
    current_time = datetime.now()
    try:
        xml_content_str = xml_content_bytes.decode('utf-8')
        root = ET.fromstring(xml_content_str)

        # Detectar namespace
        ns_match = re.match(r'\{.*\}', root.tag)
        namespace = ns_match.group(0)[1:-1] if ns_match else None
        ns_map = {'ns': namespace} if namespace else None # Namespace map for findall etc.
        ns_prefix = 'ns:' if namespace else '' # Prefix for findall paths (less robust than ns_map)


        pub_date_elem = root.find(f'.//{ns_prefix}Publish_Date', ns_map) # Use ns_map preferred
        publish_date = pub_date_elem.text if pub_date_elem is not None else 'Unknown'
        logging.info(f"Fecha de publicación de la lista OFAC: {publish_date}")

        sdn_entries = root.findall(f'.//{ns_prefix}sdnEntry', ns_map)
        logging.info(f"Encontradas {len(sdn_entries)} entradas SDN.")

        for entry in sdn_entries:
            # Use safe_findtext with the namespace map
            uid = safe_findtext(entry, './uid', ns=ns_map)
            sdn_type = safe_findtext(entry, './sdnType', ns=ns_map)
            remarks = safe_findtext(entry, './remarks', ns=ns_map) # *** Extraer Remarks ***

            first_name = safe_findtext(entry, './firstName', ns=ns_map)
            last_name = safe_findtext(entry, './lastName', ns=ns_map)

            primary_name = f"{first_name} {last_name}".strip() if sdn_type == 'Individual' and first_name else last_name

            programs = [p.text for p in entry.findall(f'./{ns_prefix}programList/{ns_prefix}program', ns_map) if p.text]

            aliases = []
            for aka in entry.findall(f'./{ns_prefix}akaList/{ns_prefix}aka', ns_map):
                aka_type = safe_findtext(aka, './type', ns=ns_map)
                # category = safe_findtext(aka, './category', ns=ns_map) # Not currently used
                aka_fname = safe_findtext(aka, './firstName', ns=ns_map)
                aka_lname = safe_findtext(aka, './lastName', ns=ns_map)
                aka_name = f"{aka_fname} {aka_lname}".strip() if aka_fname else aka_lname
                if aka_name:
                    aliases.append(f"{aka_type}: {aka_name}")

            addresses = []
            for addr in entry.findall(f'./{ns_prefix}addressList/{ns_prefix}address', ns_map):
                addr_parts = [
                    safe_findtext(addr, './address1', ns=ns_map),
                    safe_findtext(addr, './address2', ns=ns_map),
                    safe_findtext(addr, './address3', ns=ns_map),
                    safe_findtext(addr, './city', ns=ns_map),
                    safe_findtext(addr, './stateOrProvince', ns=ns_map),
                    safe_findtext(addr, './postalCode', ns=ns_map),
                    safe_findtext(addr, './country', ns=ns_map)
                ]
                full_addr = ", ".join(filter(None, addr_parts))
                if full_addr:
                    addresses.append(full_addr)

            dobs = [dob.text for dob in entry.findall(f'./{ns_prefix}dateOfBirthList/{ns_prefix}dateOfBirthItem/{ns_prefix}dateOfBirth', ns_map) if dob.text]
            pobs = [pob.text for pob in entry.findall(f'./{ns_prefix}placeOfBirthList/{ns_prefix}placeOfBirthItem/{ns_prefix}placeOfBirth', ns_map) if pob.text]


            ids = []
            nationalities = []
            citizenships = []
            for id_elem in entry.findall(f'./{ns_prefix}idList/{ns_prefix}id', ns_map):
                id_type = safe_findtext(id_elem, './idType', ns=ns_map)
                id_num = safe_findtext(id_elem, './idNumber', ns=ns_map)
                id_country = safe_findtext(id_elem, './idCountry', ns=ns_map)
                id_details = f"{id_type}: {id_num}"
                if id_country:
                    id_details += f" ({id_country})"

                if id_type == 'Nationality':
                     if id_country: nationalities.append(id_country)
                elif id_type == 'Citizenship':
                     if id_country: citizenships.append(id_country)
                # Avoid adding empty ID fields
                elif id_num or (id_type and id_type not in ['Nationality', 'Citizenship']):
                     ids.append(id_details.strip())


            vessel_info = entry.find(f'./{ns_prefix}vesselInfo', ns_map)
            vessel_details = ""
            if vessel_info is not None:
                vessel_parts = [
                    f"Call Sign: {safe_findtext(vessel_info, './callSign', ns=ns_map)}",
                    f"Type: {safe_findtext(vessel_info, './vesselType', ns=ns_map)}",
                    f"Flag: {safe_findtext(vessel_info, './vesselFlag', ns=ns_map)}",
                    f"Owner: {safe_findtext(vessel_info, './vesselOwner', ns=ns_map)}",
                    f"Tonnage: {safe_findtext(vessel_info, './tonnage', ns=ns_map)}",
                    f"GRT: {safe_findtext(vessel_info, './grossRegisteredTonnage', ns=ns_map)}"
                ]
                vessel_details = "; ".join(filter(lambda x: ':' in x and x.split(':')[1].strip(), vessel_parts))

            aircraft_info = entry.find(f'./{ns_prefix}aircraftInfo', ns_map)
            aircraft_details = ""
            # Add parsing logic if needed

            parsed_entry = {
                'uid': int(uid) if uid and uid.isdigit() else None, # Check uid exists before converting
                'entry_type': sdn_type if sdn_type else None,
                'primary_name': primary_name if primary_name else None,
                'remarks': remarks if remarks else None, # *** Añadir Remarks ***
                'sanction_programs': "; ".join(programs) if programs else None,
                'aliases': "; ".join(aliases) if aliases else None,
                'addresses': "; ".join(addresses) if addresses else None,
                'nationalities': "; ".join(nationalities) if nationalities else None,
                'citizenships': "; ".join(citizenships) if citizenships else None,
                'dates_of_birth': "; ".join(dobs) if dobs else None,
                'places_of_birth': "; ".join(pobs) if pobs else None,
                'identifications': "; ".join(ids) if ids else None,
                'vessel_details': vessel_details if vessel_details else None,
                'aircraft_details': aircraft_details if aircraft_details else None,
                'load_timestamp': current_time
            }
            # Filter out entries with no UID or no primary name, as they are likely invalid/incomplete
            if parsed_entry['uid'] and parsed_entry['primary_name']:
                 entries.append(parsed_entry)
            else:
                 logging.warning(f"Entrada SDN descartada por falta de UID o nombre primario: UID='{uid}', Name='{primary_name}'")


        logging.info(f"Parseadas {len(entries)} entradas SDN válidas con éxito.")
        return entries

    except ET.ParseError as e:
        logging.error(f"Error parseando XML: {e}")
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
        'uid', 'entry_type', 'primary_name', 'remarks', 'sanction_programs', # Added remarks
        'aliases', 'addresses', 'nationalities', 'citizenships', 'dates_of_birth',
        'places_of_birth', 'identifications', 'vessel_details',
        'aircraft_details', 'load_timestamp'
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Reordenar y seleccionar columnas
    df = df[expected_columns]

    # Convertir tipos de datos
    df['load_timestamp'] = pd.to_datetime(df['load_timestamp'])
    df['uid'] = pd.to_numeric(df['uid'], errors='coerce').astype('Int64')

    # Reemplazar NaN/NaT por None para BD
    df = df.where(pd.notnull(df), None)

    # Eliminar duplicados basados en UID (en caso de que el XML contenga duplicados)
    df.drop_duplicates(subset=['uid'], keep='first', inplace=True)

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

def create_ofac_table_if_not_exists(db_conn, table_name):
    """Crea la tabla OFAC SDN si no existe."""
    # Usar el SQL con la columna 'remarks'
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        uid INTEGER NOT NULL UNIQUE,
        entry_type VARCHAR(20),
        primary_name TEXT NOT NULL,
        remarks TEXT,                      -- Columna añadida
        sanction_programs TEXT,
        aliases TEXT,
        addresses TEXT,
        nationalities TEXT,
        citizenships TEXT,
        dates_of_birth TEXT,
        places_of_birth TEXT,
        identifications TEXT,
        vessel_details TEXT,
        aircraft_details TEXT,
        load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    -- Add indexes for common query patterns
    CREATE INDEX IF NOT EXISTS idx_{table_name}_uid ON {table_name} (uid);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name} (primary_name); -- Consider GIN/Fulltext
    CREATE INDEX IF NOT EXISTS idx_{table_name}_type ON {table_name} (entry_type);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_programs ON {table_name} (sanction_programs); -- May need optimization
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
    """Carga el DataFrame en la tabla OFAC, reemplazando datos antiguos."""
    if df.empty:
        logging.warning("DataFrame vacío, no se cargará nada en la base de datos.")
        # Truncate the table even if the DataFrame is empty
        pass

    cursor = None
    try:
        cursor = db_conn.cursor()

        logging.info(f"Truncando tabla '{table_name}' antes de la carga...")
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
        logging.info(f"Tabla '{table_name}' truncada.")

        if df.empty:
            logging.info("DataFrame vacío, la tabla ha sido truncada. Carga completada (tabla vacía).")
            db_conn.commit() # Commit the truncate even if no data is inserted
            return True

        logging.info(f"Insertando {len(df)} nuevos registros en la tabla '{table_name}'...")

        # Adaptar para incluir 'remarks'
        cols = df.columns.tolist()
        cols_str = ", ".join([f'"{c}"' for c in cols])
        data_tuples = [tuple(row) for row in df.values]

        insert_query = f"""
            INSERT INTO {table_name} ({cols_str})
            VALUES %s;
        """
        execute_values(cursor, insert_query, data_tuples, page_size=1000) # Ajustar page_size según sea necesario

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


# --- Flujo Principal (sin cambios) ---
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

        parsed_data = parse_xml(xml_bytes)
        if parsed_data is None:
            logging.error("Error parseando el XML. No se actualizará la base de datos ni el hash.")
            return

        df = format_data_for_pandas(parsed_data)

        conn = connect_db(DB_PARAMS)
        if not conn:
            logging.error("No se pudo conectar a la base de datos. Finalizando.")
            return

        try:
            create_ofac_table_if_not_exists(conn, DB_TABLE_NAME)

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