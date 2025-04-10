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
SOURCE_URL = "https://assets.publishing.service.gov.uk/media/67f7a7908b9b26024aef2ffc/UK_Sanctions_List.xml"
LOG_FILE = 'uk_sanctions_status.log'
HASH_FILE = 'uk_sanctions.xml.hash'  # Archivo para guardar el hash actual del XML

DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT
}

# Nombre de la tabla nueva en PostgreSQL
DB_TABLE_NAME = "sanctions_list_uk"
SOURCE_LIST_NAME = "UK_Sanctions_List"

# Configuración de Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

# --- Funciones Auxiliares ---
def get_xml_content():
    """Obtiene el contenido XML desde la URL."""
    logging.info(f"Consultando URL: {SOURCE_URL}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 Python UK Sanctions Scraper'}
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
    """Calcula el hash MD5 de los datos en bytes."""
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

def safe_findtext(element, tag, default=''):
    """Busca de forma segura el texto de un elemento con etiqueta 'tag'."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return default

def clean_uk_date(date_str):
    """
    Extrae la fecha en formato DD/MM/YYYY usando una expresión regular y la convierte a un string en formato ISO (YYYY-MM-DD).
    Si no se encuentra un formato válido, retorna el valor original.
    """
    if date_str:
        match = re.match(r'(\d{2}/\d{2}/\d{4})', date_str)
        if match:
            # Convertir de DD/MM/YYYY a YYYY-MM-DD
            try:
                date_obj = datetime.strptime(match.group(1), '%d/%m/%Y')
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                return date_str
    return date_str

# --- Función de Parsing para la UK Sanctions List ---
def parse_uk_xml(xml_content_bytes):
    """Parsea el XML de la UK Sanctions List extrayendo los campos relevantes."""
    entries = []
    current_time = datetime.now()
    try:
        xml_str = xml_content_bytes.decode('utf-8')
        root = ET.fromstring(xml_str)
        
        # Obtener la fecha de generación (DateGenerated) del XML
        date_generated = safe_findtext(root, "DateGenerated", "Unknown")
        date_generated = clean_uk_date(date_generated)
        logging.info(f"Fecha generada (DateGenerated): {date_generated}")
        
        # Procesar cada Designation
        designations = root.findall("Designation")
        logging.info(f"Encontradas {len(designations)} designaciones en el XML.")
        
        for des in designations:
            last_updated   = safe_findtext(des, "LastUpdated")
            date_designated = safe_findtext(des, "DateDesignated")
            unique_id = safe_findtext(des, "UniqueID")
            ofsi_group_id = safe_findtext(des, "OFSIGroupID")
            un_reference_number = safe_findtext(des, "UNReferenceNumber")
            
            last_updated = clean_uk_date(last_updated)
            date_designated = clean_uk_date(date_designated)

            # Extraer nombres: separamos el Primary Name de los Alias.
            primary_name = ""
            aliases = []
            names_elem = des.find("Names")
            if names_elem is not None:
                for name in names_elem.findall("Name"):
                    name6 = safe_findtext(name, "Name6")
                    name_type = safe_findtext(name, "NameType")
                    if name_type.lower() == "primary name":
                        primary_name = name6
                    else:
                        if name6:
                            aliases.append(name6)
            names_full = primary_name if primary_name else None
            names_aliases = "; ".join(aliases) if aliases else None

            # Extraer los NonLatinNames (se unen si hay más de uno)
            nonlatin_names = []
            nonlatin_elem = des.find("NonLatinNames")
            if nonlatin_elem is not None:
                for nt in nonlatin_elem.findall("NonLatinName"):
                    nt_text = safe_findtext(nt, "NameNonLatinScript")
                    if nt_text:
                        nonlatin_names.append(nt_text)
            nonlatin_names_str = "; ".join(nonlatin_names) if nonlatin_names else None

            regime_name = safe_findtext(des, "RegimeName")
            individual_entity_ship = safe_findtext(des, "IndividualEntityShip")
            designation_source = safe_findtext(des, "DesignationSource")
            sanctions_imposed = safe_findtext(des, "SanctionsImposed")
            
            # Procesar SanctionsImposedIndicators: concatenamos cada indicador con su valor
            indicators = des.find("SanctionsImposedIndicators")
            sanctions_indicators = []
            if indicators is not None:
                for child in indicators:
                    # Por cada indicador se formatea "Etiqueta: valor"
                    if child.tag and child.text:
                        sanctions_indicators.append(f"{child.tag}: {child.text.strip()}")
            sanctions_indicators_str = "; ".join(sanctions_indicators) if sanctions_indicators else None

            other_information = safe_findtext(des, "OtherInformation")
            uk_statement = safe_findtext(des, "UKStatementofReasons")
            
            # Procesar Addresses: recorrer todos los <Address> y unir las líneas no vacías
            addresses_list = []
            addresses_elem = des.find("Addresses")
            if addresses_elem is not None:
                for addr in addresses_elem.findall("Address"):
                    # Se toman todos los tags que comiencen con "AddressLine" y también AddressCountry
                    addr_parts = []
                    for tag in addr:
                        value = safe_findtext(addr, tag.tag)
                        if value:
                            addr_parts.append(value)
                    if addr_parts:
                        addresses_list.append(", ".join(addr_parts))
            addresses_str = "; ".join(addresses_list) if addresses_list else None

            # Procesar PhoneNumbers
            phone_numbers_elem = des.find("PhoneNumbers")
            phone_numbers = []
            if phone_numbers_elem is not None:
                for pn in phone_numbers_elem.findall("PhoneNumber"):
                    if pn.text:
                        phone_numbers.append(pn.text.strip())
            phone_numbers_str = "; ".join(phone_numbers) if phone_numbers else None

            # Procesar EmailAddresses
            email_elem = des.find("EmailAddresses")
            emails = []
            if email_elem is not None:
                for em in email_elem.findall("EmailAddress"):
                    if em.text:
                        emails.append(em.text.strip())
            emails_str = "; ".join(emails) if emails else None

            parsed_entry = {
                'last_updated': last_updated if last_updated else None,
                'date_designated': date_designated if date_designated else None,
                'unique_id': unique_id if unique_id else None,
                'ofsi_group_id': ofsi_group_id if ofsi_group_id else None,
                'un_reference_number': un_reference_number if un_reference_number else None,
                'primary_name': names_full,
                'aliases': names_aliases,
                'nonlatin_names': nonlatin_names_str,
                'regime_name': regime_name if regime_name else None,
                'individual_entity_ship': individual_entity_ship if individual_entity_ship else None,
                'designation_source': designation_source if designation_source else None,
                'sanctions_imposed': sanctions_imposed if sanctions_imposed else None,
                'sanctions_indicators': sanctions_indicators_str,
                'other_information': other_information if other_information else None,
                'uk_statement': uk_statement if uk_statement else None,
                'addresses': addresses_str,
                'phone_numbers': phone_numbers_str,
                'email_addresses': emails_str,
                'date_generated': date_generated,
                'load_timestamp': current_time
            }
            
            if not unique_id:
                logging.warning(f"Designación descartada por falta de UniqueID: {parsed_entry}")
            else:
                entries.append(parsed_entry)

        logging.info(f"Parseadas {len(entries)} designaciones de UK Sanctions con éxito.")
        return entries

    except ET.ParseError as e:
        logging.error(f"Error parseando XML UK Sanctions: {e}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado durante el parseo del XML UK Sanctions: {e}")
        return None

def format_data_for_pandas(parsed_data):
    """Convierte la lista de diccionarios en un DataFrame y asegura que estén todas las columnas."""
    if not parsed_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(parsed_data)
    
    expected_columns = [
        'unique_id', 'last_updated', 'date_designated', 'ofsi_group_id', 'un_reference_number',
        'primary_name', 'aliases', 'nonlatin_names', 'regime_name', 'individual_entity_ship',
        'designation_source', 'sanctions_imposed', 'sanctions_indicators', 'other_information',
        'uk_statement', 'addresses', 'phone_numbers', 'email_addresses', 'date_generated',
        'load_timestamp'
    ]
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    df = df[expected_columns]
    df['load_timestamp'] = pd.to_datetime(df['load_timestamp'])
    
    # Si se requiere convertir fechas a un formato específico o al tipo DATE, se puede hacer aquí.
    # Por ejemplo, se puede dejar 'date_designated' y 'last_updated' en formato de texto o aplicar una conversión.
    
    df = df.where(pd.notnull(df), None)
    df.drop_duplicates(subset=['unique_id'], keep='first', inplace=True)
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

def create_uk_table_if_not_exists(db_conn, table_name):
    """Crea la tabla para la UK Sanctions List si no existe."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        unique_id VARCHAR(50) NOT NULL UNIQUE,
        last_updated DATE,
        date_designated DATE,
        ofsi_group_id VARCHAR(50),
        un_reference_number VARCHAR(50),
        primary_name TEXT,
        aliases TEXT,
        nonlatin_names TEXT,
        regime_name TEXT,
        individual_entity_ship TEXT,
        designation_source TEXT,
        sanctions_imposed TEXT,
        sanctions_indicators TEXT,
        other_information TEXT,
        uk_statement TEXT,
        addresses TEXT,
        phone_numbers TEXT,
        email_addresses TEXT,
        date_generated DATE,
        load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_{table_name}_unique_id ON {table_name} (unique_id);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name} (primary_name);
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
    """Carga el DataFrame en la tabla UK Sanctions en PostgreSQL."""
    if df.empty:
        logging.warning("DataFrame vacío, no se cargará nada a la base de datos.")
        # Se trunca la tabla aunque esté vacía
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
        insert_query = f"""INSERT INTO {table_name} ({cols_str}) VALUES %s;"""
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
        logging.error("Finalizando script por error al obtener XML.")
        return

    current_hash = calculate_hash(xml_bytes)
    logging.info(f"Hash actual del XML: {current_hash}")
    last_hash = read_last_hash(HASH_FILE)
    logging.info(f"Último hash leído: {last_hash}")

    if current_hash == last_hash:
        logging.info("No se detectaron cambios en el XML. No se requiere actualización.")
    else:
        logging.info("Cambios detectados en el XML. Procediendo a parsear y actualizar la base de datos.")
        parsed_data = parse_uk_xml(xml_bytes)
        if parsed_data is None:
            logging.error("Error en el parseo del XML. No se actualizará la base de datos ni el hash.")
            return

        df = format_data_for_pandas(parsed_data)
        conn = connect_db(DB_PARAMS)
        if not conn:
            logging.error("No se pudo conectar a la base de datos. Finalizando.")
            return

        try:
            create_uk_table_if_not_exists(conn, DB_TABLE_NAME)
            if load_to_postgres(df, DB_TABLE_NAME, conn):
                write_current_hash(HASH_FILE, current_hash)
            else:
                logging.error(f"Error al cargar datos en la tabla '{DB_TABLE_NAME}'. No se actualizará el hash.")
        finally:
            if conn:
                conn.close()
                logging.info("Conexión a la base de datos cerrada.")

    logging.info(f"--- Script {SOURCE_LIST_NAME} Finalizado ---")

if __name__ == "__main__":
    main()
