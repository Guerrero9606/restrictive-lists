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
# TODO: Encontrar la URL real y estable de la lista más reciente.
# Esta es la URL del documento de ejemplo, podría no ser la actual.
# Una búsqueda en EUR-Lex por "Posición Común 2001/931/PESC" es necesaria.
# Usaremos el archivo local para desarrollo basado en la estructura proporcionada.
SOURCE_URL = "https://eur-lex.europa.eu/legal-content/es/TXT/HTML/?uri=CELEX:32019D1341&from=en" # Placeholder
USE_LOCAL_FILE = False # Set to False when using SOURCE_URL
LOCAL_FILE_PATH = 'list-eu.txt' # Path to the provided file
LOG_FILE = 'eu_terrorist_list_status.log' # Guarda el último hash y estado
HASH_FILE = 'eu_terrorist_list.hash' # Archivo para guardar solo el hash

# TODO: Usar variables de entorno o AWS Secrets Manager en producción
DB_PARAMS = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST, # ej. 'xxxxx.rds.amazonaws.com'
    "port": DB_PORT
}
DB_TABLE_NAME = "sanctions_list_eu" # Nombre de la tabla en PostgreSQL
SOURCE_LIST_NAME = "EU_Terrorist_List"

# Configuración de Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

# --- Funciones Auxiliares ---

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
            response = requests.get(SOURCE_URL, timeout=30)
            response.raise_for_status() # Lanza excepción para errores HTTP 4xx/5xx
            # Asegurar codificación correcta (EUR-Lex suele ser UTF-8)
            response.encoding = response.apparent_encoding
            return response.text
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

def parse_details(text, entry_type):
    """Parsea el texto de una entrada para extraer detalles."""
    details = {
        'name': None, 'aliases': None, 'nationality': None,
        'date_of_birth': None, 'place_of_birth': None,
        'identification_details': None, 'other_details': None,
        'raw_data': text
    }

    # Extraer alias (generalmente entre paréntesis)
    aliases = []
    # Buscar alias explícitos como (alias ...) o (otras denominaciones: ...)
    alias_match = re.search(r'\((?:alias|otras\s+denominaciones:)\s+(.*?)\)', text, re.IGNORECASE)
    if alias_match:
        aliases.extend([a.strip() for a in alias_match.group(1).split(',')])
        # Eliminar la parte del alias para facilitar el parseo posterior
        text = text.replace(alias_match.group(0), '').strip()

    # Buscar otros textos entre paréntesis que podrían ser alias o detalles
    extra_parentheses = re.findall(r'\((.*?)\)', text)
    for item in extra_parentheses:
         # Evitar capturar fechas de nacimiento como alias
        if not re.search(r'\d{1,2}\.\d{1,2}\.\d{4}', item):
            aliases.append(item.strip())
            text = text.replace(f'({item})', '').strip() # Remover para simplificar

    details['aliases'] = '; '.join(aliases) if aliases else None

    # Extraer nombre principal (lo que queda al principio)
    # Asumir que el nombre es lo que viene antes de la primera coma o detalles como "nacido el"
    name_match = re.match(r'^(.*?)(?:,?\s+nacido el|,?\s+\(alias|,?\s+Direcciones:|$)', text, re.IGNORECASE)
    if name_match:
        details['name'] = name_match.group(1).strip().replace('«', '').replace('»', '').replace('"', '')
        # Limpiar nombre de posibles indicadores de entidad en lista de personas y viceversa
        if entry_type == 'Person':
            details['name'] = details['name'].replace('Organización', '').replace('Brigada', '').strip()
        # Podría necesitar más limpieza
    else:
        details['name'] = text.split(',')[0].strip().replace('«', '').replace('»', '').replace('"', '') # Fallback


    if entry_type == 'Person':
        # Extraer fecha de nacimiento (varios formatos posibles)
        dob_match = re.search(r'nacido el\s+(\d{1,2}\.\d{1,2}\.\d{4})', text, re.IGNORECASE)
        if dob_match:
            try:
                # Intentar convertir a objeto date (formato DD.MM.YYYY)
                details['date_of_birth'] = datetime.strptime(dob_match.group(1), '%d.%m.%Y').date()
            except ValueError:
                logging.warning(f"Formato de fecha inválido encontrado: {dob_match.group(1)}")
                details['other_details'] = f"DOB info: {dob_match.group(1)}; "

        # Extraer lugar de nacimiento (suele estar entre paréntesis después de la fecha)
        pob_match = re.search(r'nacido el.*?en\s+(.*?)(?:\(|\.\s+Nacional|\.$|$)', text, re.IGNORECASE)
        if pob_match:
            details['place_of_birth'] = pob_match.group(1).strip().rstrip(')')

        # Extraer nacionalidad
        nat_match = re.search(r'nacional de\s+(.*?)(?:\.|\.$|,|Pasaporte|$)', text, re.IGNORECASE)
        if nat_match:
            details['nationality'] = nat_match.group(1).strip()

        # Extraer detalles de identificación (Pasaporte, DNI, etc.)
        id_matches = re.findall(r'(Pasaporte|documento nacional de identidad|permiso de conducción).*?:\s*([^\.\(]+)', text, re.IGNORECASE)
        if id_matches:
            details['identification_details'] = '; '.join([f"{m[0].strip()}: {m[1].strip()}" for m in id_matches])

        # Otros detalles (Direcciones, Rango, etc.)
        other = []
        addr_match = re.search(r'Direcciones:\s*(.*)', text, re.IGNORECASE)
        if addr_match:
            other.append(f"Direcciones: {addr_match.group(1).strip()}")
        rank_match = re.search(r'Rango:\s*(.*)', text, re.IGNORECASE)
        if rank_match:
             other.append(f"Rango: {rank_match.group(1).strip()}")
        if details['other_details']: # Añadir DOB inválida si existe
             other.insert(0, details['other_details'])
        details['other_details'] = '; '.join(other) if other else None


    elif entry_type == 'Entity':
         # Para entidades, el 'name' ya podría ser lo principal.
         # 'other_details' podría capturar el resto si es necesario.
         pass # Añadir lógica específica si es necesario para entidades

    # Limpieza final
    details['name'] = details['name'].replace('—', '').strip() if details['name'] else None

    return details


def parse_html(html_content):
    """Parsea el HTML para extraer la lista de personas y entidades."""
    soup = BeautifulSoup(html_content, 'html.parser')
    entries = []
    current_time = datetime.now()

    # Encontrar el anexo que contiene las listas
    annex = soup.find('div', id='anx_1')
    if not annex:
        logging.error("No se encontró el anexo (div id='anx_1') en el HTML.")
        return None # O [] si se prefiere lista vacía en error

    # Procesar Personas
    persons_header = annex.find('p', string=re.compile(r'I\.\s*PERSONAS'))
    if persons_header:
        current_element = persons_header.find_next_sibling()
        while current_element and current_element.name == 'table':
            rows = current_element.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) == 3: # Esperamos 3 columnas: vacía, número, datos
                    # El texto está en el tercer td, a veces dentro de un span
                    data_cell = cols[2]
                    raw_text = data_cell.get_text(separator=' ', strip=True)
                    if raw_text:
                         parsed = parse_details(raw_text, 'Person')
                         parsed.update({
                             'source_list': SOURCE_LIST_NAME,
                             'entry_type': 'Person',
                             'last_update_date': current_time
                         })
                         entries.append(parsed)
            # Avanzar al siguiente elemento hermano
            current_element = current_element.find_next_sibling()
            # Parar si encontramos el siguiente encabezado o dejamos de encontrar tablas
            if current_element and current_element.name == 'p' and 'II.' in current_element.get_text():
                break
    else:
        logging.warning("No se encontró la sección de Personas.")


    # Procesar Grupos y Entidades
    entities_header = annex.find('p', string=re.compile(r'II\.\s*GRUPOS\s+Y\s+ENTIDADES'))
    if entities_header:
        current_element = entities_header.find_next_sibling()
        while current_element and current_element.name == 'table':
            rows = current_element.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) == 3:
                    data_cell = cols[2]
                    raw_text = data_cell.get_text(separator=' ', strip=True)
                    if raw_text:
                        parsed = parse_details(raw_text, 'Entity')
                        parsed.update({
                            'source_list': SOURCE_LIST_NAME,
                            'entry_type': 'Entity',
                            'last_update_date': current_time
                        })
                        entries.append(parsed)
            current_element = current_element.find_next_sibling()
    else:
         logging.warning("No se encontró la sección de Grupos y Entidades.")


    if not entries:
         logging.warning("No se extrajo ninguna entrada de la lista.")
         return None

    logging.info(f"Se extrajeron {len(entries)} entradas.")
    return entries

def format_data_for_pandas(parsed_data):
    """Convierte los datos parseados a un DataFrame de Pandas."""
    if not parsed_data:
        return pd.DataFrame() # Devuelve DataFrame vacío si no hay datos

    df = pd.DataFrame(parsed_data)

    # Asegurar que todas las columnas esperadas existan
    expected_columns = [
        'source_list', 'entry_type', 'name', 'aliases', 'nationality',
        'date_of_birth', 'place_of_birth', 'identification_details',
        'other_details', 'raw_data', 'last_update_date'
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None # O pd.NA

    # Reordenar y seleccionar columnas
    df = df[expected_columns]

    # Convertir tipos de datos apropiados
    df['last_update_date'] = pd.to_datetime(df['last_update_date'])
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce').dt.date # Convertir a fecha, NaT si falla

    # Reemplazar NaT (Not a Time) por None (NULL en BD) para fechas
    df['date_of_birth'] = df['date_of_birth'].astype(object).where(pd.notnull(df['date_of_birth']), None)

    # Convertir todo a string para hashing consistente, excepto fechas/timestamps
    df_string = df.astype(str).to_json(orient='records', date_format='iso')

    return df, df_string # Devolver DataFrame y su representación en string para hash

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
        return False

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
        # Preparar datos para inserción (lista de tuplas)
        # Asegurar que el orden de las columnas en la tupla coincida con INSERT
        cols = ['source_list', 'entry_type', 'name', 'aliases', 'nationality',
                'date_of_birth', 'place_of_birth', 'identification_details',
                'other_details', 'raw_data', 'load_timestamp']
        # Reemplazar NaN/NaT de Pandas por None de Python para compatibilidad con psycopg2
        data_tuples = [tuple(row) for row in df[cols].where(pd.notnull(df), None).values]

        insert_query = f"""
            INSERT INTO {table_name} ({", ".join(cols)})
            VALUES %s;
        """
        # Usar execute_values para inserción eficiente
        execute_values(cursor, insert_query, data_tuples)

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

# --- Flujo Principal ---
def main():
    logging.info("--- Iniciando Script de Consulta Lista Terroristas UE ---")

    # 1. Obtener HTML
    html = get_html_content()
    if not html:
        logging.error("Finalizando script debido a error obteniendo HTML.")
        return # Salir si no se pudo obtener el HTML

    # 2. Parsear HTML
    parsed_data = parse_html(html)
    if parsed_data is None: # Diferenciar entre error (None) y lista vacía ([])
        logging.error("Finalizando script debido a error de parseo.")
        return
    if not parsed_data: # Lista vacía, pero sin error de parseo
        logging.warning("El parseo no encontró entradas, verificando cambios de hash con 'vacio'.")
        current_data_string = "vacio" # Representación para lista vacía
    else:
        # 3. Formatear datos y obtener string para hash
        df, current_data_string = format_data_for_pandas(parsed_data)
        if df.empty and parsed_data: # Si el DataFrame está vacío pero había datos parseados, hubo error en formato
             logging.error("Error al formatear datos en DataFrame, finalizando.")
             return

    # 4. Calcular hash actual
    current_hash = calculate_hash(current_data_string)
    logging.info(f"Hash actual calculado: {current_hash}")

    # 5. Comparar con hash anterior
    last_hash = read_last_hash(HASH_FILE)
    logging.info(f"Último hash leído: {last_hash}")

    if current_hash == last_hash:
        logging.info("No se detectaron cambios en la lista. No se requiere actualización.")
    else:
        logging.info("Cambios detectados en la lista. Procediendo a actualizar la base de datos.")

        # 6. Conectar a la BD
        conn = connect_db(DB_PARAMS)
        if not conn:
            logging.error("No se pudo conectar a la base de datos. Finalizando.")
            return # Salir si no hay conexión

        # 7. Cargar datos a PostgreSQL
        if not parsed_data: # Si la lista ahora está vacía pero antes no lo estaba
             logging.info("La lista actual está vacía. Borrando registros anteriores...")
             cursor = conn.cursor()
             try:
                 cursor.execute(f"DELETE FROM {DB_TABLE_NAME} WHERE source_list = %s;", (SOURCE_LIST_NAME,))
                 conn.commit()
                 logging.info(f"Registros para {SOURCE_LIST_NAME} eliminados.")
                 # Guardar el nuevo hash (de la lista vacía)
                 write_current_hash(HASH_FILE, current_hash)
             except (Exception, psycopg2.Error) as e:
                 logging.error(f"Error borrando registros de lista vacía: {e}")
                 conn.rollback()
             finally:
                 if cursor: cursor.close()
                 if conn: conn.close()
        elif load_to_postgres(df, DB_TABLE_NAME, conn):
            # 8. Si la carga fue exitosa, guardar el nuevo hash
            write_current_hash(HASH_FILE, current_hash)
        else:
            logging.error("La carga a la base de datos falló. No se actualizará el hash.")

        # Cerrar conexión
        if conn:
            conn.close()
            logging.info("Conexión a la base de datos cerrada.")

    logging.info("--- Script Finalizado ---")

if __name__ == "__main__":
    main()