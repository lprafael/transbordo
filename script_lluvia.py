"""
Script para descarga de datos de lluvia del Departamento Central, Paraguay (Open-Meteo).

CÓMO LLAMAR AL SCRIPT:
----
1. Asegúrate de tener instalado Python y las dependencias:
   pip install requests psycopg2-binary python-dotenv

2. Configura tu archivo .env con las credenciales de la base de datos (DB_HOST, DB_NAME, etc.).

3. Ejecuta el script desde la terminal:
   python script_lluvia.py
   
   Por defecto, el script descargará los datos de "ayer" para todas las localidades 
   configuradas y registrará en la base de datos aquellos eventos donde la lluvia 
   supere el umbral definido (20mm).

4. (Opcional) Si deseas cargar fechas específicas, puedes editar la sección 
   'if __name__ == "__main__":' al final del archivo para llamar a las funciones:
   - cargar_por_fecha(date(2025, 11, 5))
   - cargar_rango_fechas(date(2025, 11, 1), date(2025, 12, 9))
"""

import os
import sys
import requests
import psycopg2
from datetime import datetime, timedelta, date
from dateutil.rrule import rrule, DAILY
from dotenv import load_dotenv

load_dotenv()

# Variable global para rastrear si ocurren errores/alertas
HAS_ERROR = False

# ====
# CONFIGURACIÓN GENERAL
# ====

# Configuración de Open-Meteo
OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/era5"
TIMEZONE = "auto"    # o "UTC", "America/Asuncion", etc.

# Coordenadas de todos los distritos del Departamento Central, Paraguay
LOCATIONS = [
    {"name": "Areguá",              "lat": -25.3125, "lon": -57.3847},
    {"name": "Capiatá",             "lat": -25.3552, "lon": -57.4454},
    {"name": "Fernando de la Mora", "lat": -25.3386, "lon": -57.5217},
    {"name": "Guarambaré",          "lat": -25.4910, "lon": -57.4557},
    {"name": "Itá",                 "lat": -25.5005, "lon": -57.3672},
    {"name": "Itauguá",             "lat": -25.3926, "lon": -57.3542},
    {"name": "J. Augusto Saldívar", "lat": -25.4446, "lon": -57.4344},
    {"name": "Lambaré",             "lat": -25.3468, "lon": -57.6065},
    {"name": "Limpio",              "lat": -25.1661, "lon": -57.4856},
    {"name": "Luque",               "lat": -25.2700, "lon": -57.4872},
    {"name": "Mariano Roque Alonso","lat": -25.2079, "lon": -57.5320},
    {"name": "Ñemby",               "lat": -25.3949, "lon": -57.5357},
    {"name": "Nueva Italia",        "lat": -25.6108, "lon": -57.4656},
    {"name": "San Antonio",         "lat": -25.4213, "lon": -57.5473},
    {"name": "San Lorenzo",         "lat": -25.3397, "lon": -57.5088},
    {"name": "Villa Elisa",         "lat": -25.3676, "lon": -57.5927},
    {"name": "Villeta",             "lat": -25.4949, "lon": -57.5584},
    {"name": "Ypacaraí",            "lat": -25.4078, "lon": -57.2889},
    {"name": "Ypané",               "lat": -25.4500, "lon": -57.5300},
]

# Umbral para registrar el evento (mm/h)
UMBRAL_REGISTRO = 2.5
# Umbral para considerar "lluvia intensa" (para la bandera Es_Lluvia_Intensa)
UMBRAL_LLUVIA_INTENSA = 15

# Configuración de PostgreSQL
PG_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# ====
# FUNCIONES DE UTILIDAD
# ====

def get_db_connection(retries=3, delay=5):
    """
    Intenta establecer una conexión con la base de datos con un sistema de reintentos.
    """
    for i in range(retries):
        try:
            return psycopg2.connect(
                host=PG_CONFIG["host"],
                port=PG_CONFIG["port"],
                dbname=PG_CONFIG["dbname"],
                user=PG_CONFIG["user"],
                password=PG_CONFIG["password"],
                connect_timeout=10
            )
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if i < retries - 1:
                print(f"Error de conexión: {e}. Reintentando en {delay} segundos... ({i+1}/{retries})")
                import time
                time.sleep(delay)
            else:
                return None

def registrar_alerta(descripcion: str, id_tipo_alerta: int = 1):
    """
    Registra una alerta en la tabla alertas.control_alertas cuando ocurre un error.
    
    Args:
        descripcion: Descripción del error/incidente
        id_tipo_alerta: ID del tipo de alerta (por defecto 1)
    """
    global HAS_ERROR
    HAS_ERROR = True

    try:
        conn = get_db_connection()
        if not conn:
            print(f"  ⚠ No se pudo conectar a BD para registrar alerta: {descripcion[:50]}...")
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO alertas.control_alertas 
            (fuente, fechahora_alerta, id_tipo_alerta, verificado, corregido, descripcion_incidente)
            VALUES (%s, NOW(), %s, false, false, %s)
        """, ('script_lluvia.py', id_tipo_alerta, descripcion))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"  ℹ Alerta registrada en control_alertas")
        return True
    except Exception as e:
        print(f"  ⚠ Error al registrar alerta en BD: {e}")
        return False



def registrar_ejecucion_correcta(id_script: int = 1):
    """
    Registra una ejecución correcta en la tabla alertas.registro_ejecuciones.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return

        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO alertas.registro_ejecuciones 
            (id_script, fecha_proceso, fecha_ejecucion, estado, detalles)
            VALUES (%s, CURRENT_DATE, NOW(), %s, %s)
        """, (id_script, 'OK', 'Ejecución completada correctamente'))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("  ℹ Ejecución registrada en alertas.registro_ejecuciones")

    except Exception as e:
        print(f"  ⚠ Error al registrar ejecución en BD: {e}")


def fetch_rain_data(start_date: date, end_date: date):
    """
    Llama a la API de Open-Meteo para el rango [start_date, end_date].
    Consulta múltiples coordenadas (LOCATIONS) y para cada hora toma el valor MÁXIMO de lluvia.
    Devuelve una tupla:
        - records: lista de dicts con:
            - dt (datetime de la hora)
            - rain_mm (mm de lluvia máximo en esa hora)
        - max_accumulated_rain: float (mm acumulados máximos detectados en una estación en el rango solicitado)
    """

    # Preparamos las listas de latitudes y longitudes
    lats = [str(loc["lat"]) for loc in LOCATIONS]
    lons = [str(loc["lon"]) for loc in LOCATIONS]

    params = {
        "latitude": ",".join(lats),
        "longitude": ",".join(lons),
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": "rain",
        "timezone": TIMEZONE,
    }

    try:
        resp = requests.get(OPENMETEO_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        error_msg = f"Error obteniendo datos de lluvia de Open-Meteo: {e}"
        print(f"✗ {error_msg}")
        registrar_alerta(error_msg)
        return [], 0.0

    # Si pedimos varias coordenadas, data es una lista de diccionarios
    if isinstance(data, list):
        data_list = data
    else:
        # Una sola ubicación (o fallback)
        data_list = [data]

    # Validamos estructura básica del primer elemento
    if not data_list or "hourly" not in data_list[0]:
        raise ValueError("Respuesta inesperada de Open-Meteo.")

    # Tiempos (son los mismos para todas las locaciones en la misma request)
    times = data_list[0]["hourly"]["time"]
    num_hours = len(times)

    # Vamos a calcular el MAX rain de todas las locaciones para cada hora
    # Estructura: rains_by_loc[loc_index] es una lista de rains
    rains_by_loc = []
    for d in data_list:
        if "hourly" in d and "rain" in d["hourly"]:
            rains_by_loc.append(d["hourly"]["rain"])
        else:
            rains_by_loc.append([0.0] * num_hours)
    
    records = []
    for i, t_str in enumerate(times):
        # Encontrar la lluvia máxima en esta hora entre todas las zonas
        max_rain = 0.0
        for r_list in rains_by_loc:
            # r_list puede tener menos elementos si hubo error, pero asumimos igual longitud
            if i < len(r_list):
                val = r_list[i]
                if val is not None and float(val) > max_rain:
                    max_rain = float(val)

        dt = datetime.fromisoformat(t_str)
        
        records.append({
            "dt": dt,
            "rain_mm": max_rain,
        })

    # Calculamos el acumulado máximo registrado por una estación en todo el periodo (usualmente un día)
    max_accumulated_rain = 0.0
    for r_list in rains_by_loc:
        # Sumamos la lluvia de esa estación (tratando None como 0)
        total_loc = sum(float(x) for x in r_list if x is not None)
        if total_loc > max_accumulated_rain:
            max_accumulated_rain = total_loc
    
    return records, max_accumulated_rain


def insert_rain_records_into_db(records):
    """
    Inserta en la tabla T_CASUISTICAS_LLUVIA un registro por cada hora con lluvia > UMBRAL_REGISTRO.
    """

    if not records:
        print("No hay datos para insertar.")
        return

    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
    except Exception as e:
        registrar_alerta(f"Error de conexión a BD en insert_rain_records_into_db: {e}")
        return

    insert_sql = """
        INSERT INTO control_metricas.t_casuisticas_lluvia (
            fecha_evento,
            hora_inicio,
            hora_fin,
            es_lluvia_intensa,
            registro_comprobado,
            factor_ajuste,
            fuente_registro,
            mm_caidos,
            notas
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    count_inserted = 0

    for rec in records:
        if rec["rain_mm"] <= UMBRAL_REGISTRO:
            continue  # Solo registramos si supera el umbral (20mm)

        dt = rec["dt"]
        rain_mm = rec["rain_mm"]

        fecha_evento = dt.date()
        hora_inicio = dt.time()
        hora_fin = (dt + timedelta(hours=1)).time()

        es_lluvia_intensa = rain_mm >= UMBRAL_LLUVIA_INTENSA
        registro_comprobado = False
        factor_ajuste = 0.00
        fuente_registro = "Open-Meteo"
        notas = f"Lluvia MAX Central: {rain_mm:.2f} mm"

        try:
            cur.execute(
                insert_sql,
                (
                    fecha_evento,
                    hora_inicio,
                    hora_fin,
                    es_lluvia_intensa,
                    registro_comprobado,
                    factor_ajuste,
                    fuente_registro,
                    rain_mm,
                    notas,
                ),
            )
            count_inserted += 1
        except psycopg2.IntegrityError:
            conn.rollback()
            # Si ya existe, ignoramos y seguimos
            continue
        except Exception as e:
            conn.rollback()
            print(f"Error insertando registro {dt}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"Insertadas {count_inserted} filas en T_CASUISTICAS_LLUVIA.")


def upsert_rainy_day(fecha_evento: date, mm_acumulados: float):
    """
    Inserta un nuevo registro en control_metricas.dias_atipicos
    con tipo_atipico = 'LLUVIA' si el día cumple condición de lluvia (> 5mm).
    Factor de ajuste (exigencia) = 0.5
    """
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
    except Exception as e:
        registrar_alerta(f"Error de conexión a BD en upsert_rainy_day: {e}")
        return

    insert_sql = """
        INSERT INTO control_metricas.dias_atipicos (
            fecha,
            tipo_atipico,
            factor_exigencia,
            descartar_historico,
            fuente_dato,
            observacion
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (fecha, tipo_atipico) DO UPDATE
        SET
            factor_exigencia = EXCLUDED.factor_exigencia,
            descartar_historico = EXCLUDED.descartar_historico,
            fuente_dato = EXCLUDED.fuente_dato,
            observacion = EXCLUDED.observacion;
    """

    try:
        cur.execute(insert_sql, (
            fecha_evento,
            'LLUVIA',         # tipo_atipico en mayúsculas
            0.50,             # factor_exigencia
            False,            # descartar_historico
            'Open-Meteo',     # fuente_dato
            f"Lluvia acumulada > 5mm ({mm_acumulados:.2f} mm)" # observacion
        ))
        conn.commit()
        print(f"Día {fecha_evento} registrado/actualizado como ATÍPICO (Lluvia {mm_acumulados:.2f}mm, factor 0.5).")
    except Exception as e:
        conn.rollback()
        print(f"Error al upsert día atípico {fecha_evento}: {e}")
    finally:
        cur.close()
        conn.close()


# ====
# FUNCIONES PÚBLICAS
# ====

def cargar_por_fecha(fecha: date):
    """
    Caso 1: Una fecha específica (ej: 2025-11-05).
    Llama a la API solo para ese día y guarda los registros.
    """
    print(f"Cargando datos de lluvia para el día {fecha}...")
    records, max_acc = fetch_rain_data(fecha, fecha)
    insert_rain_records_into_db(records)

    # Verificar si es día de lluvia (> 5mm acumulados)
    if max_acc > 5.0:
        upsert_rainy_day(fecha, max_acc)


def cargar_rango_fechas(fecha_inicio: date, fecha_fin: date):
    """
    Caso 2: Desde una fecha a otra (ej: 2025-11-01 a 2025-12-09).
    Si el rango es grande, es recomendable trocearlo día a día
    para no saturar la API ni tener problemas de tamaño de respuesta.
    """
    if fecha_fin < fecha_inicio:
        raise ValueError("fecha_fin no puede ser anterior a fecha_inicio.")

    print(f"Cargando datos de lluvia desde {fecha_inicio} hasta {fecha_fin}...")

    # Ejemplo: llamamos día por día
    for dt in rrule(DAILY, dtstart=fecha_inicio, until=fecha_fin):
        dia = dt.date()
        print(f"  - Día {dia}")
        records, max_acc = fetch_rain_data(dia, dia)
        insert_rain_records_into_db(records)
        
        # Verificar si es día de lluvia (> 5mm acumulados)
        if max_acc > 5.0:
            upsert_rainy_day(dia, max_acc)


def cargar_por_defecto_ayer():
    """
    Caso 3: Sin parámetro -> cargar datos de AYER respecto a hoy.
    """
    hoy = date.today()
    ayer = hoy - timedelta(days=1)
    print(f"Cargando datos de lluvia para AYER: {ayer}...")
    cargar_por_fecha(ayer)


# ====
# FUNCIÓN PRINCIPAL PARA PREFECT
# ====

def main():
    """
    Función principal que ejecutará Prefect.
    Por defecto carga los datos de ayer.
    """
    import traceback
    
    args = sys.argv[1:]
    
    try:
        if len(args) == 0:
            cargar_por_defecto_ayer()
        elif len(args) == 1:
            try:
                f = date.fromisoformat(args[0])
                cargar_por_fecha(f)
            except ValueError:
                print("Error: El formato debe ser YYYY-MM-DD")
        elif len(args) >= 2:
            try:
                f_fin = date.fromisoformat(args[1])
                cargar_rango_fechas(f_inicio, f_fin)
            except ValueError:
                print("Error: El formato debe ser YYYY-MM-DD")
        
        # Si llegamos aquí sin excepción, verificamos si hubo alertas previas
        if not HAS_ERROR:
            registrar_ejecucion_correcta()
        else:
            print("  ⚠ Se generaron alertas durante la ejecución. No se registra en bitácora de ejecuciones correctas.")

    except Exception as e:
        error_detail = f"Fallo en script_lluvia.py: {str(e)}\n\n{traceback.format_exc()}"
        print("\n=== ERROR DETECTADO ===")
        print(error_detail)
        registrar_alerta(error_detail)


# ====
# MAIN DE EJEMPLO
# ====

if __name__ == "__main__":
    main()