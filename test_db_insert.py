
import os
import psycopg2
from dotenv import load_dotenv
from datetime import date

load_dotenv()

PG_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)

def test_upsert():
    conn = get_pg_connection()
    cur = conn.cursor()
    
    fecha_evento = date(2025, 11, 14) # Un dia que sabemos que llovio
    
    print(f"Intentando insertar/actualizar {fecha_evento}...")
    
    try:
        # Intento 1: Sin ON CONFLICT para ver el error real si falla clave duplicada
        # O Intento 2: Con ON CONFLICT para ver si la constraint existe
        
        upsert_sql = """
            INSERT INTO control_metricas.dias_atipicos (
                fecha,
                tipo_atipico,
                factor_exigencia,
                descartar_historico,
                fuente_dato,
                observacion
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (fecha) DO UPDATE
            SET factor_exigencia = EXCLUDED.factor_exigencia,
                observacion = EXCLUDED.observacion;
        """
        
        cur.execute(upsert_sql, (
            fecha_evento,
            'Lluvia',
            0.50,
            False,
            'TEST_DEBUG',
            f"Prueba de insercion directa"
        ))
        conn.commit()
        print("Inserción EXITOSA.")
    except Exception as e:
        print("\n--- ERROR DETECTADO ---")
        print(e)
        print("-----------------------\n")
        conn.rollback()
        
        # Si falla el ON CONFLICT, probemos ver las constraints (aproximacion)
        # O intentar un INSERT simple para provocar el error de constraint y ver su nombre
        try:
            print("Intentando INSERT simple para ver nombre de constraint...")
            simple_insert = """
            INSERT INTO control_metricas.dias_atipicos (fecha, tipo_atipico, factor_exigencia, descartar_historico, fuente_dato, observacion)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.execute(simple_insert, (fecha_evento, 'Lluvia', 0.5, False, 'TEST', 'TEST'))
            conn.commit()
            print("INSERT simple exitoso (No existía el registro).")
        except Exception as e2:
            print(f"Error en INSERT simple: {e2}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    test_upsert()
