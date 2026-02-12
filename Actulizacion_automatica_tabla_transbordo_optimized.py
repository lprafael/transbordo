import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import time
import numpy as np

# ======================================================
# INICIO
# ======================================================
inicio = time.time()
hora_inicio = datetime.now()
print(f"\n======================================")
print(f"INICIO DEL PROCESO OPTIMIZADO: {hora_inicio}")
print(f"======================================\n")

# ======================================================
# CONEXIONES
# ======================================================

DB_TRANSACCIONES = {
    "host": "replicatransacciones.vmt.gov.py",
    "port": "5435",
    "dbname": "transacciones",
    "user": "devmt",
    "password": "FootgearBlinkedDigFreewillStricken",
    "options": "-c statement_timeout=0"
}

DB_MONITOREO = {
    "host": "168.90.177.232",
    "port": "2024",
    "dbname": "bbdd-monitoreo-cid",
    "user": "FPorta",
    "password": "portaf2024"
}

# ======================================================
# 1) DETERMINAR FECHA A PROCESAR
# ======================================================

conn_mon = psycopg2.connect(**DB_MONITOREO)
cur_mon = conn_mon.cursor()

cur_mon.execute("""
    SELECT MAX(fecha_transbordo::date)
    FROM public.transbordos;
""")

ultima_fecha = cur_mon.fetchone()[0]

if ultima_fecha is None:
    fecha_proceso = datetime.strptime("2025-12-11", "%Y-%m-%d").date()
else:
    fecha_proceso = ultima_fecha + timedelta(days=1)

# 👉 OPCIÓN MANUAL (Para pruebas o re-procesos)
fecha_proceso = datetime.strptime("2025-12-11", "%Y-%m-%d").date()

fecha_inicio = fecha_proceso.strftime("%Y-%m-%d")
fecha_fin = (fecha_proceso + timedelta(days=1)).strftime("%Y-%m-%d")
# Para el pool de madres, retrocedemos 2 horas del inicio del día (máximo intervalo de transbordo)
fecha_pool_inicio = (datetime.combine(fecha_proceso, datetime.min.time()) - timedelta(hours=2.5)).strftime("%Y-%m-%d %H:%M:%S")

print(f"📅 FECHA A PROCESAR: {fecha_inicio}")

# ======================================================
# OBTENER ULTIMO ID_TRANSBORDO
# ======================================================

cur_mon.execute("SELECT COALESCE(MAX(id_transbordo), 0) FROM public.transbordos;")
ultimo_id = cur_mon.fetchone()[0]
print(f"🔢 Último id_transbordo en BD: {ultimo_id}")

# ======================================================
# 2) EXTRAER TRANSBORDOS (AZURE)
# ======================================================

conn_trx = psycopg2.connect(**DB_TRANSACCIONES)

query_transfers = f"""
SELECT DISTINCT ON (idsam, consecutivoevento, serialmediopago)
    idsam,
    serialmediopago,
    fechahoraevento,
    entidad,
    latitude,
    longitude,
    idrutaestacion,
    tipotransporte,
    tipoevento,
    consecutivoevento,
    númerotransbordos as numerotransbordos,
    montoevento
FROM c_transacciones
WHERE fechahoraevento >= '{fecha_inicio}'
  AND fechahoraevento < '{fecha_fin}'
  AND idproducto IN ('4d4f')
  AND tipoevento = 4 
  AND (
      (entidad = '0002' AND númerotransbordos IN (1,5,6,9,10))
      OR
      (entidad = '0003' AND númerotransbordos IN (1,2))
  )
"""

print("📥 Consultando transbordos...")
df_transfers = pd.read_sql(query_transfers, conn_trx)
print(f"✅ Transbordos encontrados: {len(df_transfers)}")

if df_transfers.empty:
    print("⚠️ No hay transbordos para procesar.")
    conn_trx.close()
    conn_mon.close()
    exit()

# ======================================================
# 3) OBTENER HISTORIAL PARA ESTAS TARJETAS (EVITAR LATERAL JOIN)
# ======================================================

unique_cards = df_transfers['serialmediopago'].unique().tolist()
print(f"🎴 Tarjetas únicas identificadas: {len(unique_cards)}")

# Creamos una tabla temporal en Azure para filtrar eficientemente
cur_trx = conn_trx.cursor()
cur_trx.execute("CREATE TEMP TABLE tmp_target_cards (card_id BIGINT PRIMARY KEY);")
execute_values(cur_trx, "INSERT INTO tmp_target_cards (card_id) VALUES %s", [(c,) for c in unique_cards])

query_history = f"""
SELECT 
    idsam,
    serialmediopago,
    fechahoraevento,
    entidad,
    idrutaestacion,
    latitude,
    longitude,
    consecutivoevento,
    montoevento
FROM c_transacciones c
JOIN tmp_target_cards tc ON c.serialmediopago = tc.card_id
WHERE c.fechahoraevento >= '{fecha_pool_inicio}'
  AND c.fechahoraevento < '{fecha_fin}'
  AND c.tipoevento IN (4, 8)
  AND c.montoevento > 0
"""

print("🧠 Consultando historial de tarjetas para madres...")
df_history = pd.read_sql(query_history, conn_trx)
print(f"✅ Historial cargado: {len(df_history)} registros")

conn_trx.close() # Ya no necesitamos la conexión a Azure

# ======================================================
# 4) VINCULACIÓN DE MADRES (PANDAS MERGE_ASOF)
# ======================================================

print("🔗 Vinculando transbordos con su validación madre...")

# Preparar DataFrames para merge_asof (deben estar ordenados por la clave de unión)
df_transfers = df_transfers.sort_values(['serialmediopago', 'consecutivoevento'])
df_history = df_history.sort_values(['serialmediopago', 'consecutivoevento'])

# merge_asof busca hacia atrás (direction='backward') la madre más cercana
# allow_exact_matches=False asegura que no se vincule con sigo mismo
df_linked = pd.merge_asof(
    df_transfers,
    df_history,
    on='consecutivoevento',
    by='serialmediopago',
    direction='backward',
    allow_exact_matches=False,
    suffixes=('_transbordo', '_madre')
)

# Filtro de seguridad: consecutivoevento_madre debe estar entre [consecutivo - 10, consecutivo - 1]
# Y la fecha de la madre no puede ser posterior (implícito en backward)
mask_valid_madre = (df_linked['consecutivoevento'] - df_linked['consecutivoevento_madre'] <= 10)
df_linked.loc[~mask_valid_madre, [c for c in df_linked.columns if '_madre' in c]] = None

# Renombrar columnas para consistencia con el esquema final
df_linked = df_linked.rename(columns={
    'fechahoraevento_transbordo': 'fecha_transbordo',
    'idrutaestacion_transbordo': 'idruta_transbordo',
    'latitude_transbordo': 'latitud_transbordo',
    'longitude_transbordo': 'longitud_transbordo',
    'idsam_transbordo': 'idsam_transbordo',
    'montoevento_transbordo': 'montoevento_transbordo',
    'fechahoraevento_madre': 'fecha_madre',
    'idrutaestacion_madre': 'idruta_madre',
    'latitude_madre': 'latitud_madre',
    'longitude_madre': 'longitud_madre',
    'idsam_madre': 'idsam_madre',
    'montoevento_madre': 'montoevento_madre'
})

# ======================================================
# 5) CÁLCULOS ADICIONALES
# ======================================================

# Intervalo en minutos
df_linked["fecha_transbordo"] = pd.to_datetime(df_linked["fecha_transbordo"])
df_linked["fecha_madre"] = pd.to_datetime(df_linked["fecha_madre"])

df_linked["intervalo"] = (
    (df_linked["fecha_transbordo"] - df_linked["fecha_madre"])
    .dt.total_seconds() / 60
)

# Limpiar intervalos irrepales
df_linked.loc[(df_linked["intervalo"] < 0) | (df_linked["intervalo"] > 120), "intervalo"] = None

# Tipo de transbordo (1 o 2)
df_linked["tipo_transbordo"] = 1
df_linked.loc[df_linked["numerotransbordos"].isin([2, 6, 10]), "tipo_transbordo"] = 2

# id_transbordo correlativo
df_linked["id_transbordo"] = range(ultimo_id + 1, ultimo_id + 1 + len(df_linked))

# ======================================================
# 6) ENRIQUECIMIENTO CON EMPRESAS (CID)
# ======================================================

print("🏷️ Enriqueciendo nombres de empresas...")

query_empresas = """
SELECT 
    r.ruta_hex,
    e.eot_nombre AS empresa
FROM catalogo_rutas r
JOIN eots e 
    ON r.id_eot_catalogo = e.cod_catalogo;
"""

df_empresas = pd.read_sql(query_empresas, conn_mon).drop_duplicates("ruta_hex")

df_linked = df_linked.merge(
    df_empresas,
    left_on="idruta_transbordo",
    right_on="ruta_hex",
    how="left"
).rename(columns={"empresa": "empresa_transbordo"}).drop(columns=["ruta_hex"])

df_linked = df_linked.merge(
    df_empresas,
    left_on="idruta_madre",
    right_on="ruta_hex",
    how="left"
).rename(columns={"empresa": "empresa_madre"}).drop(columns=["ruta_hex"])

df_linked["servicio_transbordo"] = (
    df_linked["empresa_transbordo"].fillna("SIN_EMPRESA")
    + "-" +
    df_linked["empresa_madre"].fillna("SIN_EMPRESA")
)

# ======================================================
# 7) PERSISTENCIA (MONITOREO)
# ======================================================

print(f"🗑️ Limpiando {fecha_inicio} en BD Monitoreo...")
cur_mon.execute(f"DELETE FROM public.transbordos WHERE fecha_transbordo::date = '{fecha_inicio}';")
conn_mon.commit()

print("💾 Insertando datos finales...")

# Limpiar nulos para Postgres
df_linked = df_linked.replace({pd.NaT: None, np.nan: None})

cols = [
    "id_transbordo", "serialmediopago", "fecha_transbordo", "tipotransporte",
    "consecutivoevento", "idruta_transbordo", "empresa_transbordo",
    "entidad", "latitud_transbordo", "longitud_transbordo",
    "idsam_transbordo", "montoevento_transbordo", "idsam_madre",
    "fecha_madre", "entidad_madre", "idruta_madre", "empresa_madre",
    "latitud_madre", "longitud_madre", "consecutivoevento_madre",
    "montoevento_madre", "servicio_transbordo", "numerotransbordos",
    "intervalo", "tipo_transbordo"
]

# El esquema de la tabla espera nombres específicos, aseguramos correspondencia
df_final = df_linked.rename(columns={
    'tipotransporte': 'tipotransporte_transbordo', # Si es necesario segun esquema
    'consecutivoevento': 'consecutivoevento_transbordo',
    'entidad_transbordo': 'entidad_transbordo', # Ya esta
    'entidad': 'entidad_transbordo'
})

# Re-chequeo de nombres de columnas segun el script original
cols_insert = [
    "id_transbordo", "serialmediopago", "fecha_transbordo", "tipotransporte", 
    "consecutivoevento", "idruta_transbordo", "empresa_transbordo",
    "entidad", "latitud_transbordo", "longitud_transbordo",
    "idsam_transbordo", "montoevento_transbordo", "idsam_madre",
    "fecha_madre", "entidad_madre", "idruta_madre", "empresa_madre",
    "latitud_madre", "longitud_madre", "consecutivoevento_madre",
    "montoevento_madre", "servicio_transbordo", "numerotransbordos",
    "intervalo", "tipo_transbordo"
]
# Ajuste final de nombres para el insert (basado en el script original lineas 340-365)
cols_final_mapped = [
    "id_transbordo", "serialmediopago", "fecha_transbordo", "tipotransporte",
    "consecutivoevento_transbordo", "idruta_transbordo", "empresa_transbordo",
    "entidad_transbordo", "latitud_transbordo", "longitud_transbordo",
    "idsam_transbordo", "montoevento_transbordo", "idsam_madre",
    "fecha_madre", "entidad_madre", "idruta_madre", "empresa_madre",
    "latitud_madre", "longitud_madre", "consecutivoevento_madre",
    "montoevento_madre", "servicio_transbordo", "numerotransbordos",
    "intervalo", "tipo_transbordo"
]

# Creamos el dataframe final con el mapeo correcto
df_to_insert = pd.DataFrame()
df_to_insert["id_transbordo"] = df_linked["id_transbordo"]
df_to_insert["serialmediopago"] = df_linked["serialmediopago"]
df_to_insert["fecha_transbordo"] = df_linked["fecha_transbordo"]
df_to_insert["tipotransporte_transbordo"] = df_linked["tipotransporte"]
df_to_insert["consecutivoevento_transbordo"] = df_linked["consecutivoevento"]
df_to_insert["idruta_transbordo"] = df_linked["idruta_transbordo"]
df_to_insert["empresa_transbordo"] = df_linked["empresa_transbordo"]
df_to_insert["entidad_transbordo"] = df_linked["entidad"]
df_to_insert["latitud_transbordo"] = df_linked["latitud_transbordo"]
df_to_insert["longitud_transbordo"] = df_linked["longitud_transbordo"]
df_to_insert["idsam_transbordo"] = df_linked["idsam_transbordo"]
df_to_insert["montoevento_transbordo"] = df_linked["montoevento_transbordo"]
df_to_insert["idsam_madre"] = df_linked["idsam_madre"]
df_to_insert["fecha_madre"] = df_linked["fecha_madre"]
df_to_insert["entidad_madre"] = df_linked["entidad_madre"]
df_to_insert["idruta_madre"] = df_linked["idruta_madre"]
df_to_insert["empresa_madre"] = df_linked["empresa_madre"]
df_to_insert["latitud_madre"] = df_linked["latitud_madre"]
df_to_insert["longitud_madre"] = df_linked["longitud_madre"]
df_to_insert["consecutivoevento_madre"] = df_linked["consecutivoevento_madre"]
df_to_insert["montoevento_madre"] = df_linked["montoevento_madre"]
df_to_insert["servicio_transbordo"] = df_linked["servicio_transbordo"]
df_to_insert["numerotransbordos"] = df_linked["numerotransbordos"]
df_to_insert["intervalo"] = df_linked["intervalo"]
df_to_insert["tipo_transbordo"] = df_linked["tipo_trans_bor_do"] if 'tipo_trans_bor_do' in df_linked else df_linked["tipo_transbordo"]

insert_sql = f"""
INSERT INTO public.transbordos ({",".join(cols_final_mapped)})
VALUES %s;
"""

execute_values(cur_mon, insert_sql, df_to_insert.values.tolist(), page_size=2000)

conn_mon.commit()
conn_mon.close()

# ======================================================
# FIN
# ======================================================

hora_fin = datetime.now()
tiempo = time.time() - inicio

print("\n======================================")
print("✅ PROCESO FINALIZADO")
print(f"Inicio : {hora_inicio}")
print(f"Fin    : {hora_fin}")
print(f"Tiempo : {tiempo/60:.2f} minutos")
print(f"Registros procesados: {len(df_to_insert)}")
print("======================================\n")
