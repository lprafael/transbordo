"""
Análisis de Transbordos - Aplicación Streamlit.
Para ejecutar: streamlit run analisis_transbordos_streamlit.py
"""
import streamlit as st
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import time
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ======================================================
# FUNCIONES DE PROCESAMIENTO PARALELO (TOP LEVEL)
# ======================================================
def worker_matching_logic(chunk_df, h_dict):
    """
    Función que ejecutan los workers en paralelo. 
    Se encarga de buscar la validación madre para un conjunto de transbordos.
    """
    import pandas as pd
    results_list = []
    
    for _, row in chunk_df.iterrows():
        card_id = row['serialmediopago']
        
        # Obtener historial de la tarjeta desde el diccionario pre-agrupado
        card_history_data = h_dict.get(card_id, [])
        if not card_history_data:
            results_list.append({
                'idsam_madre': None, 'fechahoraevento_madre': None, 
                'entidad_madre': None, 'idrutaestacion_madre': None,
                'latitude_madre': None, 'longitude_madre': None,
                'consecutivoevento_madre': None, 'montoevento_madre': None
            })
            continue

        # Determinar target_num_trans según lógica de negocio
        target_num_trans = None
        if row['entidad'] == '0002':
            if row['numerotransbordos'] in [5, 6]: target_num_trans = 4
            elif row['numerotransbordos'] in [9, 10]: target_num_trans = 8
        elif row['entidad'] == '0003': 
            target_num_trans = 0

        # Filtrar historial de la tarjeta: consecutivo debe ser menor
        # Convertimos a DF temporalmente para esta tarjeta (es rápido ya que son pocos registros)
        card_history_df = pd.DataFrame(card_history_data)
        valid_madres = card_history_df[card_history_df['consecutivoevento'] < row['consecutivoevento']]
        
        if target_num_trans is not None and not valid_madres.empty:
            especificas = valid_madres[valid_madres['numerotransbordos'] == target_num_trans]
            if not especificas.empty:
                valid_madres = especificas
        
        if valid_madres.empty:
            results_list.append({
                'idsam_madre': None, 'fechahoraevento_madre': None, 
                'entidad_madre': None, 'idrutaestacion_madre': None,
                'latitude_madre': None, 'longitude_madre': None,
                'consecutivoevento_madre': None, 'montoevento_madre': None
            })
        else:
            # Tomar la más reciente (mayor consecutivo)
            madre = valid_madres.sort_values('consecutivoevento', ascending=False).iloc[0]
            results_list.append({
                'idsam_madre': madre['idsam'],
                'fechahoraevento_madre': madre['fechahoraevento'],
                'entidad_madre': madre['entidad'],
                'idrutaestacion_madre': madre['idrutaestacion'],
                'latitude_madre': madre['latitude'],
                'longitude_madre': madre['longitude'],
                'consecutivoevento_madre': madre['consecutivoevento'],
                'montoevento_madre': madre['montoevento']
            })
            
    return pd.DataFrame(results_list)

def vectorized_clasificar_descuento(df):
    """Clasificación de descuentos optimizada vectorialmente"""
    import numpy as np
    # Auxiliares
    tarifa = np.where(df['tipotransporte_str'] == '3', 3400, 2400)
    porcentaje = np.where(df['monto_ahorrado'] >= tarifa * 0.95, "100%", 
                         np.where(df['monto_ahorrado'] >= tarifa * 0.45, "50%", "Otro"))
    
    conds = [
        (df['entidad_transbordo'] == '0002') & (df['numerotransbordos'] == 5),
        (df['entidad_transbordo'] == '0002') & (df['numerotransbordos'] == 6),
        (df['entidad_transbordo'] == '0002') & (df['numerotransbordos'] == 9),
        (df['entidad_transbordo'] == '0002') & (df['numerotransbordos'] == 10),
        (df['entidad_transbordo'] == '0003') & (df['numerotransbordos'] == 1),
        (df['entidad_transbordo'] == '0003') & (df['numerotransbordos'] == 2)
    ]
    
    prefixes = ['TDP_V1_T1_', 'TDP_V1_T2_', 'TDP_V2_T1_', 'TDP_V2_T2_', 'EPAS_T1_', 'EPAS_T2_']
    
    results = np.full(len(df), "Otro", dtype=object)
    for cond, prefix in zip(conds, prefixes):
        results[cond] = prefix + porcentaje[cond]
    
    return results

def run_parallel_matching(transfers_df, h_dict, n_workers=6, progress_bar=None):
    """Ejecuta la lógica de matching en paralelo utilizando ProcessPoolExecutor"""
    chunks = np.array_split(transfers_df, n_workers)
    
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(worker_matching_logic, chunk, h_dict) for chunk in chunks]
        
        results = []
        for i, future in enumerate(futures):
            results.append(future.result())
            if progress_bar:
                progress_bar.progress(70 + int(((i+1) / n_workers) * 10))
        
        return pd.concat(results, ignore_index=True)

# ======================================================
# CONFIGURACIÓN DE PÁGINA
# ======================================================
st.set_page_config(
    page_title="Análisis de Transbordos",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🚌 Análisis de Transbordos - Sistema Optimizado")
st.markdown("---")

# ======================================================
# CONEXIONES
# ======================================================
DB_TRANSACCIONES = {
    "host": os.getenv("DB_TRANSACCIONES_HOST", "replicatransacciones.vmt.gov.py"),
    "port": os.getenv("DB_TRANSACCIONES_PORT", "5435"),
    "dbname": os.getenv("DB_TRANSACCIONES_NAME", "transacciones"),
    "user": os.getenv("DB_TRANSACCIONES_USER", "devmt"),
    "password": os.getenv("DB_TRANSACCIONES_PASS", "FootgearBlinkedDigFreewillStricken"),
    "options": "-c statement_timeout=0"
}

DB_MONITOREO = {
    "host": os.getenv("DB_MONITOREO_HOST", "168.90.177.232"),
    "port": os.getenv("DB_MONITOREO_PORT", "2024"),
    "dbname": os.getenv("DB_MONITOREO_NAME", "bbdd-monitoreo-cid"),
    "user": os.getenv("DB_MONITOREO_USER", "FPorta"),
    "password": os.getenv("DB_MONITOREO_PASS", "portaf2024")
}

# ======================================================
# SIDEBAR - CONFIGURACIÓN
# ======================================================
# Cabecera del Sidebar
st.sidebar.markdown("""
    <div style='text-align: center; margin-bottom: 5px;'>
        <p style='margin: 0; font-weight: bold; font-size: 14px;'>Coordinación de Innovación y Desarrollo</p>
        <p style='margin: 0; font-size: 12px;'>DMT - VMT</p>
    </div>
""", unsafe_allow_html=True)

try:
    st.sidebar.image("imagen/Logo_CIDSA2.jpg", width=180)
except Exception:
    pass

st.sidebar.header("⚙️ Configuración")

# Selector de fecha
fecha_seleccionada = st.sidebar.date_input(
    "Fecha a procesar",
    value=datetime.now().date(),
    min_value=datetime.strptime("2025-12-01", "%Y-%m-%d").date(),
    max_value=datetime.now().date()
)

# Filtro de tipo de empresas
filtro_tipo_empresa = st.sidebar.radio(
    "Filtrado de Empresas",
    options=["Todos", "Nuevos"],
    index=0,
    help="1) Todos: todas las empresas\n2) Nuevos: Solo Magno, San Isidro, Ñanduti y La Sanlorenzana"
)

if st.sidebar.button("🔄 Procesar Datos", type="primary"):
    
    inicio = time.time()
    
    # ======================================================
    # PREPARAR FECHAS
    # ======================================================
    fecha_proceso = fecha_seleccionada
    fecha_inicio = fecha_proceso.strftime("%Y-%m-%d")
    fecha_fin = (fecha_proceso + timedelta(days=1)).strftime("%Y-%m-%d")
    fecha_pool_inicio = (datetime.combine(fecha_proceso, datetime.min.time()) - timedelta(hours=2.5)).strftime("%Y-%m-%d %H:%M:%S")
    
    st.info(f"📅 Procesando fecha: **{fecha_inicio}**")
    
    # ======================================================
    # PROGRESS BAR
    # ======================================================
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # ======================================================
    # 1) EXTRAER TRANSBORDOS
    # ======================================================
    status_text.text("📥 Consultando transbordos desde Azure...")
    progress_bar.progress(10)
    
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
      AND tipoevento IN (4, 8) 
      AND (
          (entidad = '0002' AND númerotransbordos IN (1, 5, 6, 9, 10))
          OR
          (entidad = '0003' AND númerotransbordos IN (1, 2))
      )
    """
    
    df_transfers = pd.read_sql(query_transfers, conn_trx)
    progress_bar.progress(30)
    
    if df_transfers.empty:
        st.warning("⚠️ No hay transbordos para procesar en esta fecha.")
        st.stop()
    
    st.success(f"✅ Transbordos encontrados: **{len(df_transfers):,}**")
    
    # ======================================================
    # 2) OBTENER HISTORIAL DE TARJETAS
    # ======================================================
    status_text.text("🎴 Obteniendo historial de tarjetas...")
    progress_bar.progress(40)
    
    unique_cards = df_transfers['serialmediopago'].unique().tolist()
    
    cur_trx = conn_trx.cursor()
    cur_trx.execute("DROP TABLE IF EXISTS tmp_target_cards; CREATE TEMP TABLE tmp_target_cards (card_id BIGINT PRIMARY KEY);")
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
        montoevento,
        númerotransbordos as numerotransbordos
    FROM c_transacciones c
    JOIN tmp_target_cards tc ON c.serialmediopago = tc.card_id
    WHERE c.fechahoraevento >= '{fecha_pool_inicio}'
      AND c.fechahoraevento < '{fecha_fin}'
      AND c.tipoevento IN (4, 8)
      AND c.númerotransbordos IN (0, 1, 2, 4, 5, 6, 8, 9, 10)
      AND c.montoevento >= 0
    """
    
    df_history = pd.read_sql(query_history, conn_trx)
    conn_trx.close()
    progress_bar.progress(60)
    
    st.success(f"✅ Historial cargado: **{len(df_history):,}** registros de **{len(unique_cards):,}** tarjetas únicas")
    
    # ======================================================
    # 3) VINCULACIÓN DE MADRES (OPTIMIZADO CON 6 WORKERS)
    # ======================================================
    status_text.text("🔗 Preparando vinculación de transbordos...")
    progress_bar.progress(70)
    
    # Preparar datos
    df_transfers['consecutivoevento'] = df_transfers['consecutivoevento'].astype('int64')
    df_history['consecutivoevento'] = df_history['consecutivoevento'].astype('int64')
    
    status_text.text("📦 Agrupando historial de tarjetas para búsqueda rápida...")
    # Pre-agrupar historial por tarjeta para evitar filtrado repetitivo
    # Ordenamos por consecutivoevento descendente para que la más reciente sea la primera
    history_dict = {
        card: group.to_dict('records') 
        for card, group in df_history.groupby('serialmediopago')
    }
    
    status_text.text("🚀 Ejecutando vinculación paralela con 6 workers...")
    madre_info = run_parallel_matching(df_transfers, history_dict, n_workers=6, progress_bar=progress_bar)
    
    # Combinar resultados
    df_linked = pd.concat([df_transfers.reset_index(drop=True), madre_info], axis=1)
    
    # DEBUG: Mostrar columnas disponibles
    # st.write("**Columnas disponibles:**", df_linked.columns.tolist())
    
    # Renombrar columnas (df_transfers no tiene sufijos, solo madre_info)
    rename_dict = {}
    
    # Columnas de transbordo (vienen de df_transfers)
    if 'fechahoraevento' in df_linked.columns:
        rename_dict['fechahoraevento'] = 'fecha_transbordo'
    if 'idrutaestacion' in df_linked.columns:
        rename_dict['idrutaestacion'] = 'idruta_transbordo'
    if 'latitude' in df_linked.columns:
        rename_dict['latitude'] = 'latitud_transbordo'
    if 'longitude' in df_linked.columns:
        rename_dict['longitude'] = 'longitud_transbordo'
    if 'idsam' in df_linked.columns:
        rename_dict['idsam'] = 'idsam_transbordo'
    if 'montoevento' in df_linked.columns:
        rename_dict['montoevento'] = 'montoevento_transbordo'
    if 'entidad' in df_linked.columns:
        rename_dict['entidad'] = 'entidad_transbordo'
    
    # Columnas de madre (vienen de madre_info)
    if 'fechahoraevento_madre' in df_linked.columns:
        rename_dict['fechahoraevento_madre'] = 'fecha_madre'
    if 'idrutaestacion_madre' in df_linked.columns:
        rename_dict['idrutaestacion_madre'] = 'idruta_madre'
    if 'latitude_madre' in df_linked.columns:
        rename_dict['latitude_madre'] = 'latitud_madre'
    if 'longitude_madre' in df_linked.columns:
        rename_dict['longitude_madre'] = 'longitud_madre'
    
    df_linked = df_linked.rename(columns=rename_dict)
    
    # st.write("**Columnas después de renombrar:**", df_linked.columns.tolist())
    
    progress_bar.progress(80)
    
    # ======================================================
    # 4) CÁLCULOS ADICIONALES
    # ======================================================
    status_text.text("🧮 Calculando métricas...")
    
    df_linked["fecha_transbordo"] = pd.to_datetime(df_linked["fecha_transbordo"])
    df_linked["fecha_madre"] = pd.to_datetime(df_linked["fecha_madre"])
    
    df_linked["intervalo"] = (
        (df_linked["fecha_transbordo"] - df_linked["fecha_madre"])
        .dt.total_seconds() / 60
    )
    
    df_linked.loc[(df_linked["intervalo"] < 0) | (df_linked["intervalo"] > 120), "intervalo"] = None
    
    # ======================================================
    # NUEVA LÓGICA: CÁLCULO DE MONTO AHORRADO
    # ======================================================
    # tipotransporte = 1 -> tarifa 2400 (Convencional)
    # tipotransporte = 3 -> tarifa 3400 (Diferencial)
    
    # Vectorización de monto ahorrado
    df_linked['monto_ahorrado'] = 0
    df_linked['tipotransporte_str'] = df_linked['tipotransporte'].astype(str)
    
    mask_conv = df_linked['tipotransporte_str'] == '1'
    mask_dif = df_linked['tipotransporte_str'] == '3'
    
    df_linked.loc[mask_conv, 'monto_ahorrado'] = (2400 - df_linked['montoevento_transbordo']).clip(lower=0)
    df_linked.loc[mask_dif, 'monto_ahorrado'] = (3400 - df_linked['montoevento_transbordo']).clip(lower=0)
    
    # Clasificar tipo de transbordo (1 = primer beneficio, 2 = segundo beneficio)
    df_linked["tipo_transbordo"] = 1
    # Segundo transbordo: 6 o 10 para TDP, 2 para EPAS
    df_linked.loc[df_linked["numerotransbordos"].isin([6, 10, 2]), "tipo_transbordo"] = 2
    
    df_linked['tipo_descuento'] = vectorized_clasificar_descuento(df_linked)
    df_linked = df_linked.drop(columns=['tipotransporte_str'])
    
    progress_bar.progress(85)
    
    # ======================================================
    # 5) ENRIQUECIMIENTO CON EMPRESAS
    # ======================================================
    status_text.text("🏷️ Enriqueciendo con nombres de empresas...")
    
    conn_mon = psycopg2.connect(**DB_MONITOREO)
    
    query_empresas = """
    SELECT 
        r.ruta_hex,
        e.eot_nombre AS empresa
    FROM catalogo_rutas r
    JOIN eots e 
        ON r.id_eot_catalogo = e.cod_catalogo;
    """
    
    df_empresas = pd.read_sql(query_empresas, conn_mon).drop_duplicates("ruta_hex")
    conn_mon.close()
    
    df_linked = df_linked.merge(
        df_empresas,
        left_on="idruta_transbordo",
        right_on="ruta_hex",
        how="left"
    ).rename(columns={"empresa": "empresa_transbordo"}).drop(columns=["ruta_hex"], errors='ignore')
    
    df_linked = df_linked.merge(
        df_empresas,
        left_on="idruta_madre",
        right_on="ruta_hex",
        how="left"
    ).rename(columns={"empresa": "empresa_madre"}).drop(columns=["ruta_hex"], errors='ignore')
    
    df_linked["servicio_transbordo"] = (
        df_linked["empresa_transbordo"].fillna("SIN_EMPRESA")
        + " → " +
        df_linked["empresa_madre"].fillna("SIN_EMPRESA")
    )
    
    # Clasificar tipo de transbordo
    df_linked["clasificacion_transbordo"] = "Sin Madre"
    df_linked.loc[df_linked["empresa_madre"].notna(), "clasificacion_transbordo"] = "Intra-Empresa"
    df_linked.loc[
        (df_linked["empresa_madre"].notna()) & 
        (df_linked["empresa_transbordo"] != df_linked["empresa_madre"]), 
        "clasificacion_transbordo"
    ] = "Inter-Empresa"
    
    progress_bar.progress(100)
    status_text.text("✅ Procesamiento completado!")
    
    tiempo_total = time.time() - inicio
    
    # Enriquecer df_history con empresas antes de guardar
    df_history = df_history.merge(
        df_empresas,
        left_on="idrutaestacion",
        right_on="ruta_hex",
        how="left"
    ).drop(columns=["ruta_hex"], errors='ignore')

    # ======================================================
    # GUARDAR EN SESSION STATE
    # ======================================================
    st.session_state['df_linked'] = df_linked
    st.session_state['df_history'] = df_history
    st.session_state['fecha_proceso'] = fecha_inicio
    st.session_state['tiempo_proceso'] = tiempo_total
    
    st.success(f"⏱️ Tiempo de procesamiento: **{tiempo_total:.2f} segundos** ({tiempo_total/60:.2f} minutos)")

# ======================================================
# VISUALIZACIÓN DE DATOS
# ======================================================
if 'df_linked' in st.session_state:
    
    df = st.session_state['df_linked']
    
    # ======================================================
    # FILTRADO SEGÚN SELECCIÓN DE SIDEBAR
    # ======================================================
    if filtro_tipo_empresa == "Nuevos":
        # Lista de empresas solicitadas para el filtro "Nuevos"
        empresas_nuevas = ['MAGNO', 'SAN ISIDRO', 'ÑANDUTI', 'SANLORENZANA']
        pattern = '|'.join(empresas_nuevas)
        df = df[df['empresa_transbordo'].str.contains(pattern, case=False, na=False)]
    
    # ======================================================
    # CÁLCULO DE EXCESO DE TRANSBORDOS (> 2 viajes/día)
    # ======================================================
    # Un viaje con transbordo se identifica por una validación madre única (consecutivoevento_madre)
    viajes_con_transbordo_por_tarjeta = df.groupby('serialmediopago')['consecutivoevento_madre'].nunique()
    tarjetas_con_exceso = (viajes_con_transbordo_por_tarjeta > 2).sum()
    
    st.markdown("---")
    st.header(f"📊 Resultados - {st.session_state['fecha_proceso']}")
    
    # ======================================================
    # MÉTRICAS PRINCIPALES
    # ======================================================
    st.subheader("📊 Métricas Generales")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Transbordos",
            f"{len(df):,}",
            help="📊 Cantidad total de eventos de transbordo detectados en el período seleccionado. Incluye todos los tipos de transbordo (primero y segundo) de todas las empresas operadoras."
        )
    
    with col2:
        tarjetas_unicas = df['serialmediopago'].nunique()
        st.metric(
            "Tarjetas Únicas",
            f"{tarjetas_unicas:,}",
            help="🎴 Número de tarjetas diferentes que realizaron al menos un transbordo en el período. Esta métrica permite estimar cuántos usuarios únicos utilizaron el beneficio del sistema de transbordos."
        )
    
    with col3:
        monto_total_ahorrado = df['monto_ahorrado'].sum()
        st.metric(
            "Monto Total Ahorrado",
            f"Gs {monto_total_ahorrado:,.0f}",
            help="💰 Suma total de los beneficios económicos reales para el usuario. Se calcula como la diferencia entre la tarifa completa (2300 o 3400) y el monto pagado en el transbordo."
        )

    with col4:
        # Métrica en rojo para exceso de transbordos
        st.markdown(f"""
            <div style='text-align: center; color: #ff4b4b; background-color: #ffecec; padding: 15px; border-radius: 10px; border: 1px solid #ff4b4b; height: 100%;'>
                <h5 style='margin: 0; color: #ff4b4b; font-size: 14px;'>Tarjetas con > 2 viajes/día</h5>
                <p style='margin: 0; font-size: 28px; font-weight: bold;'>{tarjetas_con_exceso:,}</p>
                <small style='color: #ff4b4b;'>⚠️ Uso excesivo del beneficio</small>
            </div>
        """, unsafe_allow_html=True)
        
    # Mostrar detalle de tarjetas con exceso si el usuario lo solicita o si hay casos
    if tarjetas_con_exceso > 0:
        with st.expander("🚨 Ver detalle de tarjetas con exceso de transbordos (> 2 viajes/día)"):
            exceso_df = viajes_con_transbordo_por_tarjeta[viajes_con_transbordo_por_tarjeta > 2].reset_index()
            exceso_df.columns = ['Serial Tarjeta', 'Cant. Viajes con Transbordo']
            # Marcar específicamente las que tienen > 3 como pidió el usuario
            exceso_df['Nivel Alerta'] = exceso_df['Cant. Viajes con Transbordo'].apply(lambda x: 'ALTA (>3)' if x > 3 else 'MODERADA (3)')
            
            st.dataframe(exceso_df.sort_values('Cant. Viajes con Transbordo', ascending=False), use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.subheader("🕵️ Historial Detallado de Tarjetas en Alerta")
            
            # Selector de tarjeta para ver su historia
            tarjeta_analizar = st.selectbox(
                "Seleccione una tarjeta para ver su línea de tiempo completa:",
                options=exceso_df['Serial Tarjeta'].unique()
            )
            
            if tarjeta_analizar and 'df_history' in st.session_state:
                h_df = st.session_state['df_history']
                tarjeta_history = h_df[h_df['serialmediopago'] == tarjeta_analizar].copy()
                tarjeta_history = tarjeta_history.sort_values('fechahoraevento')
                
                # Clasificar eventos para mejor visualización
                def etiquetar_evento(row):
                    nt = row['numerotransbordos']
                    ent = row['entidad']
                    if ent == '0002': # TDP
                        if nt == 4: return "🏠 Madre (Viaje 1)"
                        if nt == 8: return "🏠 Madre (Viaje 2)"
                        if nt == 5: return "🚌 1er Transbordo (V1)"
                        if nt == 6: return "🚌 2do Transbordo (V1)"
                        if nt == 9: return "🚌 1er Transbordo (V2)"
                        if nt == 10: return "🚌 2do Transbordo (V2)"
                    elif ent == '0003': # EPAS
                        if nt == 0: return "🏠 Madre (Base)"
                        if nt == 1: return "🚌 1er Transbordo"
                        if nt == 2: return "🚌 2do Transbordo"
                    return "💳 Validación Base"
                
                tarjeta_history['Tipo Evento'] = tarjeta_history.apply(etiquetar_evento, axis=1)
                
                # Formatear para mostrar
                display_cols = ['fechahoraevento', 'Tipo Evento', 'idsam', 'empresa', 'montoevento', 'numerotransbordos', 'consecutivoevento']
                st.table(tarjeta_history[display_cols].rename(columns={
                    'fechahoraevento': 'Fecha/Hora',
                    'idsam': 'ID SAM',
                    'empresa': 'Empresa/Bus',
                    'montoevento': 'Monto',
                    'numerotransbordos': 'Cód. Transb.',
                    'consecutivoevento': 'Consecutivo'
                }))
    
    st.markdown("---")
    
    # ======================================================
    # DISCRIMINACIÓN PRIMER VS SEGUNDO TRANSBORDO
    # ======================================================
    st.subheader("🔄 Discriminación de Transbordos")
    
    col1, col2 = st.columns(2)
    
    primer_transbordo = (df['tipo_transbordo'] == 1).sum()
    segundo_transbordo = (df['tipo_transbordo'] == 2).sum()
    
    with col1:
        st.metric(
            "1er Transbordo",
            f"{primer_transbordo:,}",
            f"{(primer_transbordo/len(df)*100):.1f}%",
            help="🔄 Cantidad de primeros transbordos realizados. Es el primer cambio de bus que realiza el usuario después de su validación original. Generalmente tiene descuentos más altos (100% o 50% según la política de la empresa)."
        )
    
    with col2:
        st.metric(
            "2do Transbordo",
            f"{segundo_transbordo:,}",
            f"{(segundo_transbordo/len(df)*100):.1f}%",
            help="🔄🔄 Cantidad de segundos transbordos realizados. Es el segundo cambio de bus que realiza el usuario. Solo disponible en ciertos casos según la política de la empresa. Generalmente tiene descuentos menores o iguales al primer transbordo (50%)."
        )
    
    # ======================================================
    # TIPOS DE DESCUENTO
    # ======================================================
    # Obtener el recuento de descuentos
    descuentos_count = df['tipo_descuento'].value_counts()
    
    col_tdp1, col_tdp2, col_epas, col_total = st.columns(4)
    
    # Pre-calcular valores para sumatorias
    v1_t1_50 = descuentos_count.get('TDP_V1_T1_50%', 0)
    v1_t1_100 = descuentos_count.get('TDP_V1_T1_100%', 0)
    v1_t2_50 = descuentos_count.get('TDP_V1_T2_50%', 0)
    v1_t2_100 = descuentos_count.get('TDP_V1_T2_100%', 0)
    
    v2_t1_50 = descuentos_count.get('TDP_V2_T1_50%', 0)
    v2_t1_100 = descuentos_count.get('TDP_V2_T1_100%', 0)
    v2_t2_50 = descuentos_count.get('TDP_V2_T2_50%', 0)
    v2_t2_100 = descuentos_count.get('TDP_V2_T2_100%', 0)
    
    epas_t1_50 = descuentos_count.get('EPAS_T1_50%', 0)
    epas_t1_100 = descuentos_count.get('EPAS_T1_100%', 0)
    epas_t2_50 = descuentos_count.get('EPAS_T2_50%', 0)
    epas_t2_100 = descuentos_count.get('EPAS_T2_100%', 0)

    with col_tdp1:
        st.markdown("### 🟦 Viaje 1 (TDP)")
        st.metric("1er Transbordo - 50%", f"{v1_t1_50:,}")
        st.metric("1er Transbordo - 100%", f"{v1_t1_100:,}")
        st.metric("2do Transbordo - 50%", f"{v1_t2_50:,}")
        st.metric("2do Transbordo - 100%", f"{v1_t2_100:,}")

    with col_tdp2:
        st.markdown("### 🟩 Viaje 2 (TDP)")
        st.metric("1er Transbordo - 50%", f"{v2_t1_50:,}")
        st.metric("1er Transbordo - 100%", f"{v2_t1_100:,}")
        st.metric("2do Transbordo - 50%", f"{v2_t2_50:,}")
        st.metric("2do Transbordo - 100%", f"{v2_t2_100:,}")

    with col_epas:
        st.markdown("### 🟧 Viaje X (EPAS)")
        st.metric("1er Transbordo - 50%", f"{epas_t1_50:,}")
        st.metric("1er Transbordo - 100%", f"{epas_t1_100:,}")
        st.metric("2do Transbordo - 50%", f"{epas_t2_50:,}")
        st.metric("2do Transbordo - 100%", f"{epas_t2_100:,}")
    
    with col_total:
        st.markdown("### 📈 Totales")
        st.metric("Total 1er T - 50%", f"{(v1_t1_50 + v2_t1_50 + epas_t1_50):,}")
        st.metric("Total 1er T - 100%", f"{(v1_t1_100 + v2_t1_100 + epas_t1_100):,}")
        st.metric("Total 2do T - 50%", f"{(v1_t2_50 + v2_t2_50 + epas_t2_50):,}")
        st.metric("Total 2do T - 100%", f"{(v1_t2_100 + v2_t2_100 + epas_t2_100):,}")

    st.markdown("---")
    # Mostrar "Otro" y permitir análisis
    otros_count = descuentos_count.get('Otro', 0)
    col_otro, col_btn = st.columns([1, 3])
    with col_otro:
        st.metric("Anomalías (Otro)", f"{otros_count:,}")
    
    if otros_count > 0:
        with st.expander("🔍 Analizar registros clasificados como 'Otro' (Anomalías)"):
            df_otros = df[df['tipo_descuento'] == 'Otro'][['serialmediopago', 'fecha_transbordo', 'entidad_transbordo', 'numerotransbordos', 'montoevento_transbordo', 'monto_ahorrado']]
            st.dataframe(df_otros, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.subheader("🕵️ Análisis Forense de Anomalías")
            
            tarjeta_otro = st.selectbox(
                "Seleccione una tarjeta anómala para analizar su contexto:",
                options=df_otros['serialmediopago'].unique(),
                key="sb_anomalias"
            )
            
            if tarjeta_otro and 'df_history' in st.session_state:
                h_df = st.session_state['df_history']
                t_history = h_df[h_df['serialmediopago'] == tarjeta_otro].copy()
                t_history = t_history.sort_values('fechahoraevento')
                
                # Usar la misma lógica de etiquetado
                def etiquetar_evento_otro(row):
                    nt = row['numerotransbordos']
                    ent = row['entidad']
                    if ent == '0002': # TDP
                        if nt == 4: return "🏠 Madre (Viaje 1)"
                        if nt == 8: return "🏠 Madre (Viaje 2)"
                        if nt == 5: return "🚌 1er Transbordo (V1)"
                        if nt == 6: return "🚌 2do Transbordo (V1)"
                        if nt == 9: return "🚌 1er Transbordo (V2)"
                        if nt == 10: return "🚌 2do Transbordo (V2)"
                    elif ent == '0003': # EPAS
                        if nt == 0: return "🏠 Madre (Base)"
                        if nt == 1: return "🚌 1er Transbordo"
                        if nt == 2: return "🚌 2do Transbordo"
                    return f"❓ Evento Desconocido (Cód: {nt})"
                
                t_history['Tipo Evento'] = t_history.apply(etiquetar_evento_otro, axis=1)
                
                st.table(t_history[['fechahoraevento', 'Tipo Evento', 'idsam', 'empresa', 'montoevento', 'numerotransbordos']].rename(columns={
                    'fechahoraevento': 'Fecha/Hora',
                    'idsam': 'ID SAM',
                    'empresa': 'Empresa/Bus',
                    'montoevento': 'Monto',
                    'numerotransbordos': 'Cód. Transb.'
                }))
                
                st.info("💡 **¿Por qué es una anomalía?** Generalmente ocurre cuando:\n"
                        "1. El código de transbordo no coincide con la entidad (ej: código 1 en TDP).\n"
                        "2. El monto ahorrado no es exactamente 50% o 100% de la tarifa oficial.\n"
                        "3. Es un código no contemplado en las reglas de viaje 1 o 2.")
    
    st.markdown("---")
    
    # ======================================================
    # MÉTRICAS DE VALIDACIÓN MADRE
    # ======================================================
    st.subheader("🔗 Vinculación con Validación Madre")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        transbordos_con_madre = df['empresa_madre'].notna().sum()
        st.metric(
            "Con Validación Madre",
            f"{transbordos_con_madre:,}",
            f"{(transbordos_con_madre/len(df)*100):.1f}%",
            help="🔗 Porcentaje de transbordos que pudieron vincularse exitosamente con su validación madre (el viaje original que habilita el transbordo). La validación madre es el evento de pago inicial antes de realizar cualquier transbordo. Un porcentaje alto indica buena calidad de datos y trazabilidad."
        )
    
    with col2:
        inter_empresa = (df['clasificacion_transbordo'] == 'Inter-Empresa').sum()
        st.metric(
            "Inter-Empresa",
            f"{inter_empresa:,}",
            help="🔀 Transbordos realizados entre diferentes empresas operadoras (ej: MAGNO → SAN ISIDRO). Esta métrica es importante para analizar los flujos de pasajeros entre operadores y para cálculos de compensación económica entre empresas."
        )
    
    with col3:
        intra_empresa = (df['clasificacion_transbordo'] == 'Intra-Empresa').sum()
        st.metric(
            "Intra-Empresa",
            f"{intra_empresa:,}",
            help="🔁 Transbordos realizados dentro de la misma empresa operadora (ej: MAGNO → MAGNO). Útil para análisis de rutas internas y patrones de movilidad dentro de la red de una misma empresa."
        )
    
    st.markdown("---")
    
    # ======================================================
    # TABS DE ANÁLISIS
    # ======================================================
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "💰 Tipos de Descuento",
        "📈 Resumen por Empresa", 
        "🔄 Matriz de Transbordos",
        "⏱️ Distribución de Intervalos",
        "📋 Datos Detallados",
        "🗺️ Mapa de Calor General",
        "📍 Análisis Geográfico Detallado"
    ])
    
    # ======================================================
    # TAB 1: ANÁLISIS DE TIPOS DE DESCUENTO
    # ======================================================
    with tab1:
        st.subheader("💰 Análisis Detallado de Tipos de Descuento")
        
        # Gráfico de distribución general
        descuentos_df = df['tipo_descuento'].value_counts().reset_index()
        descuentos_df.columns = ['Tipo de Descuento', 'Cantidad']
        
        st.subheader("🥧 Distribución General de Tipos de Descuento", help="📊 **Qué es:** Muestra la proporción de cada beneficio de transbordo aplicado.\n\n💡 **Utilidad:** Permite identificar cuál es el beneficio más utilizado por los usuarios (ej: 100% vs 50%).\n\n🧮 **Cálculo:** Se agrupan todos los transbordos del día por su código de beneficio y se calcula el porcentaje sobre el total.")
        fig1 = px.pie(
            descuentos_df,
            values='Cantidad',
            names='Tipo de Descuento',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig1.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig1, use_container_width=True)
        
        # Tabla resumen de tipos de descuento
        st.subheader("📊 Resumen de Tipos de Descuento")
        
        resumen_descuentos = df.groupby('tipo_descuento').agg({
            'serialmediopago': 'count',
            'monto_ahorrado': 'sum'
        }).reset_index()
        resumen_descuentos.columns = ['Tipo de Descuento', 'Cantidad', 'Monto Total Ahorrado']
        resumen_descuentos['Porcentaje'] = (resumen_descuentos['Cantidad'] / resumen_descuentos['Cantidad'].sum() * 100).round(2)
        resumen_descuentos = resumen_descuentos.sort_values('Cantidad', ascending=False)
        
        # Formatear monto
        resumen_descuentos['Monto Total Ahorrado'] = resumen_descuentos['Monto Total Ahorrado'].apply(lambda x: f"Gs {x:,.0f}")
        resumen_descuentos['Porcentaje'] = resumen_descuentos['Porcentaje'].apply(lambda x: f"{x}%")
        
        st.dataframe(resumen_descuentos, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Análisis por empresa y tipo de descuento
        st.subheader("🏢 Tipos de Descuento por Empresa")
        
        empresa_descuento = df.groupby(['empresa_transbordo', 'tipo_descuento']).size().reset_index(name='cantidad')
        
        st.subheader("🏢 Distribución de Tipos de Descuento por Empresa", help="📊 **Qué es:** Desglose de beneficios otorgados por cada empresa operadora.\n\n💡 **Utilidad:** Permite comparar qué empresas están otorgando más beneficios y de qué tipo.\n\n🧮 **Cálculo:** Se cuentan los transbordos agrupándolos por el nombre de la empresa y el tipo de descuento asignado.")
        fig2 = px.bar(
            empresa_descuento,
            x='empresa_transbordo',
            y='cantidad',
            color='tipo_descuento',
            labels={'cantidad': 'Cantidad de Transbordos', 'empresa_transbordo': 'Empresa', 'tipo_descuento': 'Tipo de Descuento'},
            barmode='stack',
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig2.update_layout(height=500, xaxis_tickangle=-45)
        st.plotly_chart(fig2, use_container_width=True)
        
        st.markdown("---")
        
        # Comparación 1er vs 2do transbordo
        st.subheader("🔄 Comparación: 1er vs 2do Transbordo")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Primer transbordo
            primer_tb = df[df['tipo_transbordo'] == 1]['tipo_descuento'].value_counts().reset_index()
            primer_tb.columns = ['Tipo de Descuento', 'Cantidad']
            
            st.markdown("##### 🟢 1er Transbordo", help="📊 **Qué es:** Tipos de beneficios aplicados en el primer cambio de bus.\n\n💡 **Utilidad:** Analizar la efectividad del primer nivel de transbordo.\n\n🧮 **Cálculo:** Filtrado de transbordos clasificados como 'tipo_transbordo = 1'.")
            fig3 = px.bar(
                primer_tb,
                x='Tipo de Descuento',
                y='Cantidad',
                color='Tipo de Descuento',
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig3.update_layout(showlegend=False, xaxis_tickangle=-45)
            st.plotly_chart(fig3, use_container_width=True)
        
        with col2:
            # Segundo transbordo
            segundo_tb = df[df['tipo_transbordo'] == 2]['tipo_descuento'].value_counts().reset_index()
            segundo_tb.columns = ['Tipo de Descuento', 'Cantidad']
            
            st.markdown("##### 🔵 2do Transbordo", help="📊 **Qué es:** Tipos de beneficios aplicados en el segundo cambio de bus.\n\n💡 **Utilidad:** Analizar el uso del segundo nivel de transbordo (secuencia de beneficios).\n\n🧮 **Cálculo:** Filtrado de transbordos clasificados como 'tipo_transbordo = 2'.")
            fig4 = px.bar(
                segundo_tb,
                x='Tipo de Descuento',
                y='Cantidad',
                color='Tipo de Descuento',
                color_discrete_sequence=px.colors.qualitative.Set1
            )
            fig4.update_layout(showlegend=False, xaxis_tickangle=-45)
            st.plotly_chart(fig4, use_container_width=True)
    
    # ======================================================
    # TAB 2: RESUMEN POR EMPRESA
    # ======================================================
    with tab2:
        st.subheader("📊 Transbordos por Empresa", help="📊 **Qué es:** Muestra el volumen total de transbordos por cada empresa operadora.\n\n💡 **Utilidad:** Identifica qué empresas tienen mayor demanda de conexiones.\n\n🧮 **Cálculo:** Suma de todos los transbordos registrados, divididos en 'Intra-Empresa' (mismo operador) e 'Inter-Empresa' (cambio de operador).")
        
        # Resumen por empresa de transbordo
        resumen_empresa = df.groupby('empresa_transbordo').agg({
            'serialmediopago': 'count',
            'clasificacion_transbordo': lambda x: (x == 'Inter-Empresa').sum()
        }).reset_index()
        resumen_empresa.columns = ['Empresa', 'Total Transbordos', 'Inter-Empresa']
        resumen_empresa['Intra-Empresa'] = resumen_empresa['Total Transbordos'] - resumen_empresa['Inter-Empresa']
        resumen_empresa = resumen_empresa.sort_values('Total Transbordos', ascending=False)
        fig = px.bar(
            resumen_empresa,
            x='Empresa',
            y=['Intra-Empresa', 'Inter-Empresa'],
            labels={'value': 'Cantidad', 'variable': 'Tipo'},
            barmode='stack',
            color_discrete_map={'Intra-Empresa': '#3498db', 'Inter-Empresa': '#e74c3c'}
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(resumen_empresa, use_container_width=True, hide_index=True)
    
    # ======================================================
    # TAB 3: MATRIZ DE TRANSBORDOS
    # ======================================================
    with tab3:
        st.subheader("🗺️ Matriz de Flujo de Transbordos", help="📊 **Qué es:** Un mapa de calor que muestra de dónde vienen y a dónde van los usuarios.\n\n💡 **Utilidad:** Detectar alianzas naturales o necesidades de conexión entre empresas.\n\n🧮 **Cálculo:** Eje Y (Empresa Madre) -> Eje X (Empresa de Transbordo). Cada celda representa la cantidad de usuarios que hicieron ese cambio específico.")
        
        # Filtrar solo transbordos con madre identificada
        df_con_madre = df[df['empresa_madre'].notna()].copy()
        
        if len(df_con_madre) > 0:
            matriz = df_con_madre.groupby(['empresa_madre', 'empresa_transbordo']).size().reset_index(name='cantidad')
            matriz_pivot = matriz.pivot(index='empresa_madre', columns='empresa_transbordo', values='cantidad').fillna(0)
            
            fig = px.imshow(
                matriz_pivot,
                labels=dict(x="Empresa Transbordo", y="Empresa Madre", color="Cantidad"),
                x=matriz_pivot.columns,
                y=matriz_pivot.index,
                color_continuous_scale='Blues'
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("🔝 Top 10 Rutas de Transbordo", help="📊 **Qué es:** Muestra las combinaciones más frecuentes de Empresa Origen -> Empresa Destino.\n\n💡 **Utilidad:** Identificar rutas críticas y corredores de alta transferencia.\n\n🧮 **Cálculo:** Se cuentan todas las combinaciones únicas de 'Servicio Origen → Servicio Transbordo' y se muestran las 10 más altas.")
            top_rutas = df_con_madre.groupby('servicio_transbordo').size().reset_index(name='cantidad')
            top_rutas = top_rutas.sort_values('cantidad', ascending=False).head(10)
            
            fig2 = px.bar(
                top_rutas,
                x='cantidad',
                y='servicio_transbordo',
                orientation='h',
                labels={'cantidad': 'Cantidad de Transbordos', 'servicio_transbordo': 'Ruta'}
            )
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("No hay transbordos con validación madre identificada.")
    
    # ======================================================
    # TAB 4: DISTRIBUCIÓN DE INTERVALOS
    # ======================================================
    with tab4:
        st.subheader("⏱️ Distribución de Intervalos de Tiempo", help="📊 **Qué es:** Analiza cuánto tiempo pasa el usuario entre que bajó de un bus y subió al siguiente.\n\n💡 **Utilidad:** Permite evaluar la eficiencia de las frecuencias y el tiempo de espera del usuario.\n\n🧮 **Cálculo:** `Tiempo Transbordo - Tiempo Madre`. Se muestra la frecuencia de estos intervalos en minutos.")
        
        df_con_intervalo = df[df['intervalo'].notna()].copy()
        
        if len(df_con_intervalo) > 0:
            fig = px.histogram(
                df_con_intervalo,
                x='intervalo',
                nbins=50,
                labels={'intervalo': 'Intervalo (minutos)', 'count': 'Frecuencia'},
                color_discrete_sequence=['#2ecc71']
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Intervalo Promedio", f"{df_con_intervalo['intervalo'].mean():.1f} min")
            with col2:
                st.metric("Intervalo Mediano", f"{df_con_intervalo['intervalo'].median():.1f} min")
            with col3:
                st.metric("Intervalo Máximo", f"{df_con_intervalo['intervalo'].max():.1f} min")
            
            # Distribución por rangos
            df_con_intervalo['rango_intervalo'] = pd.cut(
                df_con_intervalo['intervalo'],
                bins=[0, 15, 30, 60, 90, 120],
                labels=['0-15 min', '15-30 min', '30-60 min', '60-90 min', '90-120 min']
            )
            
            rangos = df_con_intervalo['rango_intervalo'].value_counts().reset_index()
            rangos.columns = ['Rango', 'Cantidad']
            
            st.subheader("🍰 Distribución por Rangos de Tiempo", help="📊 **Qué es:** Agrupa los tiempos de espera en rangos lógicos (ej: 0-15 min).\n\n💡 **Utilidad:** Visión simplificada de la puntualidad y tiempos de conexión.\n\n🧮 **Cálculo:** Se clasifican los intervalos en cubetas predefinidas (0-15, 15-30, etc.) y se cuentan los registros en cada una.")
            fig2 = px.pie(
                rangos,
                values='Cantidad',
                names='Rango'
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("No hay datos de intervalo disponibles.")
    
    # ======================================================
    # TAB 5: DATOS DETALLADOS
    # ======================================================
    with tab5:
        st.subheader("Datos Detallados")
        
        # Filtros
        col1, col2 = st.columns(2)
        with col1:
            filtro_empresa = st.multiselect(
                "Filtrar por Empresa Transbordo",
                options=df['empresa_transbordo'].dropna().unique(),
                default=None
            )
        with col2:
            filtro_clasificacion = st.multiselect(
                "Filtrar por Clasificación",
                options=df['clasificacion_transbordo'].unique(),
                default=None
            )
        
        df_filtrado = df.copy()
        if filtro_empresa:
            df_filtrado = df_filtrado[df_filtrado['empresa_transbordo'].isin(filtro_empresa)]
        if filtro_clasificacion:
            df_filtrado = df_filtrado[df_filtrado['clasificacion_transbordo'].isin(filtro_clasificacion)]
        
        # Seleccionar columnas relevantes
        columnas_mostrar = [
            'serialmediopago', 'fecha_transbordo', 'empresa_transbordo', 
            'empresa_madre', 'servicio_transbordo', 'clasificacion_transbordo',
            'tipo_transbordo', 'tipo_descuento', 'montoevento_transbordo', 'monto_ahorrado', 'tipotransporte', 'intervalo', 'numerotransbordos'
        ]
        
        st.dataframe(
            df_filtrado[columnas_mostrar].head(1000),
            use_container_width=True,
            hide_index=True
        )
        
        st.info(f"Mostrando {min(len(df_filtrado), 1000):,} de {len(df_filtrado):,} registros")
        
        # Botón de descarga
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar CSV completo",
            data=csv,
            file_name=f"transbordos_{st.session_state['fecha_proceso']}.csv",
            mime="text/csv",
        )
    
    # ======================================================
    # TAB 6: MAPA DE CALOR GENERAL
    # ======================================================
    with tab6:
        st.subheader("�️ Mapa de Calor General de Transbordos", help="📊 **Qué es:** Visualización de densidad que muestra las zonas con mayor concentración de transbordos.\n\n💡 **Utilidad:** Identificar rápidamente los 'puntos calientes' de transferencia en la ciudad.\n\n🧮 **Cálculo:** Mapa de calor basado exclusivamente en las coordenadas de los eventos de transbordo realizados.")
        
        df_geo_heatmap = df[df['latitud_transbordo'].notna() & df['longitud_transbordo'].notna()].copy()
        
        if len(df_geo_heatmap) > 0:
            fig_heat = px.density_mapbox(
                df_geo_heatmap.head(2000),
                lat='latitud_transbordo',
                lon='longitud_transbordo',
                radius=10,
                center=dict(lat=df_geo_heatmap['latitud_transbordo'].mean(), lon=df_geo_heatmap['longitud_transbordo'].mean()),
                zoom=11,
                mapbox_style="open-street-map",
                height=700
            )
            fig_heat.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_heat, use_container_width=True)
        else:
            st.warning("No hay datos geográficos disponibles para el mapa de calor.")

    # ======================================================
    # TAB 7: ANÁLISIS GEOGRÁFICO DETALLADO (MAPA DE CALOR DINÁMICO)
    # ======================================================
    with tab7:
        st.subheader("📍 Análisis Geográfico Detallado: Mapa de Calor por Etapa", help="📊 **Qué es:** Visualización de densidad que permite ver dónde se concentran los inicios de viaje (Madre) comparado con dónde se concentran los transbordos.\n\n💡 **Utilidad:** Comparar si las zonas de inicio de viaje coinciden con las zonas de transbordo.\n\n🧮 **Cálculo:** Mapa de densidad basado en la etapa del trayecto seleccionada.")
        
        # Filtros superiores
        col1, col2 = st.columns([2, 3])
        
        with col1:
            etapa_seleccionada = st.radio(
                "Seleccionar Etapa para Ver Densidad:",
                ["🏠 Validación Madre", "🟢 1er Transbordo", "🔵 2do Transbordo"],
                index=1, # Por defecto 1er Transbordo
                key="radio_etapa_geo"
            )
        
        with col2:
            empresas_madre_geo = df['empresa_madre'].dropna().unique()
            filtro_empresa_madre = st.multiselect(
                "Filtrar por Empresa de Origen (Madre):",
                options=empresas_madre_geo,
                default=None,
                key="geo_filtro_empresa_calor"
            )
        
        # Filtrado de datos base
        df_geo_base = df.copy()
        if filtro_empresa_madre:
            df_geo_base = df_geo_base[df_geo_base['empresa_madre'].isin(filtro_empresa_madre)]
            
        # Preparar data según la etapa
        if etapa_seleccionada == "🏠 Validación Madre":
            data_mapa = df_geo_base[df_geo_base['latitud_madre'].notna() & df_geo_base['longitud_madre'].notna()].copy()
            lat_col, lon_col = 'latitud_madre', 'longitud_madre'
            color_scale = ['#FEE5D9', '#FCAE91', '#FB6A4A', '#DE2D26', '#A50F15'] # Escala Roja/Naranja
            titulo_mapa = "Densidad: Validaciones Madre (Inicio de Viaje)"
        
        elif etapa_seleccionada == "🟢 1er Transbordo":
            data_mapa = df_geo_base[(df_geo_base['tipo_transbordo'] == 1) & df_geo_base['latitud_transbordo'].notna()].copy()
            lat_col, lon_col = 'latitud_transbordo', 'longitud_transbordo'
            color_scale = ['#EDF8E9', '#BAE4B3', '#74C476', '#31A354', '#006D2C'] # Escala Verde
            titulo_mapa = "Densidad: Primer Beneficio de Transbordo"
            
        else: # 2do Transbordo
            data_mapa = df_geo_base[(df_geo_base['tipo_transbordo'] == 2) & df_geo_base['latitud_transbordo'].notna()].copy()
            lat_col, lon_col = 'latitud_transbordo', 'longitud_transbordo'
            color_scale = ['#EFF3FF', '#BDD7E7', '#6BAED6', '#3182BD', '#08519C'] # Escala Azul
            titulo_mapa = "Densidad: Segundo Beneficio de Transbordo"
        
        if len(data_mapa) > 0:
            fig_det = px.density_mapbox(
                data_mapa.head(2000),
                lat=lat_col,
                lon=lon_col,
                radius=12,
                center=dict(lat=data_mapa[lat_col].mean(), lon=data_mapa[lon_col].mean()),
                zoom=11,
                mapbox_style="open-street-map",
                color_continuous_scale=color_scale,
                height=700
            )
            
            fig_det.update_layout(
                margin={"r":0,"t":40,"l":0,"b":0},
                title=dict(text=titulo_mapa, x=0.5, y=0.98, font=dict(size=20))
            )
            
            st.plotly_chart(fig_det, use_container_width=True)
            st.info(f"Mostrando mapa de densidad para {len(data_mapa):,} registros filtrados.")
        else:
            st.warning(f"No hay suficientes datos geográficos para mostrar la densidad de: {etapa_seleccionada}")

else:
    st.info("👈 Selecciona una fecha y presiona **Procesar Datos** para comenzar el análisis.")
