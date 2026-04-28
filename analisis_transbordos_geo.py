import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import os
from dotenv import load_dotenv
import numpy as np
import folium
from streamlit_folium import st_folium

# Cargar variables de entorno
load_dotenv()

# ======================================================
# CONFIGURACIÓN DE PÁGINA
# ======================================================
st.set_page_config(
    page_title="Análisis Geoespacial de Validaciones",
    page_icon="📍",
    layout="wide"
)

st.title("📍 Análisis Geoespacial Interactivos (Tipos 4 y 8)")
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

# ======================================================
# LÓGICA DE DATOS Y DISTANCIA
# ======================================================

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calcula la distancia en metros entre dos puntos usando Haversine"""
    R = 6371000  # Radio de la Tierra en metros
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def get_all_validations_optimized(all_rutas, ranges):
    """Optimización: Una sola consulta para todos los periodos y rutas, con filtro horario"""
    conn = psycopg2.connect(**DB_TRANSACCIONES)
    rutas_str = "','".join(all_rutas)
    
    all_starts = [r[0] for r in ranges.values()]
    all_ends = [r[1] for r in ranges.values()]
    min_date = min(all_starts)
    max_date = max(all_ends)

    query = f"""
    SELECT 
        fechahoraevento,
        latitude,
        longitude,
        idrutaestacion,
        serialmediopago,
        montoevento
    FROM c_transacciones
    WHERE fechahoraevento >= '{min_date}'
      AND fechahoraevento < '{max_date}'
      AND idrutaestacion IN ('{rutas_str}')
      AND tipoevento IN (4, 8)
      AND idproducto IN ('4d4f')
      AND (fechahoraevento::time >= '05:00:00' AND fechahoraevento::time <= '22:59:59')
      AND latitude IS NOT NULL 
      AND latitude != 0 
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    def assign_period(dt):
        dt_date = dt.date()
        for label, (start, end) in ranges.items():
            start_dt = datetime.strptime(start, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end, "%Y-%m-%d").date()
            if start_dt <= dt_date < end_dt:
                return label
        return None

    df['periodo_label'] = df['fechahoraevento'].apply(assign_period)
    return df[df['periodo_label'].notna()]

# ======================================================
# CONFIGURACIÓN DE PERIODOS Y RUTAS
# ======================================================
# Definición de fechas (Hoy es 2026-03-06 según metadatos)
today = datetime(2026, 3, 6)
date_cut = datetime(2025, 12, 24)

# Definimos los 4 periodos solicitados vinculados a la fecha de corte y la actual
ranges = {
    "1) Post-Corte (24/12 + 7d)": (date_cut.strftime("%Y-%m-%d"), (date_cut + timedelta(days=7)).strftime("%Y-%m-%d")),
    "2) Pre-Corte (24/12 - 7d)": ((date_cut - timedelta(days=7)).strftime("%Y-%m-%d"), date_cut.strftime("%Y-%m-%d")),
    "3) Últimos 7 días (Hoy)": ((today - timedelta(days=7)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")),
    "4) Mismo período (Año Ant.)": ((today - timedelta(days=365+7)).strftime("%Y-%m-%d"), (today - timedelta(days=365)).strftime("%Y-%m-%d"))
}

group_1_rutas = ["0212", "0213"]
group_2_rutas = ["0214", "0215"]
all_rutas_to_query = group_1_rutas + group_2_rutas

# ======================================================
# SIDEBAR
# ======================================================
st.sidebar.header("🚀 Acciones")
st.sidebar.info(f"📅 Corte: **24/12/2025**\n\n⏰ Horario: **05:00 a 23:00**")

if st.sidebar.button("� Cargar/Actualizar Datos (Batch)", type="primary"):
    with st.spinner("Consultando validaciones..."):
        df_all = get_all_validations_optimized(all_rutas_to_query, ranges)
        st.session_state['df_all'] = df_all
        st.session_state['active_polygon'] = None  # Resetear filtro de polígono
        st.sidebar.success(f"Cargados {len(df_all)} registros.")

if st.sidebar.button("🧹 Limpiar Filtro de Polígono"):
    st.session_state['active_polygon'] = None

import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
from shapely.geometry import Point, Polygon
import numpy as np

# ======================================================
# PROCESAMIENTO E INTERACCIÓN (POLÍGONO)
# ======================================================
if 'df_all' in st.session_state:
    df_raw = st.session_state['df_all']
    
    st.subheader("📐 Mapa con Geocerca de Polígono Libre")
    st.markdown("""
    **Instrucciones:**
    1. Use la herramienta de dibujo (cuadrado o polígono) en el mapa para definir su área de interés.
    2. Al finalizar el dibujo, el sistema filtrará las validaciones dentro del área.
    3. Puede borrar y redibujar cuantas veces necesite.
    """)
    
    route_colors = {
        "0212": "#1f77b4", "0213": "#2ca02c", 
        "0214": "#ff7f0e", "0215": "#9467bd"
    }

    # Definir el polígono actual desde el estado
    current_polygon = st.session_state.get('active_polygon', None)

    # Filtrar datos por polígono si existe
    if current_polygon:
        # Shapely espera (lon, lat) para Point y Polygon
        poly_shape = Polygon(current_polygon)
        # Vectorizar el chequeo de "puntos en polígono" es lento, usamos apply para claridad inicial
        mask = df_raw.apply(lambda row: poly_shape.contains(Point(row['longitude'], row['latitude'])), axis=1)
        df_filtered = df_raw[mask].copy()
    else:
        df_filtered = df_raw.copy()

    # Crear mapa
    center_lat = df_raw['latitude'].mean()
    center_lon = df_raw['longitude'].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles="cartodbpositron")

    # Plugin de Dibujo
    draw = Draw(
        export=False,
        position='topleft',
        draw_options={
            'polyline': False,
            'marker': False,
            'circlemarker': False,
            'circle': False,
            'rectangle': True,
            'polygon': True
        },
        edit_options={'remove': True}
    )
    draw.add_to(m)

    # Añadir Leyenda HTML
    legend_html = f'''
     <div style="position: fixed; bottom: 50px; left: 50px; width: 140px; height: 100px; 
     border:2px solid grey; z-index:9999; font-size:12px; background-color:white; opacity: 0.8; padding: 10px;">
     <b>Rutas</b><br>
     <i style="background:{route_colors['0212']};width:10px;height:10px;display:inline-block;"></i> 0212<br>
     <i style="background:{route_colors['0213']};width:10px;height:10px;display:inline-block;"></i> 0213<br>
     <i style="background:{route_colors['0214']};width:10px;height:10px;display:inline-block;"></i> 0214<br>
     <i style="background:{route_colors['0215']};width:10px;height:10px;display:inline-block;"></i> 0215
     </div>
     '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Pintar puntos (Muestra para performance si no hay filtro)
    display_df = df_filtered if (current_polygon or len(df_raw) < 5000) else df_raw.sample(5000)
    for _, row in display_df.iterrows():
        color = route_colors.get(str(row['idrutaestacion']), 'gray')
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=4, color=color, fill=True, fill_color=color, fill_opacity=0.6,
            popup=f"Ruta: {row['idrutaestacion']}<br>{row['fechahoraevento']}",
            weight=1
        ).add_to(m)

    # Mostrar el mapa y capturar el dibujo
    output = st_folium(m, width="100%", height=600, key="polygon_map")

    # Detectar nuevo dibujo o eliminación
    new_polygon_coords = None
    if output and output.get("all_drawings"):
        if len(output["all_drawings"]) > 0:
            last_drawing = output["all_drawings"][-1]
            if last_drawing['geometry']['type'] in ['Polygon', 'Rectangle']:
                 # Folium devuelve [[lat, lon], ...], Shapely usa (lon, lat)
                 coords = last_drawing['geometry']['coordinates'][0]
                 new_polygon_coords = [[c[1], c[0]] for c in coords]
    
    # Si el usuario borró el polígono desde la herramienta del mapa
    elif output and output.get("all_drawings") == []:
        new_polygon_coords = None

    # Lógica de actualización de estado
    if new_polygon_coords != st.session_state.get('active_polygon'):
        st.session_state['active_polygon'] = new_polygon_coords
        st.rerun()

    # Métricas y Gráficos (Basados en df_filtered)
    if current_polygon:
        st.success(f"✅ Área Definida: **{len(df_filtered)}** validaciones encontradas dentro del polígono.")
        # Botón para limpiar si se desea desde aquí también
        if st.button("🗑️ Eliminar Filtro de Área"):
            st.session_state['active_polygon'] = None
            st.rerun()
    else:
        st.info(f"🌐 Vista General (Dibuje un polígono para filtrar).")

    st.markdown("---")

    # GRÁFICOS DE BARRAS - Basados en df_filtered
    if not df_filtered.empty:
        st.subheader(f"📊 Comparativa de Validaciones (Datos del Mapa)")
        col1, col2 = st.columns(2)

        def get_bar_df(df_group, group_rutas):
            data = df_group[df_group['idrutaestacion'].isin(group_rutas)]
            results = []
            for label in ranges.keys():
                count = len(data[data['periodo_label'] == label])
                results.append({"Periodo": label, "Cantidad": count})
            return pd.DataFrame(results)

        df_g1_bars = get_bar_df(df_filtered, group_1_rutas)
        df_g2_bars = get_bar_df(df_filtered, group_2_rutas)

        with col1:
            st.markdown(f"**Rutas {', '.join(group_1_rutas)}**")
            fig1 = px.bar(df_g1_bars, x='Periodo', y='Cantidad', text='Cantidad', color='Periodo',
                         color_discrete_map={
                             "1) Post-Corte (24/12 + 7d)": "#ef553b",
                             "2) Pre-Corte (24/12 - 7d)": "#636efa",
                             "3) Últimos 7 días (Hoy)": "#00cc96",
                             "4) Mismo período (Año Ant.)": "#ab63fa"
                         })
            fig1.update_traces(texttemplate='%{text}', textposition='outside')
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            st.markdown(f"**Rutas {', '.join(group_2_rutas)}**")
            fig2 = px.bar(df_g2_bars, x='Periodo', y='Cantidad', text='Cantidad', color='Periodo',
                         color_discrete_map={
                             "1) Post-Corte (24/12 + 7d)": "#ef553b",
                             "2) Pre-Corte (24/12 - 7d)": "#636efa",
                             "3) Últimos 7 días (Hoy)": "#00cc96",
                             "4) Mismo período (Año Ant.)": "#ab63fa"
                         })
            fig2.update_traces(texttemplate='%{text}', textposition='outside')
            st.plotly_chart(fig2, use_container_width=True)

        with st.expander(" Detalle de registros en el área"):
            st.dataframe(df_filtered.sort_values('fechahoraevento'), use_container_width=True, hide_index=True)
    else:
        st.warning("No hay validaciones en el área seleccionada.")
else:
    st.info("Haga clic en 'Cargar/Actualizar Datos' en el panel lateral para iniciar.")
    
# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    Coordinación de Innovación y Desarrollo - DMT/VMT
</div>
""", unsafe_allow_html=True)
