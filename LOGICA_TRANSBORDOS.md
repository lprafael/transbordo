# 📋 Documentación: Lógica de Análisis de Transbordos

## 🎯 Objetivo del Sistema

Este dashboard analiza los transbordos realizados en el sistema de transporte público, clasificándolos según el tipo de descuento aplicado y vinculándolos con sus validaciones madre (el viaje original que habilita el transbordo).

---

## 🔢 Clasificación de Transbordos

### Tipos de Transbordo

Los transbordos se clasifican en dos categorías principales:

1. **Primer Transbordo (tipo_transbordo = 1)**
   - Es el primer cambio de bus que realiza el usuario después de su validación original
   - Permite al usuario continuar su viaje en otra línea/empresa
   - Generalmente tiene descuentos más altos

2. **Segundo Transbordo (tipo_transbordo = 2)**
   - Es el segundo cambio de bus que realiza el usuario
   - Solo disponible en ciertos casos según la política de la empresa
   - Generalmente tiene descuentos menores o iguales al primer transbordo

---

## 💰 Tipos de Descuento

El sistema identifica el tipo de descuento basándose en el campo `numerotransbordos` de la base de datos. Este campo es un código que indica tanto el orden del transbordo como el porcentaje de descuento aplicado.

### Entidad 0002 (MAGNO SA)

| Código | Tipo de Descuento | Descripción |
|--------|-------------------|-------------|
| 1 | 100% (1er transbordo) | Primer beneficio con descuento total (viaje gratis) |
| 2 | 50% (1er transbordo) | Primer beneficio con 50% de descuento (cuando no hubo beneficio previo) |
| 5 | 100% + 100% | Primer beneficio de una secuencia que permite dos tramos gratuitos |
| 6 | 100% + 50% (2do transbordo) | Segundo beneficio con 50%, precedido por un descuento del 100% |
| 9 | 50% + 100% | Primer beneficio de una secuencia que inicia con 50% |
| 10 | 50% + 50% (2do transbordo) | Segundo beneficio con 50%, precedido por otro descuento del 50% |

### Entidad 0003 (SAN ISIDRO SRL)

| Código | Tipo de Descuento | Descripción |
|--------|-------------------|-------------|
| 1 | 100% (1er transbordo) | Primer beneficio con descuento total (viaje gratis) |
| 2 | 50% (1er transbordo) | Primer beneficio con 50% de descuento |

### Otras Entidades

Los transbordos que no coincidan con los códigos anteriores se clasifican como "Otro".

---

## 🔗 Vinculación con Validación Madre

### ¿Qué es la Validación Madre?

La **validación madre** es el evento de validación (pago) original que habilita los transbordos posteriores. Es el viaje inicial del usuario antes de realizar cualquier transbordo.

### Metodología de Vinculación

Para cada transbordo, el sistema busca su validación madre utilizando los siguientes criterios:

1. **Misma tarjeta**: Debe ser la misma `serialmediopago`
2. **Consecutivo anterior**: El `consecutivoevento` de la madre debe estar entre `[transbordo - 10, transbordo - 1]`
3. **Más cercana**: Si hay múltiples candidatas, se selecciona la que tiene el consecutivo más alto (más reciente)

### Tipos de Clasificación

Una vez vinculado con su madre, el transbordo se clasifica como:

- **Intra-Empresa**: El transbordo ocurre dentro de la misma empresa (ej: MAGNO → MAGNO)
- **Inter-Empresa**: El transbordo ocurre entre diferentes empresas (ej: MAGNO → SAN ISIDRO)
- **Sin Madre**: No se pudo identificar la validación madre (puede ser un error de datos o un caso especial)

---

## 📊 Métricas del Dashboard

### Métricas Generales

1. **Total Transbordos**
   - Cantidad total de eventos de transbordo detectados en el período seleccionado
   - Incluye todos los tipos de transbordo (primero y segundo)

2. **Tarjetas Únicas**
   - Número de tarjetas diferentes que realizaron al menos un transbordo
   - Permite estimar cuántos usuarios únicos utilizaron el sistema de transbordos

3. **Monto Total Ahorrado**
   - Representa el beneficio económico real percibido por el usuario.
   - **Lógica de Cálculo**:
     - Si `tipotransporte = 1` (Servicio Convencional): La tarifa completa es **Gs. 2.300**. El ahorro es `2300 - monto_pagado`.
     - Si `tipotransporte = 3` (Servicio Diferencial): La tarifa completa es **Gs. 3.400**. El ahorro es `3400 - monto_pagado`.
   - Ejemplo: Si en un bus diferencial el usuario tiene un descuento del 100% (`montoevento = 0`), el ahorro registrado es de Gs. 3.400. Si tiene un descuento del 50% (`montoevento = 1700`), el ahorro es de Gs. 1.700.

### Discriminación de Transbordos

1. **1er Transbordo**
   - Cantidad y porcentaje de primeros transbordos
   - Incluye códigos: 1, 5, 9 (para MAGNO) y 1 (para SAN ISIDRO)

2. **2do Transbordo**
   - Cantidad y porcentaje de segundos transbordos
   - Incluye códigos: 2, 6, 10 (para MAGNO) y 2 (para SAN ISIDRO)

### Tipos de Descuento

Muestra la distribución de cada tipo de descuento:
- **100% (1er transbordo)**: Primer transbordo completamente gratis
- **50% (2do transbordo)**: Segundo transbordo con mitad de precio
- **100% + 100%**: Ambos transbordos gratis
- **100% + 50%**: Primer transbordo gratis, segundo a mitad de precio
- **50% + 100%**: Primer transbordo a mitad de precio, segundo gratis
- **50% + 50%**: Ambos transbordos a mitad de precio

### Vinculación con Validación Madre

1. **Con Validación Madre**
   - Porcentaje de transbordos que pudieron vincularse exitosamente con su validación original
   - Un porcentaje alto indica buena calidad de datos

2. **Inter-Empresa**
   - Transbordos realizados entre diferentes empresas operadoras
   - Importante para análisis de flujos entre operadores

3. **Intra-Empresa**
   - Transbordos realizados dentro de la misma empresa
   - Útil para análisis de rutas internas

---

## 🔍 Análisis Disponibles

### 1. Tipos de Descuento (Pestaña Principal)

- **Distribución General**: Gráfico de torta mostrando la proporción de cada tipo de descuento
- **Resumen Detallado**: Tabla con cantidad, monto total y porcentaje por tipo
- **Por Empresa**: Gráfico de barras apiladas mostrando cómo cada empresa distribuye sus descuentos
- **Comparación 1er vs 2do**: Gráficos lado a lado comparando los tipos de descuento en cada categoría

### 2. Resumen por Empresa

- Cantidad total de transbordos por empresa
- Distribución entre intra-empresa e inter-empresa
- Gráfico de barras apiladas

### 3. Matriz de Transbordos

- Mapa de calor mostrando flujos entre empresas
- Top 10 rutas de transbordo más frecuentes
- Permite identificar patrones de movilidad

### 4. Distribución de Intervalos

- Histograma del tiempo transcurrido entre validación madre y transbordo
- Estadísticas: promedio, mediana, máximo
- Distribución por rangos de tiempo (0-15, 15-30, 30-60, 60-90, 90-120 minutos)

### 5. Datos Detallados

- Tabla completa con todos los transbordos
- Filtros por empresa y clasificación
- Exportación a CSV para análisis externo

### 6. Análisis Geográfico

- Mapa de calor mostrando ubicaciones de transbordos
- Permite identificar zonas de alta concentración de transbordos

---

## 🗄️ Fuentes de Datos

### Base de Datos Transacciones (Azure)

- **Tabla**: `c_transacciones`
- **Campos principales**:
  - `serialmediopago`: Identificador de la tarjeta
  - `fechahoraevento`: Timestamp del evento
  - `entidad`: Código de la empresa operadora
  - `numerotransbordos`: Código del tipo de transbordo
  - `montoevento`: Monto del descuento aplicado
  - `consecutivoevento`: Número secuencial del evento
  - `idrutaestacion`: Identificador de la ruta

### Base de Datos Monitoreo

- **Tablas**: `catalogo_rutas`, `eots`
- **Propósito**: Enriquecer los datos con nombres de empresas

---

## ⚙️ Proceso de Cálculo

### Paso 1: Extracción de Transbordos

Se consultan todos los eventos de tipo 4 (transbordo) para la fecha seleccionada, filtrando por:
- Producto: 4d4f (tarjeta de transporte)
- Entidades: 0002 (MAGNO) y 0003 (SAN ISIDRO)
- Códigos de transbordo válidos

### Paso 2: Obtención de Historial

Para cada tarjeta que realizó transbordos, se obtiene su historial completo de validaciones en una ventana de tiempo de 2.5 horas antes del día analizado hasta el final del día.

### Paso 3: Vinculación de Madres

Se aplica un algoritmo que:
1. Filtra validaciones con consecutivo entre [transbordo - 10, transbordo - 1]
2. Selecciona la más cercana (mayor consecutivo)
3. Extrae información de la validación madre

### Paso 4: Clasificación

Se aplican las funciones de clasificación:
- `tipo_transbordo`: Basado en si es primer o segundo transbordo
- `tipo_descuento`: Basado en el código `numerotransbordos` y la entidad
- `clasificacion_transbordo`: Basado en la comparación de empresas

### Paso 5: Enriquecimiento

Se agregan nombres de empresas desde la base de datos de monitoreo.

### Paso 6: Cálculos Adicionales

- Intervalo de tiempo entre madre y transbordo
- Métricas agregadas
- Preparación de visualizaciones

---

## 📝 Notas Técnicas

### Rendimiento

- El procesamiento se realiza en lotes de 500 registros para optimizar memoria
- Se utilizan tablas temporales en PostgreSQL para mejorar velocidad de consultas
- El historial se limita a una ventana de 2.5 horas para reducir volumen de datos

### Limitaciones

- Solo se analizan transbordos de entidades 0002 y 0003
- El intervalo máximo considerado es de 120 minutos
- Los mapas geográficos se limitan a 1000 registros por rendimiento

### Validaciones

- Intervalos negativos o mayores a 120 minutos se marcan como `None`
- Transbordos sin madre identificada se clasifican como "Sin Madre"
- Códigos de transbordo no reconocidos se clasifican como "Otro"

---

## 📅 Fecha de Última Actualización

**10 de febrero de 2026**

---

## 👨‍💻 Desarrollador

**Rafael López**  
Desarrollador Full Stack  
📧 rafadevstack@gmail.com  
📱 0981165851
