import streamlit as st
import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import time

st.set_page_config(page_title="Monitoreo IoT Planta", layout="wide")
st.title("🖥️ Monitoreo de Telemetría en Tiempo Real")

# Conexión a la tabla Silver en DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('MaquinasTelemetriaSilver')

# 1. Recuperar de manera dinámica las Partition Keys (device_id) de DynamoDB
@st.cache_data(ttl=10)  # Caché de 10 segundos para no saturar DynamoDB en cada recarga
def obtener_ids_maquinas():
    try:
        # Proyectar únicamente el campo del Partition Key (device_id)
        response = table.scan(ProjectionExpression='device_id')
        items = response.get('Items', [])
        
        # Paginación en caso de que existan muchos registros
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ProjectionExpression='device_id',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
            
        # Extraer lista de IDs únicos ordenados
        ids_unicos = sorted(list(set(item['device_id'] for item in items if 'device_id' in item)))
        return ids_unicos
    except Exception as e:
        st.error(f"Error al consultar las máquinas en DynamoDB: {e}")
        return []

# Obtener lista de máquinas
maquinas_disponibles = obtener_ids_maquinas()

if not maquinas_disponibles:
    st.warning("⚠️ No se encontraron máquinas registradas en la tabla 'MaquinasTelemetriaSilver'. Esperando datos...")
    time.sleep(5)
    st.rerun()
else:
    # Selector dinámico en la barra lateral
    device_id = st.sidebar.selectbox("Seleccionar Máquina", maquinas_disponibles)

    # 2. Consultar las últimas 10 lecturas para la máquina seleccionada
    response = table.query(
        KeyConditionExpression=Key('device_id').eq(device_id),
        ScanIndexForward=False,
        Limit=10
    )
    items = response.get('Items', [])

    if items:
        ultimo = items[0]  # Lectura más reciente
        df_hist = pd.DataFrame(items)

        # Convertir datos numéricos de DynamoDB a float
        for col in ['temperatura_c', 'vibracion_mm_s', 'corriente_amp', 'health_score']:
            if col in df_hist.columns:
                df_hist[col] = df_hist[col].astype(float)

        # Mostrar metadata en la barra lateral
        st.sidebar.markdown("---")
        st.sidebar.markdown(f"**Tipo de Máquina:** {ultimo.get('tipo_maquina', 'N/A')}")
        st.sidebar.markdown(f"**Último Timestamp:** {ultimo.get('timestamp', 'N/A')}")

        # Tarjetas con los campos de la tabla
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Temperatura (°C)", float(ultimo.get('temperatura_c', 0)))
        c2.metric("Vibración (mm/s)", float(ultimo.get('vibracion_mm_s', 0)))
        c3.metric("Corriente (A)", float(ultimo.get('corriente_amp', 0)))
        c4.metric("Health Score", float(ultimo.get('health_score', 0)))

        # Evaluación del campo flag_alerta
        flag_alerta = ultimo.get('flag_alerta', False)
        if flag_alerta:
            st.error(f"🚨 ALERTA ACTIVA EN {device_id} ({ultimo.get('tipo_maquina', 'MOTOR')}): Health Score < 60 o fuera de rango.")
        else:
            st.success(f"✅ Dispositivo {device_id} ({ultimo.get('tipo_maquina', 'MOTOR')}) operando con normalidad.")

        # Gráfico histórico de las métricas
        st.subheader("📊 Histórico Reciente de Telemetría")
        cols_grafica = [c for c in ['temperatura_c', 'vibracion_mm_s', 'corriente_amp'] if c in df_hist.columns]
        if cols_grafica:
            st.line_chart(df_hist.set_index('timestamp')[cols_grafica])

        # Detalle en tabla
        with st.expander("Ver tabla de datos recientes"):
            st.dataframe(df_hist)
    else:
        st.warning(f"No se encontraron lecturas recientes para el dispositivo {device_id}.")

# Actualización automática cada 3 segundos
time.sleep(3)
st.rerun()