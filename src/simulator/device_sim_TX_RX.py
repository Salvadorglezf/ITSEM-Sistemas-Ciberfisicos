
import json
import os
import random
import ssl
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt


# ============================================================
# CONFIGURACIÓN
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

# Sustituye estos valores por los de tu cuenta AWS IoT Core.
IOT_ENDPOINT = "a1w6y3r95yz410-ats.iot.us-east-2.amazonaws.com"
CLIENT_ID = "Sensor_Planta_Principal"

CERT_FILE = BASE_DIR / "580a673012ce8e557526d95caaa32d9c36ffd9f9ac6a868275c684b83a74e523-certificate.pem.crt"
KEY_FILE = BASE_DIR / "580a673012ce8e557526d95caaa32d9c36ffd9f9ac6a868275c684b83a74e523-private.pem.key"
CA_FILE = BASE_DIR / "AmazonRootCA1.pem"

# Debe coincidir con el tópico configurado en las Lambdas.
# Se conserva el tópico utilizado en la configuración anterior.
COMMAND_TOPIC = "fabrica/maquinas/telemetria"
TELEMETRY_TOPIC = "fabrica/maquinas/telemetria"
ACK_TOPIC = COMMAND_TOPIC + "/ack"

# Número de rondas por tanda.
RONDAS_POR_EJECUCION = 2

# Segundos entre rondas.
FRECUENCIA_SEG = 15

# QoS MQTT.
MQTT_QOS = 1


# ============================================================
# DISPOSITIVOS
# ============================================================

DISPOSITIVOS = {
    "MOTOR-EL-01": {
        "tipo": "motor",
        "estado": "OPERANDO",
    },
    "ROBOT-ART-02": {
        "tipo": "robot",
        "estado": "OPERANDO",
    },
    "CONVEYOR-ALM-03": {
        "tipo": "transportador",
        "estado": "OPERANDO",
    },
}

# Protege los estados compartidos entre el hilo MQTT
# y el hilo que publica las rondas.
estado_lock = threading.Lock()

# Indica que una tanda está en ejecución.
ronda_activa = threading.Event()

# Permite finalizar el programa.
shutdown_event = threading.Event()

# Confirma que se completó la suscripción MQTT.
subscription_ready = threading.Event()


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def timestamp_utc():
    return datetime.now(timezone.utc).isoformat()


def publicar_json(topic, payload):
    """Publica JSON y comprueba si MQTT aceptó el mensaje."""

    if not client.is_connected():
        print("[MQTT] No conectado. No se publicó el mensaje.")
        return False

    try:
        result = client.publish(
            topic=topic,
            payload=json.dumps(payload, ensure_ascii=False),
            qos=MQTT_QOS,
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(
                f"[MQTT] Error al publicar en {topic}: "
                f"rc={result.rc}"
            )
            return False

        return True

    except Exception as exc:
        print(f"[MQTT] Error de publicación: {exc}")
        return False


def obtener_estado_dispositivos():
    with estado_lock:
        return {
            device_id: datos.copy()
            for device_id, datos in DISPOSITIVOS.items()
        }


# ============================================================
# TELEMETRÍA
# ============================================================

def generar_telemetria(device_id, datos):
    """Genera una lectura simulada para un dispositivo."""

    detenido = datos["estado"] == "DETENIDO"

    if detenido:
        corriente = 0.0
        rpm = 0
    else:
        corriente = round(random.uniform(2.0, 8.0), 2)
        rpm = random.randint(800, 1800)

    return {
        "device_id": device_id,
        "device_type": datos["tipo"],
        "timestamp": timestamp_utc(),
        "estado": datos["estado"],
        "corriente_a": corriente,
        "rpm": rpm,
        "temperatura_c": round(
            random.uniform(25.0, 65.0), 2
        ),
        "source": "python-simulator",
    }


def publicar_ronda(numero_ronda):
    """Publica una lectura de cada dispositivo."""

    estados = obtener_estado_dispositivos()

    print(
        f"\n[SIMULADOR] Ronda {numero_ronda}/"
        f"{RONDAS_POR_EJECUCION}"
    )

    for device_id, datos in estados.items():
        if shutdown_event.is_set():
            return

        payload = generar_telemetria(device_id, datos)

        if publicar_json(TELEMETRY_TOPIC, payload):
            print(
                f"[TELEMETRÍA] {device_id} | "
                f"Estado={payload['estado']} | "
                f"Corriente={payload['corriente_a']} A | "
                f"RPM={payload['rpm']}"
            )

    print("[SIMULADOR] Ronda publicada.")


# ============================================================
# COMANDOS DE AWS LAMBDA
# ============================================================

def publicar_ack(command_id, action, device_ids, status):
    """Envía una confirmación de recepción/procesamiento."""

    ack = {
        "command_id": command_id,
        "action": action,
        "device_ids": device_ids,
        "status": status,
        "timestamp": timestamp_utc(),
        "source": CLIENT_ID,
    }

    if publicar_json(ACK_TOPIC, ack):
        print(f"[MQTT] ACK publicado: {status}")


def procesar_comando(payload):
    """Procesa STOP_ALL y STOP_DEVICE."""

    action = payload.get("action")
    command_id = payload.get("command_id", "unknown")

    if action == "STOP_ALL":
        with estado_lock:
            for datos in DISPOSITIVOS.values():
                datos["estado"] = "DETENIDO"

        ids = list(DISPOSITIVOS.keys())

        print("\n" + "=" * 55)
        print("[COMANDO] PARO TOTAL RECIBIDO")
        print("[COMANDO] Todos los dispositivos están DETENIDOS.")
        print("=" * 55)

        publicar_ack(
            command_id=command_id,
            action=action,
            device_ids=ids,
            status="PROCESSED",
        )

    elif action == "STOP_DEVICE":
        device_id = payload.get("device_id")

        if not device_id:
            print("[COMANDO] STOP_DEVICE sin device_id.")
            return

        with estado_lock:
            if device_id not in DISPOSITIVOS:
                print(
                    f"[COMANDO] Dispositivo desconocido: "
                    f"{device_id}"
                )
                publicar_ack(
                    command_id=command_id,
                    action=action,
                    device_ids=[device_id],
                    status="UNKNOWN_DEVICE",
                )
                return

            DISPOSITIVOS[device_id]["estado"] = "DETENIDO"

        print("\n" + "=" * 55)
        print(f"[COMANDO] Paro individual: {device_id}")
        print("[COMANDO] Estado actualizado a DETENIDO.")
        print("=" * 55)

        publicar_ack(
            command_id=command_id,
            action=action,
            device_ids=[device_id],
            status="PROCESSED",
        )

    else:
        print(
            f"[MQTT] Mensaje ignorado; acción no reconocida: "
            f"{action}"
        )


# ============================================================
# CALLBACKS MQTT
# ============================================================

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Conectado a AWS IoT Core.")

        result, mid = client.subscribe(
            COMMAND_TOPIC,
            qos=MQTT_QOS,
        )

        if result == mqtt.MQTT_ERR_SUCCESS:
            print(
                f"[MQTT] Suscripción solicitada: "
                f"{COMMAND_TOPIC}, mid={mid}"
            )
        else:
            print(
                f"[MQTT] No se pudo solicitar la suscripción: "
                f"rc={result}"
            )
    else:
        print(f"[MQTT] Error de conexión: rc={rc}")


def on_subscribe(client, userdata, mid, granted_qos):
    # En la API de callbacks v1, granted_qos contiene
    # objetos MQTTMessageInfo/QoS o enteros según la versión.
    qos_values = []

    for item in granted_qos:
        qos_values.append(
            getattr(item, "value", item)
        )

    if any(qos == 128 for qos in qos_values):
        print("[MQTT] AWS rechazó la suscripción.")
        subscription_ready.clear()
        return

    subscription_ready.set()
    print(
        f"[MQTT] Suscripción confirmada. "
        f"QoS concedido: {qos_values}"
    )
    print("[MQTT] El simulador está escuchando comandos.")


def on_message(client, userdata, msg):
    try:
        raw_message = msg.payload.decode("utf-8")
        payload = json.loads(raw_message)

        # El tópico puede recibir también telemetría propia.
        # Solo los mensajes con una acción válida son comandos.
        action = payload.get("action")

        if action not in ("STOP_ALL", "STOP_DEVICE"):
            return

        print(
            f"\n[MQTT] Comando recibido en {msg.topic}: "
            f"{action}"
        )

        procesar_comando(payload)

    except json.JSONDecodeError:
        print("[MQTT] Mensaje recibido que no contiene JSON válido.")
    except Exception as exc:
        print(f"[MQTT] Error procesando el mensaje: {exc}")


def on_disconnect(client, userdata, rc):
    subscription_ready.clear()

    if rc != 0:
        print(
            f"[MQTT] Desconexión inesperada: rc={rc}. "
            "Paho intentará reconectar mientras el programa siga activo."
        )
    else:
        print("[MQTT] Desconectado.")


# ============================================================
# EJECUCIÓN DE TANDAS
# ============================================================

def ejecutar_tanda():
    """Publica una tanda y luego deja MQTT escuchando."""

    try:
        for numero in range(1, RONDAS_POR_EJECUCION + 1):
            if shutdown_event.is_set():
                break

            if not subscription_ready.is_set():
                print(
                    "[SIMULADOR] Sin suscripción activa. "
                    "Se suspende la tanda."
                )
                break

            publicar_ronda(numero)

            # Espera interrumpible por cierre del programa.
            if numero < RONDAS_POR_EJECUCION:
                shutdown_event.wait(FRECUENCIA_SEG)

        if not shutdown_event.is_set():
            print(
                "\n[SIMULADOR] Tanda terminada. "
                "El programa continúa escuchando MQTT."
            )

    finally:
        ronda_activa.clear()


def iniciar_tanda():
    if shutdown_event.is_set():
        return

    if not subscription_ready.is_set():
        print(
            "[SIMULADOR] La suscripción MQTT no está lista. "
            "No se puede iniciar una tanda."
        )
        return

    # Evita iniciar dos tandas simultáneamente.
    if ronda_activa.is_set():
        print("[SIMULADOR] Ya hay una tanda en ejecución.")
        return

    ronda_activa.set()

    hilo = threading.Thread(
        target=ejecutar_tanda,
        name="HiloTelemetria",
        daemon=True,
    )
    hilo.start()


# ============================================================
# CONSOLA
# ============================================================

def mostrar_estado():
    estados = obtener_estado_dispositivos()

    print("\n[ESTADO DE DISPOSITIVOS]")

    for device_id, datos in estados.items():
        print(f"  {device_id}: {datos['estado']}")

    print(
        f"  Tanda activa: "
        f"{'Sí' if ronda_activa.is_set() else 'No'}"
    )
    print(
        f"  MQTT conectado: "
        f"{'Sí' if client.is_connected() else 'No'}"
    )


def consola():
    print("\n" + "=" * 55)
    print("SIMULADOR MQTT - AWS IoT CORE")
    print("=" * 55)
    print("R  = Enviar una nueva tanda de rondas")
    print("E  = Consultar estado de los dispositivos")
    print("Q  = Salir")
    print("=" * 55)

    while not shutdown_event.is_set():
        try:
            opcion = input("\nSelecciona una opción: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            opcion = "q"

        if opcion == "r":
            iniciar_tanda()

        elif opcion == "e":
            mostrar_estado()

        elif opcion == "q":
            shutdown_event.set()
            print("[SIMULADOR] Cerrando el programa...")
            break

        else:
            print("[CONSOLA] Opción no válida. Usa R, E o Q.")


# ============================================================
# INICIO DEL PROGRAMA
# ============================================================

def main():
    global client

    # Validar configuración y certificados.
    if "TU_ENDPOINT" in IOT_ENDPOINT or "TU_REGION" in IOT_ENDPOINT:
        raise ValueError(
            "Configura IOT_ENDPOINT con el endpoint real de AWS IoT Core."
        )

    for path in (CERT_FILE, KEY_FILE, CA_FILE):
        if not path.is_file():
            raise FileNotFoundError(
                f"No se encontró el archivo requerido: {path}"
            )

    # Compatibilidad con Paho MQTT 1.x y 2.x.
    try:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
            client_id=CLIENT_ID,
        )
    except (AttributeError, TypeError):
        client = mqtt.Client(client_id=CLIENT_ID)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.tls_set(
        ca_certs=str(CA_FILE),
        certfile=str(CERT_FILE),
        keyfile=str(KEY_FILE),
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )

    client.reconnect_delay_set(min_delay=1, max_delay=30)

    try:
        print("[MQTT] Conectando a AWS IoT Core...")
        client.connect(
            IOT_ENDPOINT,
            port=8883,
            keepalive=60,
        )

        # Inicia el hilo MQTT; este procesa mensajes y callbacks
        # mientras la consola espera entradas de teclado.
        client.loop_start()

        if not subscription_ready.wait(timeout=20):
            raise TimeoutError(
                "No se confirmó la suscripción MQTT en 20 segundos. "
                "Verifica endpoint, certificado y política IoT."
            )

        # La consola permanece activa indefinidamente.
        consola()

    except KeyboardInterrupt:
        print("\n[SIMULADOR] Interrupción recibida.")

    finally:
        shutdown_event.set()

        if client is not None:
            try:
                if client.is_connected():
                    client.disconnect()
            finally:
                client.loop_stop()

        print("[SIMULADOR] Programa finalizado.")


if __name__ == "__main__":
    client = None
    main()
