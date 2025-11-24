# main.py

import threading
import time
import pika
from broker import Broker
from productor import Productor
from consumidor import Consumidor

# Asume que las clases Broker, Productor, y Consumidor están importadas o definidas aquí

def ejecutar_consumidor(consumidor_instance):
    """Función objetivo del hilo para ejecutar el Consumidor."""
    print("▶️ [HILO] Iniciando el Consumidor en segundo plano...")
    consumidor_instance.iniciar_consumo()
    # Nota: esta línea es bloqueante y solo terminará con Ctrl+C o error.

def main():
    # --- 1. Inicializar el Broker y asegurar colas ---
    print("--- FASE 1: Configuración del Broker ---")
    broker = Broker()
    # El Broker publica los parámetros del modelo al inicializarse.
    broker.cerrar_conexion()
    time.sleep(1) # Pequeña pausa para asegurar la inicialización de colas

    # --- 2. Iniciar el Consumidor en un Hilo ---
    print("\n--- FASE 2: Inicio del Consumidor ---")
    consumidor = Consumidor()
    
    # Crear un hilo para ejecutar el método de consumo
    hilo_consumidor = threading.Thread(target=ejecutar_consumidor, args=(consumidor,))
    # Permite que el programa principal termine incluso si este hilo sigue activo
    hilo_consumidor.daemon = True 
    hilo_consumidor.start()
    
    # Pausa para dar tiempo al Consumidor para conectarse y empezar a escuchar
    time.sleep(3) 

    # --- 3. Ejecutar el Productor ---
    print("\n--- FASE 3: Ejecución del Productor ---")
    productor = Productor(num_escenarios=5)
    productor.generar_y_publicar_escenarios()
    productor.cerrar_conexion()

    # --- 4. Esperar la Terminación (Opcional) ---
    print("\n--- FASE 4: Esperando la finalización de las tareas ---")
    # Si quieres que el programa espere a que el Consumidor procese los 5 mensajes:
    # Debes usar un mecanismo más avanzado (ej. monitorear la cola de resultados).
    
    # Por ahora, simplemente mantenemos el hilo principal vivo un momento
    # para ver el procesamiento de los 5 mensajes.
    time.sleep(10)
    
    print("\nPrograma terminado. Los mensajes restantes podrían seguir en la cola.")

if __name__ == "__main__":
    main()