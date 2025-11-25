import pika
import json
import sys
import os
import uuid

RABBIT_HOST = 'localhost'
COLA_TAREAS = 'cola_escenarios'
COLA_MODELO = 'cola_modelo'
COLA_RESULTADOS = 'cola_resultados'

# Variable global donde guardaremos la función recibida dinámicamente
funcion_modelo = None

def callback_tareas(ch, method, properties, body):
    global funcion_modelo
    
    # 1. Verificación de seguridad: ¿Tenemos el modelo cargado?
    if not funcion_modelo:
        print(" [!] Recibí tarea pero no tengo modelo. Reencolando...")
        # Nack devuelve el mensaje a la cola para que otro worker (o yo mismo luego) lo tome
        ch.basic_nack(delivery_tag=method.delivery_tag)
        return

    try:
        # 2. Intentar decodificar el mensaje
        tarea = json.loads(body)
        
        # --- BLOQUE DEFENSIVO (OPCIÓN B) ---
        # Verificamos que sea un diccionario y tenga la clave 'iteraciones'
        if not isinstance(tarea, dict) or 'iteraciones' not in tarea:
            print(f" [!] ALERTA: Mensaje 'basura' ignorado (formato incorrecto): {body}")
            
            # ¡IMPORTANTE! Hacemos ACK para eliminar este mensaje corrupto de RabbitMQ
            # Si no hacemos esto, el mensaje se queda ahí y volverá a romper el programa.
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        # -----------------------------------

        # 3. Si llegamos aquí, el mensaje es válido. Procesamos.
        iteraciones = tarea['iteraciones']
        # Usamos .get por seguridad, si no trae ID, ponemos 'sin_id'
        id_escenario = tarea.get('id_escenario', 'sin_id') 
        
        print(f" [>] Procesando escenario {id_escenario} ({iteraciones} it)...")

        # 4. EJECUTAR EL MODELO DINÁMICO
        aciertos = funcion_modelo(iteraciones)

        # 5. Publicar Resultado
        resultado = {
            'id_escenario': id_escenario,
            'iteraciones_totales': iteraciones,
            'aciertos': aciertos,
            # Generamos un ID único si no existe, o usamos uno fijo por terminal
            'worker_id': f"Worker-{str(uuid.getnode())[-4:]}" 
        }
        
        # Declaramos la cola por si acaso no existiera
        ch.queue_declare(queue=COLA_RESULTADOS, durable=True)
        
        ch.basic_publish(
            exchange='',
            routing_key=COLA_RESULTADOS,
            body=json.dumps(resultado),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        # 6. Confirmar tarea procesada exitosamente
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f" [X] Error CRÍTICO procesando mensaje: {e}")
        # En caso de error de código (ej. división por cero), también sacamos el mensaje
        # para que no genere un bucle infinito de errores.
        ch.basic_ack(delivery_tag=method.delivery_tag)


def obtener_modelo(channel):
    """
    Requisito de imagen: "leer el modelo de la cola de modelo (una vez)"
    """
    print(" [*] Buscando modelo en la cola...")
    
    # Usamos basic_get para leer UNA sola vez, no basic_consume loop
    method_frame, header_frame, body = channel.basic_get(queue=COLA_MODELO)
    
    if method_frame:
        print(" [x] Modelo recibido. Cargando lógica en memoria...")
        codigo_python = body.decode('utf-8')
        
        # MAGIA DINÁMICA: Ejecutamos el string como código Python
        # Esto crea la función 'def modelo(iteraciones):' en nuestro entorno local
        exec(codigo_python, globals())
        
        # Asignamos la función creada a nuestra variable
        global funcion_modelo
        funcion_modelo = modelo 
        
        # Confirmamos que recibimos el modelo (ACK)
        channel.basic_ack(method_frame.delivery_tag)
        return True
    else:
        print(" [!] La cola de modelos está vacía. Esperando...")
        return False

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=COLA_TAREAS, durable=True)
    channel.queue_declare(queue=COLA_MODELO, durable=True)
    
    # 1. BLOQUEAR HASTA OBTENER EL MODELO (Paso 1 de la Imagen)
    modelo_cargado = False
    while not modelo_cargado:
        modelo_cargado = obtener_modelo(channel)
        if not modelo_cargado:
            import time
            time.sleep(2) # Reintentar cada 2 segundos

    # 2. UNA VEZ CON MODELO, PROCESAR ESCENARIOS
    channel.basic_qos(prefetch_count=1)
    print(' [*] Modelo cargado. Esperando escenarios...')
    
    channel.basic_consume(queue=COLA_TAREAS, on_message_callback=callback_tareas)
    channel.start_consuming()

if __name__ == '__main__':
    main()