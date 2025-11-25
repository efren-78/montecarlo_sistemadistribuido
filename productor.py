import pika
import json
import uuid

RABBIT_HOST = 'localhost'
COLA_TAREAS = 'cola_escenarios'
COLA_MODELO = 'cola_modelo'

# ---------------------------------------------------------
# ESTE ES EL MODELO (LA FUNCIÓN) QUE ENVIAREMOS POR LA RED
# ---------------------------------------------------------
codigo_modelo_montecarlo = """
import random
def modelo(iteraciones):
    aciertos = 0
    for _ in range(iteraciones):
        x = random.random()
        y = random.random()
        if x*x + y*y <= 1.0:
            aciertos += 1
    return aciertos
"""

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()

    # 1. Declarar colas
    channel.queue_declare(queue=COLA_TAREAS, durable=True)
    channel.queue_declare(queue=COLA_MODELO, durable=True)

    # 2. PUBLICAR EL MODELO (LA FUNCIÓN)
    # Requisito de imagen: "Cola específica", "time-out delivery" (TTL)
    # Expiración: 60000ms (1 minuto) como ejemplo de time-out
    print(f" [x] Publicando código del modelo en '{COLA_MODELO}'...")
    
    channel.basic_publish(
        exchange='',
        routing_key=COLA_MODELO,
        body=codigo_modelo_montecarlo,
        properties=pika.BasicProperties(
            delivery_mode=2, 
            expiration='60000'  # El mensaje expira si nadie lo lee en 60s
        )
    )

    # 3. PUBLICAR LOS ESCENARIOS (Lectura de archivo)
    print(f" [x] Leyendo 'escenarios.txt' y publicando tareas...")
    try:
        with open('escenarios.txt', 'r') as f:
            lineas = f.readlines()
            
        for linea in lineas:
            iteraciones = int(linea.strip())
            if iteraciones <= 0: continue

            mensaje = {
                'id_escenario': str(uuid.uuid4())[:8],
                'iteraciones': iteraciones
            }
            
            channel.basic_publish(
                exchange='',
                routing_key=COLA_TAREAS,
                body=json.dumps(mensaje),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"   -> Escenario enviado: {iteraciones} iteraciones")

    except FileNotFoundError:
        print("Error: Crea un archivo 'escenarios.txt' primero.")
    
    connection.close()

if __name__ == '__main__':
    main()