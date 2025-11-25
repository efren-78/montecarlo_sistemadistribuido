# monitor.py (El Agregador de Resultados)
import pika
import json

RABBIT_HOST = 'localhost'
COLA_RESULTADOS = 'cola_resultados'

total_puntos = 0
total_aciertos = 0

def callback(ch, method, properties, body):
    global total_puntos, total_aciertos
    
    resultado = json.loads(body)
    
    # Extraemos los datos que envió el consumidor
    iteraciones = resultado['iteraciones_totales']
    aciertos = resultado['aciertos']
    id_escenario = resultado['id_escenario']
    
    # Acumulamos para el cálculo global de PI
    total_puntos += iteraciones
    total_aciertos += aciertos
    
    # Evitamos división por cero al inicio
    if total_puntos > 0:
        pi_estimado = 4 * (total_aciertos / total_puntos)
        error = abs(3.1415926535 - pi_estimado)
        
        print(f" [R] Escenario {id_escenario}: {aciertos}/{iteraciones} aciertos.")
        print(f"     >>> PI GLOBAL ESTIMADO: {pi_estimado:.6f} (Error: {error:.6f})")
        print("     ---------------------------------------------------")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()
    
    # Declaramos la cola para asegurar que existe
    channel.queue_declare(queue=COLA_RESULTADOS, durable=True)

    print(' [*] Monitor iniciado. Esperando resultados de cálculos...')
    print(' [*] Presiona CTRL+C para salir.')
    
    channel.basic_consume(queue=COLA_RESULTADOS, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nMonitor detenido.")