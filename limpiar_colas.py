# limpiar_colas.py
import pika

RABBIT_HOST = 'localhost'

def purgar():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()

    # Nombres de las colas que hemos usado
    colas_a_limpiar = ['cola_escenarios', 'cola_modelo', 'cola_resultados', 'cola_montecarlo_tareas']

    print("--- Iniciando Limpieza de RabbitMQ ---")
    
    for cola in colas_a_limpiar:
        try:
            # purge_queue elimina mensajes pero mantiene la cola viva
            method_frame = channel.queue_purge(queue=cola)
            print(f" [x] Purgada '{cola}': {method_frame.message_count} mensajes eliminados.")
        except pika.exceptions.ChannelClosedByBroker:
            # Si la cola no existe, ignoramos el error
            print(f" [!] La cola '{cola}' no existía (o ya se borró).")
            # Necesitamos reabrir la conexión si hubo error
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
        except Exception as e:
            print(f" [?] Nota sobre '{cola}': {e}")

    connection.close()
    print("--- Limpieza completada. Ya puedes correr el sistema ---")

if __name__ == '__main__':
    purgar()