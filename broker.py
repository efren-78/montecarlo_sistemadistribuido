import pika

COLA_ESCENARIOS = "cola_escenarios"
COLA_RESULTADOS = "cola_resultados"

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    channel = connection.channel()

    channel.queue_declare(queue=COLA_ESCENARIOS, durable=True)
    channel.queue_declare(queue=COLA_RESULTADOS, durable=True)

    print("Broker listo. Colas creadas:")
    print(f"- {COLA_ESCENARIOS}")
    print(f"- {COLA_RESULTADOS}")

    print("Broker en espera... (Ctrl+C para salir)")
    try:
        channel.start_consuming()  
    except KeyboardInterrupt:
        connection.close()
        print("Broker detenido.")

if __name__ == "__main__":
    main()
