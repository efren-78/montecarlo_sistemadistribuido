import pika
import grpc
import json
import random
import time

import montecarlo_pb2
import montecarlo_pb2_grpc

COLA_ESCENARIOS = "cola_escenarios"

def generar_puntos(n):
    puntos = []
    for _ in range(n):
        x = random.random()
        y = random.random()
        puntos.append({"x": x, "y": y})
    return puntos

def main():
    # --- gRPC Handshake ---
    channel = grpc.insecure_channel("localhost:50051")
    stub = montecarlo_pb2_grpc.MonteCarloServiceStub(channel)

    respuesta = stub.Handshake(
        montecarlo_pb2.HandshakeRequest(worker_id="worker_1")
    )

    lote = respuesta.puntos_por_lote
    print(f"Productor: Lote recibido = {lote} puntos")

    # --- Conexión RabbitMQ ---
    mq_conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    mq_ch = mq_conn.channel()
    mq_ch.queue_declare(queue=COLA_ESCENARIOS, durable=True)

    print("Productor listo. Enviando puntos...")

    while True:
        puntos = generar_puntos(lote)
        mensaje = json.dumps(puntos)

        mq_ch.basic_publish(
            exchange="",
            routing_key=COLA_ESCENARIOS,
            body=mensaje,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        print(f"Productor: Enviado lote de {lote} puntos")
        time.sleep(0.2)

if __name__ == "__main__":
    main()
