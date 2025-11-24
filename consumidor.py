import time
import json
import random
import pika
import grpc
from concurrent import futures
import montecarlo_pb2
import montecarlo_pb2_grpc

class MonteCarloService(montecarlo_pb2_grpc.MonteCarloServiceServicer):
    def Handshake(self, request, context):
        print(f"[gRPC] Handshake recibido de {request.worker_id}")

        return montecarlo_pb2.HandshakeReply(
            message="Handshake OK",
            accepted=True,
            puntos_por_lote=5000   # <--- valor recomendado
        )


class Consumidor:
    """
    Clase Consumidor:
    1. Lee parámetros del modelo de la 'cola_parametros_modelo'.
    2. Consume escenarios de la 'cola_escenarios'.
    3. Ejecuta la estimación de Pi.
    4. Publica el resultado en la 'cola_resultados'.
    """

    RABBITMQ_HOST = 'localhost'
    COLA_ESCENARIOS = 'cola_escenarios'
    COLA_MODELO = 'cola_parametros_modelo'
    COLA_RESULTADOS = 'cola_resultados'
    
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.RABBITMQ_HOST))
        self.channel = self.connection.channel()
        print("✅ Consumidor inicializado.")
        
        # 1. Leer Parámetros del Modelo (Asignación a variable interna)
        self.parametros_modelo = self._obtener_parametros_modelo()

        # Asegurar las colas
        self.channel.queue_declare(queue=self.COLA_ESCENARIOS, durable=True)
        self.channel.queue_declare(queue=self.COLA_RESULTADOS, durable=True)

    # --- Método de Estimación de Pi (Montecarlo) ---
    def _estimar_pi_montecarlo(self, points):
        """Función de estimación de Pi basada en Montecarlo."""
        points_in_circle = 0
        
        # Bucle de simulación: se generan 'points' puntos aleatorios (x, y) entre [0, 1)
        for _ in range(points):
            x = random.random()
            y = random.random()
            
            # Condición para verificar si el punto cae dentro del círculo de radio 1: x^2 + y^2 < 1
            if x*x + y*y <= 1:
                points_in_circle += 1
                
        # Fórmula: π ≈ 4 * (puntos_dentro_círculo / puntos_totales)
        factor = self.parametros_modelo.get('factor_multiplicador', 4)
        pi_estimate = factor * (points_in_circle / points)
        return pi_estimate

    # --- Manejo de Colas ---
    
    def _obtener_parametros_modelo(self):
        """Lee una sola vez de la cola de parámetros de modelo."""
        method_frame, properties, body = self.channel.basic_get(queue=self.COLA_MODELO, auto_ack=True)
        
        if body:
            params = json.loads(body.decode())
            print(f"📥 [CONSUMIDOR] Parámetros del modelo leídos: {params.get('version')}")
            return params
        else:
            print("⚠️ [CONSUMIDOR] Cola de modelo vacía. Usando parámetros por defecto.")
            return {"version": "default_v0.0", "description": "Default Monte Carlo", "factor_multiplicador": 4}

    def _publicar_resultados(self, scenario_id, pi_estimate, points, duration):
        """Publica el resultado en la 'cola_resultados'."""
        result_data = {
            'scenario_id': scenario_id,
            'puntos_simulados': points,
            'pi_estimado': pi_estimate,
            'tiempo_segundos': duration,
            'modelo_usado': self.parametros_modelo['version']
        }
        message = json.dumps(result_data)
        
        self.channel.basic_publish(
            exchange='',
            routing_key=self.COLA_RESULTADOS,
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
        )
        print(f"⬆️  [CONSUMIDOR] Resultado publicado para ID={scenario_id}. $\pi \\approx {pi_estimate:.6f}$")

    def iniciar_consumo(self):
        """Configura y comienza a consumir mensajes de la 'cola_escenarios'."""
        
        def callback_escenario(ch, method, properties, body):
            """Función callback ejecutada al recibir un nuevo escenario."""
            scenario = json.loads(body.decode())
            scenario_id = scenario['scenario_id']
            points = scenario['points']
            
            print(f"\n⚙️  [CONSUMIDOR] Procesando escenario ID={scenario_id} con {points:,} puntos...")
            
            # Ejecución del modelo
            start_time = time.time()
            pi_estimate = self._estimar_pi_montecarlo(points)
            end_time = time.time()
            duration = end_time - start_time
            
            # Publicar resultados
            self._publicar_resultados(scenario_id, pi_estimate, points, duration)
            
            # Reconocimiento (ACK) para remover el mensaje de la cola
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # Configurar el Consumidor
        self.channel.basic_qos(prefetch_count=1) # Procesa un mensaje a la vez
        self.channel.basic_consume(queue=self.COLA_ESCENARIOS, on_message_callback=callback_escenario)

        print(f"\n✨ Esperando mensajes en '{self.COLA_ESCENARIOS}'. Para salir, presione CTRL+C.")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("\nProceso de Consumidor detenido.")
            self.cerrar_conexion()


    def cerrar_conexion(self):
        self.connection.close()
        print("❌ Conexión del Consumidor cerrada.")



if __name__ == "__main__":
    def iniciar_grpc_server():
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        montecarlo_pb2_grpc.add_MonteCarloServiceServicer_to_server(
            MonteCarloService(), server
        )
        server.add_insecure_port('[::]:50051')
        server.start()
        print("🚀 gRPC Server iniciado en puerto 50051")
        return server
    
    # Iniciar el servidor gRPC en un hilo separado
    grpc_server = iniciar_grpc_server()

    print("--- 👂 INICIANDO CONSUMIDOR (Escuchando y Procesando) ---")

    # Crea la instancia del Consumidor
    consumidor = Consumidor()

    # Inicia el consumo (es BLOQUEANTE)
    consumidor.iniciar_consumo()

    grpc_server.wait_for_termination()
    
    # Nota: El código siguiente solo se ejecutará si se detiene el consumo (ej. Ctrl+C)
    print("Consumidor detenido.")