import pika
import json
import time
import sys
import uuid

RABBIT_HOST = 'localhost'

class WorkerMontecarlo:
    def __init__(self, host):
        self.host = host
        self.cola_tareas = 'cola_escenarios'
        self.cola_modelo = 'cola_modelo'
        self.cola_resultados = 'cola_resultados'
        
        self.connection = None
        self.channel = None
        self.funcion_modelo = None # Aquí guardaremos el código dinámico
        self.worker_id = f"Worker-{str(uuid.getnode())[-4:]}"

    def conectar(self):
        print(f" [*] Worker {self.worker_id} conectando a {self.host}...")
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.cola_tareas, durable=True)
            self.channel.queue_declare(queue=self.cola_modelo, durable=True)
            self.channel.queue_declare(queue=self.cola_resultados, durable=True)
        except Exception as e:
            print(f" [!] Error fatal conectando: {e}")
            time.sleep(5)
            sys.exit(1)

    def obtener_modelo(self):
        """Intenta descargar el modelo de la cola específica (una vez)"""
        print(" [?] Buscando modelo de simulación...")
        while self.funcion_modelo is None:
            method, header, body = self.channel.basic_get(queue=self.cola_modelo)
            
            if method:
                print(" [x] Modelo recibido. Compilando en memoria...")
                codigo = body.decode('utf-8')
                
                # Compilación dinámica (exec) dentro del contexto global
                try:
                    exec(codigo, globals())
                    # Asumimos que el código define una función llamada 'modelo'
                    self.funcion_modelo = modelo 
                    self.channel.basic_ack(method.delivery_tag)
                    return True
                except Exception as e:
                    print(f" [!] Error compilando modelo: {e}")
                    self.channel.basic_nack(method.delivery_tag)
            else:
                print(" ... Esperando modelo (retrying in 3s) ...")
                time.sleep(3)

    def _callback_tarea(self, ch, method, properties, body):
        """Manejador de mensajes de tarea"""
        try:
            datos = json.loads(body)
            
            # Validación simple
            if not isinstance(datos, dict) or 'iteraciones' not in datos:
                print(f" [!] Mensaje inválido ignorado.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            iteraciones = datos['iteraciones']
            id_esc = datos.get('id_escenario', 'unk')
            
            print(f" [>] Ejecutando Escenario {id_esc} ({iteraciones} it)...")
            
            # EJECUCIÓN DEL MODELO (POO: self.funcion_modelo)
            aciertos = self.funcion_modelo(iteraciones)
            
            # Publicar resultado
            resultado = {
                'id_escenario': id_esc,
                'iteraciones_totales': iteraciones,
                'aciertos': aciertos,
                'worker_id': self.worker_id
            }
            
            ch.basic_publish(
                exchange='',
                routing_key=self.cola_resultados,
                body=json.dumps(resultado),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f" [!] Error procesando tarea: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def iniciar_consumo(self):
        # Configurar QoS
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.cola_tareas, on_message_callback=self._callback_tarea)
        print(" [*] Esperando tareas... (CTRL+C para salir)")
        self.channel.start_consuming()

if __name__ == '__main__':
    worker = WorkerMontecarlo(RABBIT_HOST)
    worker.conectar()
    
    # Paso 1: Obtener Modelo
    worker.obtener_modelo()
    
    # Paso 2: Loop infinito de trabajo
    try:
        worker.iniciar_consumo()
    except KeyboardInterrupt:
        print("\n [!] Deteniendo worker...")