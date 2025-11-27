import pika
import json
import uuid
import sys

# ==========================================
# Configuración de Conexión
# ==========================================
RABBIT_HOST = 'localhost' 

class ProductorMontecarlo:
    def __init__(self, host):
        self.host = host
        self.cola_tareas = 'cola_escenarios'
        self.cola_modelo = 'cola_modelo'
        self.connection = None
        self.channel = None

    def conectar(self):
        """Establece conexión con el Broker"""
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            # Declaramos las colas para asegurar que existen
            self.channel.queue_declare(queue=self.cola_tareas, durable=True)
            self.channel.queue_declare(queue=self.cola_modelo, durable=True)
            print(f" [*] Conectado al Broker en {self.host}")
        except Exception as e:
            print(f" [!] Error de conexión: {e}")
            sys.exit(1)

    def publicar_modelo(self, codigo_fuente):
        """Publica el código móvil (función) con TTL"""
        print(f" [x] Publicando modelo dinámico...")
        
        self.channel.basic_publish(
            exchange='',
            routing_key=self.cola_modelo,
            body=codigo_fuente,
            properties=pika.BasicProperties(
                delivery_mode=2,
                expiration='60000' # Time-out delivery (60s)
            )
        )

    def procesar_archivo_escenarios(self, nombre_archivo):
        """Lee el archivo y publica cada línea como una tarea"""
        print(f" [x] Leyendo '{nombre_archivo}'...")
        try:
            with open(nombre_archivo, 'r') as f:
                lineas = f.readlines()
                
            count = 0
            for linea in lineas:
                try:
                    iteraciones = int(linea.strip())
                    if iteraciones > 0:
                        self._enviar_tarea(iteraciones)
                        count += 1
                except ValueError:
                    continue
            print(f" [ok] Se enviaron {count} escenarios al sistema.")
            
        except FileNotFoundError:
            print(f" [!] Error: No se encontró el archivo '{nombre_archivo}'")

    def _enviar_tarea(self, iteraciones):
        """Método privado para enviar una sola tarea"""
        mensaje = {
            'id_escenario': str(uuid.uuid4())[:8],
            'iteraciones': iteraciones
        }
        self.channel.basic_publish(
            exchange='',
            routing_key=self.cola_tareas,
            body=json.dumps(mensaje),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"   -> Escenario enviado: {iteraciones} it")

    def cerrar(self):
        if self.connection:
            self.connection.close()

# --- LÓGICA DEL MODELO ---
CODIGO_MODELO = """
import random
def modelo(iteraciones):
    aciertos = 0
    for _ in range(iteraciones):
        x = random.random()
        y = random.random()
        if (x*x + y*y) <= 1.0:
            aciertos += 1
    return aciertos
"""

if __name__ == '__main__':
    # Instanciamos el OBJETO Productor
    productor = ProductorMontecarlo(RABBIT_HOST)
    productor.conectar()
    
    # Usamos sus MÉTODOS
    productor.publicar_modelo(CODIGO_MODELO)
    productor.procesar_archivo_escenarios('escenarios.txt')
    
    productor.cerrar()