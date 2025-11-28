import pika
import json
import threading
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

RABBIT_HOST = 'localhost'

class DashboardVisual:
    def __init__(self, host):
        self.host = host
        self.cola_resultados = 'cola_resultados'
        
        # Estado del Dashboard (Datos)
        self.datos = {
            'x_iter': [],
            'y_pi': [],
            'workers': {},
            'total_aciertos': 0,
            'total_puntos': 0,
            'escenarios_count': 0
        }
        
        # Configuración de Matplotlib
        style.use('fivethirtyeight')
        self.fig = plt.figure(figsize=(10, 8))
        self.ax1 = self.fig.add_subplot(2, 1, 1)
        self.ax2 = self.fig.add_subplot(2, 1, 2)

    def _hilo_consumidor(self):
        """Método privado que corre en un hilo background"""
        
        # 1. Definimos las credenciales (usuario, contraseña)
        credentials = pika.PlainCredentials('efren', 'efren')

        # 2. Creamos la conexión usándolas
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host,
                credentials=credentials  # <--- AGREGAR ESTO
            )
        )
        
        channel = connection.channel()
        channel.queue_declare(queue=self.cola_resultados, durable=True)

        def callback(ch, method, properties, body):
            msg = json.loads(body)
            
            # Actualizar estado interno
            its = msg['iteraciones_totales']
            hits = msg['aciertos']
            wid = msg.get('worker_id', 'Unknown')
            
            self.datos['total_puntos'] += its
            self.datos['total_aciertos'] += hits
            self.datos['escenarios_count'] += 1
            self.datos['workers'][wid] = self.datos['workers'].get(wid, 0) + 1
            
            # Guardar punto para la gráfica de línea
            if self.datos['total_puntos'] > 0:
                pi_calc = 4 * (self.datos['total_aciertos'] / self.datos['total_puntos'])
                self.datos['x_iter'].append(self.datos['total_puntos'])
                self.datos['y_pi'].append(pi_calc)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)

        print(" [*] Dashboard escuchando RabbitMQ...")
        channel.basic_consume(queue=self.cola_resultados, on_message_callback=callback)
        channel.start_consuming()

    def _animar(self, i):
        """Función de refresco para Matplotlib"""
        # Gráfica 1: Convergencia
        self.ax1.clear()
        if self.datos['y_pi']:
            self.ax1.plot(self.datos['x_iter'], self.datos['y_pi'], label='Pi Estimado', color='#007acc')
            val = self.datos['y_pi'][-1]
        else:
            val = 0
        
        self.ax1.axhline(y=3.14159265, color='r', linestyle='--', linewidth=1, label='Pi Real')
        self.ax1.set_title(f"Estimación de Pi: {val:.6f}")
        self.ax1.legend(loc='upper right')

        # Gráfica 2: Workers
        self.ax2.clear()
        names = list(self.datos['workers'].keys())
        counts = list(self.datos['workers'].values())
        self.ax2.bar(names, counts, color=['#2ecc71', '#e74c3c', '#f1c40f'])
        self.ax2.set_title(f"Carga por Worker (Total: {self.datos['escenarios_count']})")

    def iniciar(self):
        # 1. Lanzar hilo de datos
        t = threading.Thread(target=self._hilo_consumidor)
        t.daemon = True
        t.start()
        
        # 2. Lanzar GUI (Bloqueante)
        ani = animation.FuncAnimation(self.fig, self._animar, interval=1000)
        plt.tight_layout()
        plt.show()

if __name__ == '__main__':
    dash = DashboardVisual(RABBIT_HOST)
    dash.iniciar()