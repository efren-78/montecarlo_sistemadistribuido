import pika
import json
import threading
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

# Configuración RabbitMQ
RABBIT_HOST = 'localhost'
COLA_RESULTADOS = 'cola_resultados'

# ==========================================
# Estado Global (Memoria compartida entre hilos)
# ==========================================
datos_grafica = {
    'x_iteraciones': [],    # Eje X: Cantidad de puntos totales
    'y_pi': [],             # Eje Y: Valor de Pi calculado
    'workers': {},          # Diccionario para contar tareas por worker {'w1': 5, 'w2': 3}
    'total_aciertos': 0,
    'total_puntos': 0,
    'escenarios_procesados': 0
}

# Usamos un estilo bonito para las gráficas
style.use('fivethirtyeight')

# Inicializamos la figura de Matplotlib
fig = plt.figure(figsize=(10, 8))
ax1 = fig.add_subplot(2, 1, 1) # Gráfico de arriba (Línea Pi)
ax2 = fig.add_subplot(2, 1, 2) # Gráfico de abajo (Barras Workers)

# ==========================================
# Hilo de RabbitMQ (Escucha en segundo plano)
# ==========================================
def hilo_consumidor():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=COLA_RESULTADOS, durable=True)

    def callback(ch, method, properties, body):
        mensaje = json.loads(body)
        
        # 1. Actualizar Datos Globales
        iteraciones = mensaje['iteraciones_totales']
        aciertos = mensaje['aciertos']
        worker_id = mensaje.get('worker_id', 'Desconocido')

        datos_grafica['total_puntos'] += iteraciones
        datos_grafica['total_aciertos'] += aciertos
        datos_grafica['escenarios_procesados'] += 1
        
        # Actualizar conteo por worker
        if worker_id in datos_grafica['workers']:
            datos_grafica['workers'][worker_id] += 1
        else:
            datos_grafica['workers'][worker_id] = 1

        # Calcular Pi actual y guardarlo para el histórico
        if datos_grafica['total_puntos'] > 0:
            pi_estimado = 4 * (datos_grafica['total_aciertos'] / datos_grafica['total_puntos'])
            
            # Guardamos datos para plotear
            # (Para evitar que la gráfica explote, solo guardamos cada cierto tiempo o todos)
            datos_grafica['x_iteraciones'].append(datos_grafica['total_puntos'])
            datos_grafica['y_pi'].append(pi_estimado)

        # Confirmar a RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)

    print(" [*] Dashboard conectado a RabbitMQ. Esperando datos...")
    channel.basic_consume(queue=COLA_RESULTADOS, on_message_callback=callback)
    channel.start_consuming()

# ==========================================
# Función de Animación (Se ejecuta cada 1000ms)
# ==========================================
def animar(i):
    # --- Gráfico 1: Convergencia de PI ---
    ax1.clear()
    if datos_grafica['y_pi']:
        ax1.plot(datos_grafica['x_iteraciones'], datos_grafica['y_pi'], label='Pi Estimado', color='#007acc')
        valor_actual = datos_grafica['y_pi'][-1]
    else:
        valor_actual = 0
        
    # Línea de referencia (Pi Real)
    ax1.axhline(y=3.14159265, color='r', linestyle='--', linewidth=1, label='Pi Real')
    
    ax1.set_title(f"Estimación de Pi en Tiempo Real: {valor_actual:.5f}")
    ax1.set_ylabel("Valor Calculado")
    ax1.legend(loc='upper right')

    # --- Gráfico 2: Carga de Trabajo por Worker ---
    ax2.clear()
    workers = list(datos_grafica['workers'].keys())
    tareas = list(datos_grafica['workers'].values())
    
    # Barras de colores
    colores = ['#2ecc71', '#e74c3c', '#9b59b6', '#f1c40f']
    ax2.bar(workers, tareas, color=colores[:len(workers)])
    
    ax2.set_title(f"Escenarios Procesados por Cliente (Total: {datos_grafica['escenarios_procesados']})")
    ax2.set_xlabel("ID del Cliente (Worker)")
    ax2.set_ylabel("Escenarios Completados")

# ==========================================
# Main
# ==========================================
if __name__ == '__main__':
    # 1. Iniciar el hilo de RabbitMQ (Daemon para que muera al cerrar la ventana)
    t = threading.Thread(target=hilo_consumidor)
    t.daemon = True
    t.start()

    # 2. Iniciar la Interfaz Gráfica
    # interval=1000 significa que actualiza cada 1 segundo
    ani = animation.FuncAnimation(fig, animar, interval=1000)
    plt.tight_layout()
    plt.show()