# Simulación Distribuida de Montecarlo 
Este proyecto implementa un sistema distribuido para estimar el valor de **Pi** utilizando el método de Montecarlo. El sistema está diseñado bajo el paradigma de **Programación Orientada a Objetos (POO)** y utiliza **RabbitMQ** como middleware para la comunicación asíncrona entre nodos.

## 1. Arquitectura del Sistema

El sistema sigue el patrón Productor-Consumidor con monitoreo en tiempo real:

* **RabbitMQ (Broker):** Middleware que gestiona las colas de mensajes y garantiza la entrega.
* **Productor (Master):**
    * Lee la carga de trabajo desde `escenarios.txt`.
    * Publica el código fuente de la función matemática en `cola_modelo` (Código Móvil).
    * Distribuye las tareas en `cola_escenarios`.
* **Consumidor (Worker):**
    * Descarga y compila dinámicamente el modelo desde `cola_modelo`.
    * Procesa tareas de forma concurrente y envía resultados a `cola_resultados`.
* **Dashboard (Monitor):**
    * Visualiza en tiempo real la convergencia de Pi y la carga de trabajo por nodo usando `Matplotlib`.

## 2. Requisitos Previos

### Software
* Python 3.x
* RabbitMQ Server (instalado y corriendo en el equipo Host).
Para instalar las librerias necesarias ejecutar el comando:

    python -m pip install - r requirements.txt

### Configuración de RabbitMQ (Importante para red)
Para permitir la conexión desde otros equipos (ej. Máquinas Virtuales), es necesario crear un usuario administrador, ya que el usuario `guest` está bloqueado remotamente.

Ejecutar en la terminal del servidor RabbitMQ:
```bash
rabbitmqctl add_user examen examen
rabbitmqctl set_user_tags examen administrator
rabbitmqctl set_permissions -p / examen ".*" ".*" ".*"