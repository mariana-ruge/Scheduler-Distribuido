# Scheduler Distribuido

Sistema de planificación distribuida de tareas entre múltiples nodos (workers + scheduler) coordinados a través de **Redis**. Cada nodo es un proceso Python independiente; Redis actúa como broker compartido (heartbeats, colas, locks, barreras, memoria distribuida y métricas).

> Originalmente pensado para correrse en varias Raspberry Pi, esta simulación permite levantar todo el clúster en una sola máquina (3 terminales) — ideal para evaluación local.

---

## Cómo ejecutar el proyecto en macOS (paso a paso)

Estas instrucciones están escritas para macOS (Apple Silicon o Intel) con **Docker Desktop** ya instalado. Solo necesitas:

- Docker Desktop corriendo (icono de la ballena en la barra de menú).
- Python 3.10+ instalado (macOS trae uno; si no, `brew install python`).
- 3 ventanas/pestañas de Terminal (Terminal.app o iTerm2).

El sistema **no requiere construir ninguna imagen Docker**: solo levantamos Redis dentro de un contenedor y ejecutamos los `.py` localmente.

### 1) Clonar y entrar al proyecto

```bash
git clone <URL-del-repo> Scheduler-Distribuido
cd Scheduler-Distribuido
```

### 2) Crear un entorno virtual e instalar dependencias

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Las dependencias son solo dos: `redis` y `psutil`.

### 3) Levantar Redis con Docker (una sola línea)

En cualquier terminal, ejecuta:

```bash
docker run -d --name redis-scheduler -p 6379:6379 redis:7-alpine
```

Esto descarga (la primera vez) y arranca Redis 7 escuchando en `localhost:6379`, que es exactamente lo que el código espera por defecto (`config.py` → `redis_host = "localhost"`).

Verificar que está vivo:

```bash
docker exec -it redis-scheduler redis-cli ping
# → debe responder: PONG
```

Comandos útiles para gestionar el contenedor:

| Acción | Comando |
|---|---|
| Detener Redis | `docker stop redis-scheduler` |
| Volver a arrancarlo | `docker start redis-scheduler` |
| Eliminarlo | `docker rm -f redis-scheduler` |
| Ver logs | `docker logs -f redis-scheduler` |

### 4) Arrancar los 3 nodos (3 terminales)

> **Importante:** en cada terminal, primero `cd` al proyecto y `source venv/bin/activate` para usar el mismo entorno virtual.

**Terminal 1 — Scheduler (orquestador, expone CLI completo):**
```bash
python main.py --node-id scheduler
```

**Terminal 2 — Worker 1:**
```bash
python main.py --node-id worker-1
```

**Terminal 3 — Worker 2:**
```bash
python main.py --node-id worker-2
```

Cada nodo se registra en Redis, empieza a publicar heartbeats cada 2 s, calcula su carga (ponderada CPU/RAM) y queda esperando tareas.

### 5) Probar el sistema desde el CLI del scheduler

En la **Terminal 1** (scheduler) verás un prompt `[scheduler] >`. Comandos típicos:

```text
help                  # lista de comandos
status                # estado de todos los nodos del clúster
scores                # puntuaciones (reputación, carga, cola)
task compute 5        # crea 5 tareas tipo "compute" — las verás ejecutarse en los workers
metrics               # resumen de métricas
events 10             # últimos 10 eventos del clúster
crash worker-1        # simula caída de worker-1 (el detector lo marcará FAILED)
mem set foo bar       # escribe en memoria distribuida
mem get foo           # lee de memoria distribuida
lock recurso-x        # adquiere un lock distribuido
unlock recurso-x      # libera el lock
flush                 # ¡cuidado! limpia TODO Redis
exit                  # sale (también Ctrl+C)
```

En las terminales de workers verás logs `⚡ Ejecutando ...` y `✔ completada ...` conforme procesan tareas.

### 6) Apagado limpio

1. En cada terminal de Python: `exit` o `Ctrl+C`.
2. Detener Redis: `docker stop redis-scheduler` (o `docker rm -f redis-scheduler` para borrarlo).

---

## Solución de problemas (macOS)

| Síntoma | Causa probable | Solución |
|---|---|---|
| `ERROR: No se puede conectar a Redis en localhost:6379` | Redis no está corriendo | `docker start redis-scheduler` o relanzar con el `docker run` del paso 3 |
| `port is already allocated` al hacer `docker run` | Ya hay algo en el 6379 | `docker rm -f redis-scheduler`, o usa otro puerto: `-p 6380:6379` y edita `config.py` (`redis_port = 6380`) |
| `command not found: python` | macOS reciente | Usa `python3` en vez de `python` |
| `psutil` falla al instalar en Apple Silicon | Falta toolchain | `xcode-select --install` y reintenta `pip install -r requirements.txt` |
| El contenedor no arranca con `docker:` errores | Docker Desktop apagado | Abre Docker Desktop y espera al icono "Engine running" |
| Quiero ver qué hay en Redis | — | `docker exec -it redis-scheduler redis-cli` luego `KEYS *`, `HGETALL node:scheduler`, etc. |

---

## Estructura del proyecto

```
.
├── main.py                       # Punto de entrada (CLI scheduler/worker)
├── config.py                     # Configuración centralizada (Redis, pesos, timeouts)
├── scheduler.py                  # DistributedScheduler (asignación, scoring, colas)
├── synchronization.py            # Locks distribuidos, barreras, reloj lógico (Lamport)
├── distributed_memory.py         # Memoria K/V distribuida con versiones y replicación
├── fault_tolerance.py            # Detector de fallos, reconfiguración, simulate_crash
├── metrics.py                    # Métricas + stream de eventos en Redis
├── scheduler(para raspberry).py  # Variante simplificada para correr en Raspberry Pi reales
├── scheduler_terminales.py       # Variante alternativa con UI por terminales
├── requirements.txt              # redis, psutil
└── README.md
```

El flujo en producción usa **`main.py` + `scheduler.py`** (los demás módulos son dependencias). Los archivos `scheduler(para raspberry).py` y `scheduler_terminales.py` son variantes y **no se necesitan para evaluar el proyecto**.

---

## Arquitectura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  scheduler  │     │  worker-1   │     │  worker-2   │
│ (orq. + CLI)│     │  (procesa)  │     │  (procesa)  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────┴──────┐
                    │    Redis    │  (Docker container)
                    │   (broker)  │
                    └─────────────┘
```

Cada nodo ejecuta una instancia de `DistributedScheduler`. Redis centraliza el estado compartido y las colas de tareas.

## Flujo completo de una tarea

```
1. REGISTRO          2. MONITOREO         3. ASIGNACIÓN        4. EJECUCIÓN         5. COMPLETADO
   DEL NODO             DE CARGA

┌──────────┐       ┌──────────┐        ┌──────────┐        ┌──────────┐        ┌──────────┐
│ __init__ │──────>│ heartbeat│        │ assign   │        │ get      │        │ complete │
│          │  c/2s │ loop     │        │ _task()  │        │ _pending │        │ _task()  │
│ Crea     │       │          │        │          │        │ _tasks() │        │          │
│ threads  │       │ Publica: │        │ 1.select │        │          │        │ Lua:     │
│ daemon   │       │ -heartbt │        │   _best  │        │ RPOP de  │        │ -marca   │
│          │  c/5s │ -carga   │        │   _node()│        │ queue:   │        │  status  │
│          │──────>│ -dispo.  │        │ 2.Lua:   │        │ {nodo}   │        │ -ajusta  │
│          │       │ -reput.  │        │  HSET+   │        │          │        │  reput.  │
│          │       │          │        │  LPUSH   │        │ HGETALL  │        │  (+1 ok) │
└──────────┘       └──────────┘        └──────────┘        └──────────┘        └──────────┘
```

## Componentes principales

### 1. Inicialización (`__init__`)
- Crea un **connection pool** Redis (4 conexiones, con timeouts y retry)
- Pre-registra los **scripts Lua** (se compilan una vez, se ejecutan por SHA)
- Lanza 2 **hilos daemon**: heartbeat (cada 2s) y monitoreo de carga (cada 5s)
- Usa `threading.Event` para shutdown limpio (en vez de `time.sleep`)

### 2. Heartbeat (`_heartbeat_loop`)
- Cada **2 segundos** publica en Redis el estado del nodo:
  - `last_heartbeat`: timestamp ISO
  - `available`: 0/1
  - `reputation`: 0-100
  - `current_load`: 0-100
- La clave `node:{id}` **expira en 10s**: si el nodo muere, desaparece solo
- Usa **pipeline** (HSET + EXPIRE en 1 roundtrip)

### 3. Monitoreo de carga (`_update_load_loop`)
- Cada **~5 segundos** calcula una métrica ponderada:
  ```
  carga = CPU × 0.7 + RAM × 0.3
  ```
- Acceso protegido con **Lock** (escritura desde este hilo, lectura desde heartbeat)

### 4. Selección de nodo (`select_best_node`)
- Descubre nodos con **SCAN** (no bloqueante, a diferencia de KEYS)
- Obtiene datos de todos los nodos en **1 roundtrip** (pipeline batch)
- Filtra nodos:
  - `available == 0` → descartado
  - `heartbeat > 10s` → descartado (nodo muerto)
- Puntúa cada nodo:
  ```
  score = reputación × 0.4 + (100 − carga) × 0.5 + bonus_vivo − penalización_cola
  ```
- Retorna el nodo con mayor score

### 5. Asignación de tarea (`assign_task`)
- Genera ID único con **UUID**
- Ejecuta **script Lua atómico** que en una sola operación:
  1. Crea el hash `task:{uuid}` con todos los campos
  2. Hace LPUSH a `queue:{nodo_destino}`
- Evita race conditions entre schedulers concurrentes

### 6. Obtener tareas (`get_pending_tasks`)
- **RPOP** de `queue:{node_id}` → cola FIFO (LPUSH en asignación, RPOP aquí)
- Obtiene datos completos de la tarea con HGETALL

### 7. Completar tarea (`complete_task`)
- **Script Lua atómico** que:
  1. Marca la tarea como `completed` o `failed`
  2. Si fue exitosa, incrementa reputación del nodo (+5, máx 100); si falló, la decrementa (-10)
- La reputación influye en futuras asignaciones

### 8. Tolerancia a fallos (`fault_tolerance.py`)
- `FailureDetector` clasifica nodos como `ALIVE` / `SUSPECTED` / `FAILED` según el último heartbeat.
- `ReconfigurationManager` re-encola tareas pendientes de un nodo caído hacia otro vivo.
- `simulate_crash(redis, node_id)` borra las claves del nodo para forzar la detección (útil para demos).

### 9. Shutdown (`stop`)
- `Event.clear()` → señaliza a ambos hilos que paren
- `join(timeout=5)` → espera a que terminen
- `pool.disconnect()` → cierra conexiones TCP a Redis

## Estructuras en Redis

| Clave | Tipo | Contenido |
|-------|------|-----------|
| `node:{id}` | Hash | `last_heartbeat`, `available`, `reputation`, `current_load` |
| `task:{uuid}` | Hash | `id`, `assigned_to`, `data`, `status`, `created_at`, `completed_at`, `result` |
| `queue:{node_id}` | List | IDs de tareas pendientes (FIFO) |
| `mem:{key}` | Hash | Memoria distribuida (valor + versión + autor) |
| `lock:{name}` | String (con TTL) | Lock distribuido |
| `barrier:{name}` | Set | Nodos que ya firmaron una barrera |
| `metrics:events` | Stream | Stream de eventos del clúster |

## Configuración

Todos los parámetros están en `config.py` (clase `SimConfig`). Lo más relevante:

| Parámetro | Default | Descripción |
|---|---|---|
| `redis_host` / `redis_port` | `localhost` / `6379` | Dónde está Redis |
| `heartbeat_interval` | `2.0` s | Frecuencia de heartbeats |
| `heartbeat_ttl` | `10` s | TTL de la clave `node:{id}` |
| `failure_threshold` / `failure_confirmed` | `5` / `10` s | Umbrales SUSPECTED / FAILED |
| `weight_reputation` / `weight_load` | `0.4` / `0.5` | Pesos del score |
| `reputation_initial` | `70` | Reputación de un nodo nuevo |
| `worker_poll_interval` | `1.0` s | Cada cuánto un worker busca tareas |

Si quieres apuntar a un Redis remoto, edita `config.py` (`redis_host = "..."`).

## Requisitos resumidos

- Python 3.10+
- Docker Desktop (solo para Redis)
- `pip install -r requirements.txt` → `redis`, `psutil`
