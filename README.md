# Scheduler Distribuido para Raspberry Pi

Sistema de planificación distribuida de tareas entre múltiples Raspberry Pi conectadas a través de Redis.

## Arquitectura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Nodo A    │     │   Nodo B    │     │   Nodo C    │
│ (Scheduler) │     │ (Scheduler) │     │ (Scheduler) │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────┴──────┐
                    │    Redis    │
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
  score = reputación × 0.4 + (100 − carga) × 0.6
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
  2. Si fue exitosa, incrementa reputación del nodo (+1, máx 100)
- La reputación influye en futuras asignaciones

### 8. Shutdown (`stop`)
- `Event.clear()` → señaliza a ambos hilos que paren
- `join(timeout=5)` → espera a que terminen
- `pool.disconnect()` → cierra conexiones TCP a Redis

## Estructuras en Redis

| Clave | Tipo | Contenido |
|-------|------|-----------|
| `node:{id}` | Hash | `last_heartbeat`, `available`, `reputation`, `current_load` |
| `task:{uuid}` | Hash | `id`, `assigned_to`, `data`, `status`, `created_at`, `completed_at`, `result` |
| `queue:{node_id}` | List | IDs de tareas pendientes (FIFO) |

## Uso

```python
# En cada Raspberry Pi
scheduler = DistributedScheduler('rpi-01', redis_host='192.168.1.100')

# Asignar tarea (desde cualquier nodo)
task_id = scheduler.assign_task({'tipo': 'procesamiento', 'payload': [1, 2, 3]})

# Procesar tareas (en el nodo asignado)
task_id, data = scheduler.get_pending_tasks()
if task_id:
    resultado = ejecutar(data)
    scheduler.complete_task(task_id, result=resultado, success=True)

# Apagar
scheduler.stop()
```

## Requisitos

```
pip install redis psutil
```

Redis debe estar corriendo en la red local (por defecto `192.168.1.100:6379`).
