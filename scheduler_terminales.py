"""
Scheduler Distribuido - Versión para pruebas entre 2 terminales.

Ejecutar en Terminal 1:  python scheduler_terminales.py --node rpi-01
Ejecutar en Terminal 2:  python scheduler_terminales.py --node pc-01

Requisitos: pip install redis psutil
Redis debe estar corriendo en localhost:6379
"""

import redis
import json
import psutil
import time
import uuid
import logging
import threading
import argparse
import random
import math
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(message)s',
    datefmt='%H:%M:%S'
)

# ═══════════════════════════════════════════════════════════════════════════════
# SCRIPTS LUA - Operaciones atómicas en Redis
# ═══════════════════════════════════════════════════════════════════════════════

# Asignación atómica: HSET tarea + LPUSH a cola
LUA_ASSIGN = """
local task_id = KEYS[1]
local queue_key = KEYS[2]
for i = 1, #ARGV, 2 do
    redis.call('HSET', task_id, ARGV[i], ARGV[i+1])
end
redis.call('LPUSH', queue_key, task_id)
return 1
"""

# Completar tarea + ajustar reputación atómicamente
LUA_COMPLETE = """
local task_id = KEYS[1]
local node_key = KEYS[2]
redis.call('HSET', task_id, 'status', ARGV[1], 'completed_at', ARGV[2], 'result', ARGV[3])
if ARGV[4] == '1' then
    local rep = tonumber(redis.call('HGET', node_key, 'reputation') or 100)
    if rep < 100 then redis.call('HSET', node_key, 'reputation', rep + 1) end
else
    local rep = tonumber(redis.call('HGET', node_key, 'reputation') or 100)
    if rep > 0 then redis.call('HSET', node_key, 'reputation', rep - 5) end
end
return 1
"""

# Reasignar tareas de un nodo caído a otro
LUA_REASSIGN = """
local dead_queue = KEYS[1]
local alive_queue = KEYS[2]
local count = 0
while true do
    local task_id = redis.call('RPOP', dead_queue)
    if not task_id then break end
    redis.call('HSET', task_id, 'assigned_to', ARGV[1], 'status', 'reassigned')
    redis.call('LPUSH', alive_queue, task_id)
    count = count + 1
end
return count
"""

# Distributed lock con TTL (protocolo de sincronización)
LUA_ACQUIRE_LOCK = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return 1
end
return 0
"""

LUA_RELEASE_LOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
"""


# ═══════════════════════════════════════════════════════════════════════════════
# 1. MEMORIA DISTRIBUIDA
# ═══════════════════════════════════════════════════════════════════════════════

class DistributedMemory:
    """Memoria compartida entre nodos con versionado y replicación via Redis."""

    def __init__(self, r, node_id):
        self._r = r
        self._node_id = node_id
        self._prefix = "dmem:"
        self._log = logging.getLogger(f"MEM:{node_id}")

    def write(self, key, value):
        """Escribe un dato compartido con versión y metadatos."""
        full_key = f"{self._prefix}{key}"
        pipe = self._r.pipeline(transaction=True)
        pipe.hincrby(full_key, "version", 1)
        pipe.hset(full_key, mapping={
            'data': json.dumps(value),
            'updated_by': self._node_id,
            'updated_at': datetime.now().isoformat()
        })
        pipe.publish(f"dmem:update:{key}", json.dumps({
            'key': key, 'node': self._node_id, 'action': 'write'
        }))
        pipe.execute()
        self._log.info("WRITE '%s' = %s", key, value)

    def read(self, key):
        """Lee un dato compartido. Retorna (valor, versión) o (None, 0)."""
        data = self._r.hgetall(f"{self._prefix}{key}")
        if not data or 'data' not in data:
            return None, 0
        return json.loads(data['data']), int(data.get('version', 0))

    def delete(self, key):
        """Elimina un dato compartido."""
        self._r.delete(f"{self._prefix}{key}")
        self._r.publish(f"dmem:update:{key}", json.dumps({
            'key': key, 'node': self._node_id, 'action': 'delete'
        }))
        self._log.info("DELETE '%s'", key)

    def list_keys(self):
        """Lista todas las claves de memoria compartida."""
        keys = []
        for k in self._r.scan_iter(match=f"{self._prefix}*", count=100):
            keys.append(k.replace(self._prefix, ""))
        return keys

    def watch(self, callback):
        """Suscribe a cambios en la memoria compartida (hilo aparte)."""
        def _listener():
            pubsub = self._r.pubsub()
            pubsub.psubscribe("dmem:update:*")
            for msg in pubsub.listen():
                if msg['type'] == 'pmessage':
                    try:
                        data = json.loads(msg['data'])
                        if data.get('node') != self._node_id:
                            callback(data)
                    except Exception:
                        pass
        t = threading.Thread(target=_listener, daemon=True)
        t.start()
        return t


# ═══════════════════════════════════════════════════════════════════════════════
# 2. SINCRONIZACIÓN DISTRIBUIDA
# ═══════════════════════════════════════════════════════════════════════════════

class DistributedSync:
    """Primitivas de sincronización: locks, barreras y elección de líder."""

    def __init__(self, r, node_id):
        self._r = r
        self._node_id = node_id
        self._log = logging.getLogger(f"SYNC:{node_id}")
        self._acquire_lock = r.register_script(LUA_ACQUIRE_LOCK)
        self._release_lock = r.register_script(LUA_RELEASE_LOCK)

    def acquire_lock(self, name, ttl_ms=5000):
        """Adquiere un lock distribuido. Retorna token si lo obtiene, None si no."""
        token = uuid.uuid4().hex
        lock_key = f"lock:{name}"
        result = self._acquire_lock(keys=[lock_key], args=[token, ttl_ms])
        if result:
            self._log.info("LOCK adquirido: '%s'", name)
            return token
        self._log.info("LOCK ocupado: '%s'", name)
        return None

    def release_lock(self, name, token):
        """Libera un lock distribuido (solo si el token coincide)."""
        result = self._release_lock(keys=[f"lock:{name}"], args=[token])
        if result:
            self._log.info("LOCK liberado: '%s'", name)
        return bool(result)

    def barrier_wait(self, name, expected_nodes, timeout=30):
        """Barrera: espera a que N nodos lleguen al mismo punto."""
        barrier_key = f"barrier:{name}"
        self._r.sadd(barrier_key, self._node_id)
        self._r.expire(barrier_key, timeout + 5)
        self._log.info("BARRIER '%s': esperando %d nodos...", name, expected_nodes)

        deadline = time.time() + timeout
        while time.time() < deadline:
            count = self._r.scard(barrier_key)
            if count >= expected_nodes:
                self._log.info("BARRIER '%s': todos los nodos llegaron (%d/%d)",
                               name, count, expected_nodes)
                return True
            time.sleep(0.5)

        count = self._r.scard(barrier_key)
        self._log.warning("BARRIER '%s': timeout (%d/%d)", name, count, expected_nodes)
        return False

    def elect_leader(self):
        """Elección de líder: el nodo con mayor reputación y menor carga gana."""
        nodes = {}
        for k in self._r.scan_iter(match="node:*", count=100):
            data = self._r.hgetall(k)
            if not data:
                continue
            try:
                last_hb = datetime.fromisoformat(data['last_heartbeat'])
                if (datetime.now() - last_hb).total_seconds() > 10:
                    continue
            except (KeyError, ValueError):
                continue
            nid = k.split(":", 1)[1]
            rep = int(data.get('reputation', 50))
            load = int(data.get('current_load', 100))
            nodes[nid] = rep * 0.4 + (100 - load) * 0.6

        if not nodes:
            return None
        leader = max(nodes, key=nodes.get)
        self._r.set("cluster:leader", leader, ex=15)
        self._log.info("LEADER elegido: %s (score=%.1f)", leader, nodes[leader])
        return leader

    def get_leader(self):
        """Retorna el líder actual o None."""
        return self._r.get("cluster:leader")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SCHEDULER DISTRIBUIDO (con reconfiguración ante fallos)
# ═══════════════════════════════════════════════════════════════════════════════

class DistributedScheduler:

    def __init__(self, node_id, redis_host='localhost', redis_port=6379):
        self.node_id = node_id
        self._log = logging.getLogger(f"SCHED:{node_id}")
        self._pool = redis.ConnectionPool(
            host=redis_host, port=redis_port,
            decode_responses=True, max_connections=8,
            socket_connect_timeout=5, socket_timeout=5,
            retry_on_timeout=True
        )
        self._r = redis.Redis(connection_pool=self._pool)

        # Verificar conexión
        self._r.ping()
        self._log.info("Conectado a Redis %s:%d", redis_host, redis_port)

        self._lock = threading.Lock()
        self._running = threading.Event()
        self._running.set()
        self._current_load = 0
        self.reputation = 100
        self.available = True

        # Subsistemas
        self.memory = DistributedMemory(self._r, node_id)
        self.sync = DistributedSync(self._r, node_id)

        # Scripts Lua
        self._assign_script = self._r.register_script(LUA_ASSIGN)
        self._complete_script = self._r.register_script(LUA_COMPLETE)
        self._reassign_script = self._r.register_script(LUA_REASSIGN)

        # Hilos internos
        self._threads = []
        for target in (self._heartbeat_loop, self._load_loop, self._fault_detection_loop,
                        self._worker_loop):
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self._threads.append(t)

        # Escuchar cambios en memoria compartida
        self.memory.watch(self._on_memory_change)

        self._log.info("Nodo '%s' iniciado", node_id)

    # ── Propiedad thread-safe ────────────────────────────────────────

    @property
    def current_load(self):
        with self._lock:
            return self._current_load

    @current_load.setter
    def current_load(self, value):
        with self._lock:
            self._current_load = value

    # ── Heartbeat ────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        node_key = f"node:{self.node_id}"
        while self._running.is_set():
            try:
                pipe = self._r.pipeline(transaction=False)
                pipe.hset(node_key, mapping={
                    'last_heartbeat': datetime.now().isoformat(),
                    'available': int(self.available),
                    'reputation': self.reputation,
                    'current_load': self.current_load,
                    'tasks_in_queue': self._r.llen(f"queue:{self.node_id}")
                })
                pipe.expire(node_key, 10)
                pipe.execute()
            except redis.RedisError as e:
                self._log.warning("Heartbeat fallido: %s", e)
            self._running.wait(timeout=2)

    # ── Monitoreo de carga ───────────────────────────────────────────

    def _load_loop(self):
        while self._running.is_set():
            try:
                cpu = psutil.cpu_percent(interval=1)
                mem = psutil.virtual_memory().percent
                self.current_load = int(cpu * 0.7 + mem * 0.3)
            except Exception as e:
                self._log.warning("Error métricas: %s", e)
            self._running.wait(timeout=4)

    # ── Detección de fallos y reconfiguración ────────────────────────

    def _fault_detection_loop(self):
        """Detecta nodos caídos y reasigna sus tareas pendientes."""
        known_nodes = set()
        while self._running.is_set():
            try:
                alive = set()
                for k in self._r.scan_iter(match="node:*", count=100):
                    data = self._r.hgetall(k)
                    nid = k.split(":", 1)[1]
                    try:
                        last_hb = datetime.fromisoformat(data['last_heartbeat'])
                        if (datetime.now() - last_hb).total_seconds() <= 10:
                            alive.add(nid)
                    except (KeyError, ValueError):
                        pass

                # Detectar nodos que desaparecieron
                dead = known_nodes - alive
                for dead_node in dead:
                    if dead_node == self.node_id:
                        continue
                    dead_queue = f"queue:{dead_node}"
                    pending = self._r.llen(dead_queue)
                    if pending > 0:
                        # Reasignar tareas del nodo caído a este nodo
                        count = self._reassign_script(
                            keys=[dead_queue, f"queue:{self.node_id}"],
                            args=[self.node_id]
                        )
                        self._log.warning(
                            "⚠ FALLO DETECTADO: nodo '%s' caído. %d tareas reasignadas a '%s'",
                            dead_node, count, self.node_id
                        )
                        # Registrar en log de eventos del clúster
                        self._r.lpush("cluster:events", json.dumps({
                            'type': 'node_failure',
                            'dead_node': dead_node,
                            'tasks_reassigned': count,
                            'reassigned_to': self.node_id,
                            'detected_by': self.node_id,
                            'timestamp': datetime.now().isoformat()
                        }))
                    else:
                        self._log.warning("⚠ Nodo '%s' desconectado (sin tareas pendientes)",
                                          dead_node)

                known_nodes = alive | {self.node_id}

                # Elegir líder periódicamente
                self.sync.elect_leader()

            except redis.RedisError as e:
                self._log.warning("Error en detección de fallos: %s", e)
            self._running.wait(timeout=5)

    # ── Worker: procesa tareas de la cola ────────────────────────────

    def _worker_loop(self):
        """Procesa tareas pendientes automáticamente."""
        while self._running.is_set():
            try:
                task_id, task_data = self.get_pending_task()
                if task_id:
                    self._process_task(task_id, task_data)
                else:
                    self._running.wait(timeout=1)
            except Exception as e:
                self._log.error("Error en worker: %s", e)
                self._running.wait(timeout=2)

    def _process_task(self, task_id, task_data):
        """Ejecuta una tarea según su tipo."""
        self._log.info("▶ Procesando tarea %s ...", task_id)
        data = json.loads(task_data.get('data', '{}'))
        task_type = data.get('type', 'compute')

        try:
            if task_type == 'compute':
                # Tarea de cómputo: cálculo factorial
                n = data.get('n', 20)
                result = {'factorial': str(math.factorial(n)), 'n': n}
                time.sleep(random.uniform(0.5, 2.0))  # Simular trabajo

            elif task_type == 'memory_write':
                # Tarea de escritura en memoria compartida
                self.memory.write(data['key'], data['value'])
                result = {'written': data['key']}

            elif task_type == 'memory_read':
                # Tarea de lectura de memoria compartida
                val, ver = self.memory.read(data['key'])
                result = {'key': data['key'], 'value': val, 'version': ver}

            elif task_type == 'stress':
                # Tarea de estrés: cálculo intensivo
                iterations = data.get('iterations', 100000)
                total = sum(math.sqrt(i) for i in range(iterations))
                result = {'iterations': iterations, 'checksum': round(total, 2)}

            else:
                result = {'echo': data}

            self.complete_task(task_id, result=result, success=True)
            self._log.info("✔ Tarea %s completada: %s", task_id, json.dumps(result)[:80])

        except Exception as e:
            self.complete_task(task_id, result={'error': str(e)}, success=False)
            self._log.error("✘ Tarea %s fallida: %s", task_id, e)

    # ── Callback de memoria compartida ───────────────────────────────

    def _on_memory_change(self, event):
        self._log.info("📥 Memoria actualizada por '%s': %s '%s'",
                        event.get('node'), event.get('action'), event.get('key'))

    # ── Selección de nodo ────────────────────────────────────────────

    def select_best_node(self):
        """score = reputación×0.4 + (100−carga)×0.6 + bonus si pocas tareas en cola"""
        node_keys = [k for k in self._r.scan_iter(match="node:*", count=100)]
        if not node_keys:
            return None

        pipe = self._r.pipeline(transaction=False)
        for key in node_keys:
            pipe.hgetall(key)
        results = pipe.execute()

        now = datetime.now()
        best_node = None
        best_score = -1.0

        for node_key, node_data in zip(node_keys, results):
            if not node_data:
                continue
            if node_data.get('available', '0') == '0':
                continue
            try:
                last_hb = datetime.fromisoformat(node_data['last_heartbeat'])
            except (KeyError, ValueError):
                continue
            if (now - last_hb).total_seconds() > 10:
                continue

            reputation = int(node_data.get('reputation', 50))
            load = int(node_data.get('current_load', 100))
            queue_len = int(node_data.get('tasks_in_queue', 0))
            # Penalización por tareas acumuladas
            queue_penalty = min(queue_len * 5, 30)
            score = reputation * 0.4 + (100 - load) * 0.6 - queue_penalty

            nid = node_key.split(":", 1)[1]
            if score > best_score:
                best_score = score
                best_node = nid

        return best_node

    # ── Gestión de tareas ────────────────────────────────────────────

    def assign_task(self, task_data):
        best_node = self.select_best_node()
        if not best_node:
            self._log.warning("No hay nodos disponibles")
            return None

        task_id = f"task:{uuid.uuid4().hex[:12]}"
        task = {
            'id': task_id,
            'assigned_to': best_node,
            'data': json.dumps(task_data),
            'status': 'pending',
            'created_at': datetime.now().isoformat()
        }
        args = [v for pair in task.items() for v in pair]
        self._assign_script(keys=[task_id, f"queue:{best_node}"], args=args)
        self._log.info("→ Tarea %s asignada a '%s'", task_id, best_node)
        return task_id

    def get_pending_task(self):
        task_id = self._r.rpop(f"queue:{self.node_id}")
        if not task_id:
            return None, None
        return task_id, self._r.hgetall(task_id)

    def complete_task(self, task_id, result=None, success=True):
        self._complete_script(
            keys=[task_id, f"node:{self.node_id}"],
            args=[
                'completed' if success else 'failed',
                datetime.now().isoformat(),
                json.dumps(result) if result else '',
                '1' if success else '0'
            ]
        )

    # ── Estado del clúster ───────────────────────────────────────────

    def get_cluster_status(self):
        """Retorna estado de todos los nodos activos."""
        nodes = {}
        for k in self._r.scan_iter(match="node:*", count=100):
            data = self._r.hgetall(k)
            nid = k.split(":", 1)[1]
            try:
                last_hb = datetime.fromisoformat(data['last_heartbeat'])
                age = (datetime.now() - last_hb).total_seconds()
            except (KeyError, ValueError):
                age = 999
            nodes[nid] = {
                'alive': age < 10,
                'heartbeat_age': round(age, 1),
                'load': data.get('current_load', '?'),
                'reputation': data.get('reputation', '?'),
                'available': data.get('available', '?'),
                'queue': data.get('tasks_in_queue', '0')
            }
        return nodes

    # ── Shutdown ─────────────────────────────────────────────────────

    def stop(self):
        self._log.info("Apagando nodo '%s'...", self.node_id)
        self._running.clear()
        for t in self._threads:
            t.join(timeout=5)
        # Limpiar estado del nodo en Redis
        self._r.delete(f"node:{self.node_id}")
        self._pool.disconnect()
        self._log.info("Nodo '%s' apagado", self.node_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. INTERFAZ INTERACTIVA (CLI)
# ═══════════════════════════════════════════════════════════════════════════════

HELP_TEXT = """
╔══════════════════════════════════════════════════════════════════╗
║                    COMANDOS DISPONIBLES                        ║
╠══════════════════════════════════════════════════════════════════╣
║ TAREAS                                                         ║
║   task compute <N>      Asigna tarea de factorial(N)           ║
║   task stress <iters>   Asigna tarea de cálculo intensivo      ║
║   task batch <count>    Asigna múltiples tareas de golpe       ║
║                                                                ║
║ MEMORIA DISTRIBUIDA                                            ║
║   mem write <key> <val> Escribe en memoria compartida          ║
║   mem read <key>        Lee de memoria compartida              ║
║   mem list              Lista claves en memoria compartida     ║
║   mem delete <key>      Elimina de memoria compartida          ║
║                                                                ║
║ SINCRONIZACIÓN                                                 ║
║   lock <name>           Adquiere un lock distribuido           ║
║   unlock <name>         Libera un lock distribuido             ║
║   barrier <name> <N>    Espera a que N nodos lleguen           ║
║   leader                Elige/muestra líder del clúster        ║
║                                                                ║
║ ESTADO                                                         ║
║   status                Muestra estado de todos los nodos      ║
║   events                Muestra últimos eventos del clúster    ║
║   flush                 Limpia todo el estado en Redis          ║
║                                                                ║
║ help / quit                                                    ║
╚══════════════════════════════════════════════════════════════════╝
"""


def print_status(scheduler):
    nodes = scheduler.get_cluster_status()
    leader = scheduler.sync.get_leader()
    print(f"\n{'─' * 70}")
    print(f" {'NODO':<14} {'ESTADO':<10} {'CARGA':<8} {'REPUT.':<8} {'COLA':<6} {'LÍDER'}")
    print(f"{'─' * 70}")
    for nid, info in sorted(nodes.items()):
        status = "🟢 ACTIVO" if info['alive'] else "🔴 CAÍDO"
        is_leader = "👑" if nid == leader else ""
        is_me = " ← tú" if nid == scheduler.node_id else ""
        print(f" {nid:<14} {status:<10} {info['load']:>4}%   {info['reputation']:>5}   "
              f"{info['queue']:>4}  {is_leader}{is_me}")
    print(f"{'─' * 70}\n")


def print_events(scheduler):
    events = scheduler._r.lrange("cluster:events", 0, 9)
    if not events:
        print(" (sin eventos)")
        return
    print(f"\n{'─' * 60}")
    print(" ÚLTIMOS EVENTOS DEL CLÚSTER")
    print(f"{'─' * 60}")
    for raw in events:
        ev = json.loads(raw)
        ts = ev.get('timestamp', '?')[:19]
        etype = ev.get('type', '?')
        if etype == 'node_failure':
            print(f" [{ts}] ⚠ FALLO: '{ev['dead_node']}' → "
                  f"{ev['tasks_reassigned']} tareas → '{ev['reassigned_to']}'")
        else:
            print(f" [{ts}] {etype}: {ev}")
    print(f"{'─' * 60}\n")


def interactive_loop(scheduler):
    active_locks = {}  # name → token

    print(HELP_TEXT)
    print(f" Nodo activo: {scheduler.node_id}")
    print(f" Escribe 'help' para ver comandos.\n")

    while True:
        try:
            raw = input(f"[{scheduler.node_id}] > ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not raw:
            continue
        parts = raw.split()
        cmd = parts[0].lower()

        # ── Tareas ───────────────────────────────────────────────
        if cmd == 'task' and len(parts) >= 2:
            subcmd = parts[1].lower()
            if subcmd == 'compute':
                n = int(parts[2]) if len(parts) > 2 else 20
                scheduler.assign_task({'type': 'compute', 'n': n})
            elif subcmd == 'stress':
                iters = int(parts[2]) if len(parts) > 2 else 100000
                scheduler.assign_task({'type': 'stress', 'iterations': iters})
            elif subcmd == 'batch':
                count = int(parts[2]) if len(parts) > 2 else 5
                for i in range(count):
                    scheduler.assign_task({'type': 'compute', 'n': random.randint(5, 50)})
                print(f" {count} tareas enviadas")
            else:
                print(" Subcomandos: compute, stress, batch")

        # ── Memoria ──────────────────────────────────────────────
        elif cmd == 'mem' and len(parts) >= 2:
            subcmd = parts[1].lower()
            if subcmd == 'write' and len(parts) >= 4:
                key = parts[2]
                val = " ".join(parts[3:])
                try:
                    val = json.loads(val)
                except json.JSONDecodeError:
                    pass
                scheduler.memory.write(key, val)
            elif subcmd == 'read' and len(parts) >= 3:
                val, ver = scheduler.memory.read(parts[2])
                print(f" '{parts[2]}' = {val}  (versión {ver})")
            elif subcmd == 'list':
                keys = scheduler.memory.list_keys()
                print(f" Claves: {keys if keys else '(vacío)'}")
            elif subcmd == 'delete' and len(parts) >= 3:
                scheduler.memory.delete(parts[2])
            else:
                print(" Subcomandos: write <k> <v>, read <k>, list, delete <k>")

        # ── Sincronización ───────────────────────────────────────
        elif cmd == 'lock' and len(parts) >= 2:
            name = parts[1]
            token = scheduler.sync.acquire_lock(name, ttl_ms=30000)
            if token:
                active_locks[name] = token
                print(f" Lock '{name}' adquirido (expira en 30s)")
            else:
                print(f" Lock '{name}' está ocupado por otro nodo")

        elif cmd == 'unlock' and len(parts) >= 2:
            name = parts[1]
            token = active_locks.pop(name, None)
            if token:
                ok = scheduler.sync.release_lock(name, token)
                print(f" Lock '{name}' {'liberado' if ok else 'ya expiró'}")
            else:
                print(f" No tienes el lock '{name}'")

        elif cmd == 'barrier' and len(parts) >= 3:
            name = parts[1]
            n = int(parts[2])
            print(f" Esperando en barrera '{name}' ({n} nodos)...")
            ok = scheduler.sync.barrier_wait(name, n, timeout=30)
            print(f" Barrera {'cruzada ✔' if ok else 'timeout ✘'}")

        elif cmd == 'leader':
            leader = scheduler.sync.elect_leader()
            current = scheduler.sync.get_leader()
            is_me = " (eres tú)" if current == scheduler.node_id else ""
            print(f" Líder actual: {current}{is_me}")

        # ── Estado ───────────────────────────────────────────────
        elif cmd == 'status':
            print_status(scheduler)

        elif cmd == 'events':
            print_events(scheduler)

        elif cmd == 'flush':
            for k in scheduler._r.scan_iter(match="*", count=500):
                scheduler._r.delete(k)
            print(" Estado de Redis limpiado")

        elif cmd == 'help':
            print(HELP_TEXT)

        elif cmd in ('quit', 'exit', 'q'):
            break

        else:
            print(f" Comando desconocido: '{raw}'. Escribe 'help'.")

    scheduler.stop()
    print("Adiós.")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Scheduler Distribuido - Prueba local")
    parser.add_argument('--node', required=True, help="ID del nodo (ej: rpi-01, pc-01)")
    parser.add_argument('--redis-host', default='localhost', help="Host Redis (default: localhost)")
    parser.add_argument('--redis-port', type=int, default=6379, help="Puerto Redis (default: 6379)")
    args = parser.parse_args()

    try:
        scheduler = DistributedScheduler(args.node, args.redis_host, args.redis_port)
    except redis.ConnectionError:
        print(f"\n ERROR: No se pudo conectar a Redis en {args.redis_host}:{args.redis_port}")
        print(" Asegúrate de que Redis esté corriendo:")
        print("   Windows: descarga Redis de https://github.com/tporadowski/redis/releases")
        print("   Docker:  docker run -d -p 6379:6379 redis")
        print("   Linux:   sudo apt install redis-server && sudo systemctl start redis\n")
        sys.exit(1)

    interactive_loop(scheduler)


if __name__ == '__main__':
    main()
