import redis
import json
import psutil
import time
import uuid
import logging
import threading
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Lua: crea hash de tarea + encola atómicamente
_LUA_ASSIGN_TASK = """
local task_id = KEYS[1]
local queue_key = KEYS[2]
for i = 1, #ARGV, 2 do
    redis.call('HSET', task_id, ARGV[i], ARGV[i+1])
end
redis.call('LPUSH', queue_key, task_id)
return 1
"""

# Lua: completa tarea + actualiza reputación atómicamente
_LUA_COMPLETE_TASK = """
local task_id = KEYS[1]
local node_key = KEYS[2]
local status = ARGV[1]
local completed_at = ARGV[2]
local result = ARGV[3]
local success = ARGV[4]
redis.call('HSET', task_id, 'status', status, 'completed_at', completed_at, 'result', result)
if success == '1' then
    local rep = tonumber(redis.call('HGET', node_key, 'reputation') or 100)
    if rep < 100 then
        redis.call('HSET', node_key, 'reputation', rep + 1)
    end
end
return 1
"""


class DistributedScheduler:

    def __init__(self, node_id, redis_host='192.168.1.100', redis_port=6379):
        self.node_id = node_id
        self._pool = redis.ConnectionPool(
            host=redis_host, port=redis_port,
            decode_responses=True, max_connections=4,
            socket_connect_timeout=5, socket_timeout=5,
            retry_on_timeout=True
        )
        self._redis = redis.Redis(connection_pool=self._pool)
        self._lock = threading.Lock()
        self._running = threading.Event()
        self._running.set()
        self._current_load = 0
        self.reputation = 100
        self.available = True

        self._assign_script = self._redis.register_script(_LUA_ASSIGN_TASK)
        self._complete_script = self._redis.register_script(_LUA_COMPLETE_TASK)

        self._threads = []
        for target in (self._heartbeat_loop, self._update_load_loop):
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self._threads.append(t)

    # ── Acceso thread-safe a current_load ────────────────────────────

    @property
    def current_load(self):
        with self._lock:
            return self._current_load

    @current_load.setter
    def current_load(self, value):
        with self._lock:
            self._current_load = value

    # ── Loops internos (hilos daemon) ────────────────────────────────

    def _heartbeat_loop(self):
        """Publica estado del nodo en Redis cada 2s. Expira en 10s."""
        node_key = f"node:{self.node_id}"
        while self._running.is_set():
            try:
                pipe = self._redis.pipeline(transaction=False)
                pipe.hset(node_key, mapping={
                    'last_heartbeat': datetime.now().isoformat(),
                    'available': int(self.available),
                    'reputation': self.reputation,
                    'current_load': self.current_load
                })
                pipe.expire(node_key, 10)
                pipe.execute()
            except redis.RedisError as e:
                logger.warning("Heartbeat fallido: %s", e)
            self._running.wait(timeout=2)

    def _update_load_loop(self):
        """Mide carga ponderada: 70% CPU + 30% RAM cada ~5s."""
        while self._running.is_set():
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                mem_percent = psutil.virtual_memory().percent
                self.current_load = int(cpu_percent * 0.7 + mem_percent * 0.3)
            except Exception as e:
                logger.warning("Error leyendo métricas: %s", e)
            self._running.wait(timeout=4)

    # ── Selección de nodos ───────────────────────────────────────────

    def select_best_node(self, task_requirements=None):
        """Elige el nodo con mejor score: reputación×0.4 + (100-carga)×0.6"""
        node_keys = [k for k in self._redis.scan_iter(match="node:*", count=100)]
        if not node_keys:
            return None

        # Obtener datos de todos los nodos en 1 roundtrip
        pipe = self._redis.pipeline(transaction=False)
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
            score = reputation * 0.4 + (100 - load) * 0.6

            if score > best_score:
                best_score = score
                best_node = node_key.split(":", 1)[1]

        return best_node

    # ── Gestión de tareas ────────────────────────────────────────────

    def assign_task(self, task_data):
        """Asigna tarea atómicamente al mejor nodo (Lua script)."""
        best_node = self.select_best_node()
        if not best_node:
            logger.warning("No hay nodos disponibles")
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

        logger.info("Tarea %s asignada al nodo %s", task_id, best_node)
        return task_id

    def get_pending_tasks(self):
        """Extrae la siguiente tarea de la cola FIFO (LPUSH/RPOP)."""
        task_id = self._redis.rpop(f"queue:{self.node_id}")
        if not task_id:
            return None, None
        task_data = self._redis.hgetall(task_id)
        return task_id, task_data

    def complete_task(self, task_id, result=None, success=True):
        """Completa tarea y ajusta reputación (+1 si éxito, máx 100)."""
        self._complete_script(
            keys=[task_id, f"node:{self.node_id}"],
            args=[
                'completed' if success else 'failed',
                datetime.now().isoformat(),
                json.dumps(result) if result else '',
                '1' if success else '0'
            ]
        )

    # ── Ciclo de vida ────────────────────────────────────────────────

    def stop(self):
        """Para hilos, espera join y desconecta pool Redis."""
        self._running.clear()
        for t in self._threads:
            t.join(timeout=5)
        self._pool.disconnect()